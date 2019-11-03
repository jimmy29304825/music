# load model
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.recommendation import MatrixFactorizationModel
import math, time, json
from kafka import KafkaConsumer
import redis
from pymongo import MongoClient

# connect mongoDB
client = MongoClient(host="10.120.28.5", port=27017)
db = client['123']
music = db.music

# connect redis
r = redis.Redis(host='10.120.28.22', port=6379)

# load model
model_path = "/home/cloudera/Desktop/datasets/music/music_lens_als"
same_model = MatrixFactorizationModel.load(sc, model_path)

# load rating data
complete_ratings_raw_data = sc.textFile("/home/cloudera/Desktop/datasets/music/ff2_data.csv")
# set column names
complete_ratings_raw_data_header = complete_ratings_raw_data.take(1)[0]
# transfer data
## 1. filiter: remove first row
## 2. map: split data by ","
## 3. map: get first 3 columns
complete_ratings_data = complete_ratings_raw_data.filter(lambda line: line!=complete_ratings_raw_data_header).map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()
print("There are %s recommendations in the complete dataset" % (complete_ratings_data.count()))

# set model's parameter
seed = 5
iterations = 10
regularization_parameter = 0.1
ranks = [4, 8, 12]
errors = [0, 0, 0]
err = 0
tolerance = 0.02
min_error = float('inf')
best_rank = 4
best_iteration = -1

# load music meta data
complete_music_raw_data = sc.textFile("/home/cloudera/Desktop/datasets/music/songs_metadata_file_new.csv")
complete_music_raw_data_header = complete_music_raw_data.take(1)[0]
complete_music_data = complete_music_raw_data.filter(lambda line: line!=complete_music_raw_data_header).map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),tokens[1],tokens[2])).cache()
complete_music_titles = complete_music_data.map(lambda x: (int(x[0]),x[1]))
print("There are %s musics in the complete dataset" % (complete_music_titles.count()))

# join data's def
def get_counts_and_averages(ID_and_ratings_tuple):
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)

# group by every mucis couts data by music id
music_ID_with_ratings_RDD = (complete_ratings_data.map(lambda x: (x[1], x[2])).groupByKey())
# get average and counts for each music 
music_ID_with_avg_ratings_RDD = music_ID_with_ratings_RDD.map(get_counts_and_averages)
# get counts for each music 
music_rating_counts_RDD = music_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))

    
# 設定要連線到Kafka集群的相關設定, 產生一個Kafka的Consumer的實例
consumer = KafkaConsumer(
    # 指定Kafka集群伺服器
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"],
    value_deserializer=lambda m: json.loads(m.decode('ascii')),
    auto_offset_reset="latest"
)

# 讓Consumer向Kafka集群訂閱指定的topic
consumer.subscribe(topics="music4")

# build a list for consumer's data
new_user_ratings = []

# start consuming data from kafka
# data sample: "{'userid': 123465, 'music':('m1',5124), 'rating': 5}"
for record in consumer:
    msgValue = eval(record.value) 
    new_user_ID = msgValue['userid'] 
    song_index = msgValue['music'][0] 
    song_id = msgValue['music'][1] 
    rating = msgValue['rating'] 
    print(uid, song_id, song_index, rating) 
    data = (new_user_ID, song_id, rating) 
    new_user_ratings.append(data) 
    if song_index != 'm3': 
        continue 
    else:  # start model and get music list when getting m3's data 
        print(new_user_ratings)
        # transfer data into RDD
        new_user_ratings_RDD = sc.parallelize(new_user_ratings)
        # merge new data into old data
        complete_data_with_new_ratings_RDD = complete_ratings_data.union(new_user_ratings_RDD)
        
        # remodel with new data
        from time import time
        t0 = time()
        new_ratings_model = ALS.train(complete_data_with_new_ratings_RDD, best_rank, seed=seed, iterations=iterations, lambda_=regularization_parameter)
        tt = time() - t0
        print("New model trained in %s seconds" % round(tt,3))
    
        new_user_ratings_ids = map(lambda x: x[1], new_user_ratings) # get just music IDs
        # keep just those not on the ID list
        new_user_unrated_music_RDD = (complete_music_data.filter(lambda x: x[0] not in new_user_ratings_ids).map(lambda x: (new_user_ID, x[0])))
        # Use the input RDD, new_user_unrated_music_RDD, with new_ratings_model.predictAll() to predict new ratings for the musics
        new_user_recommendations_RDD = new_ratings_model.predictAll(new_user_unrated_music_RDD)
    
        # get every predicct result for new user
        new_user_recommendations_rating_RDD = new_user_recommendations_RDD.map(lambda x: (x.product, x.rating))
        # merge data with music info
        new_user_recommendations_rating_title_and_count_RDD = new_user_recommendations_rating_RDD.join(complete_music_titles).join(music_rating_counts_RDD)
        new_user_recommendations_rating_title_and_count_RDD.take(3)
        # transfer data format
        new_user_recommendations_rating_title_and_count_RDD = new_user_recommendations_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))
        # sort data by rating score and list first 25 data
        top_musics = new_user_recommendations_rating_title_and_count_RDD.filter(lambda r: r[2]>=25).takeOrdered(25, key=lambda x: -x[1])
        print('TOP recommended musics (with more than 25 reviews):\n%s' % '\n'.join(map(str, top_musics)))
        
        # send music list to redis and mongodb
        result_r = r.hset('music', new_user_ID, str(top_musics))
        j = {'user': new_user_ID, 'music': top_musics}
        result_m = music.insert_one(j)
        print(result_r, result_m, new_user_ratings)
        
        # reset list
        new_user_ratings = []


