from pyspark.sql.functions import *
import os
import numpy as np

spark = sq.SparkSession.builder.master("local").appName("my app").config("spark.some.config.option", "some-value").getOrCreate()

#path="/home/iman/Dropbox/work-samples-master/data-mr/data"
path="/tmp/data"

poi = spark.read.csv(os.path.join(path,"POIList.csv"),header=True,inferSchema = True)
data = spark.read.csv(os.path.join(path,"DataSample.csv"),header=True,inferSchema = True)
#***************************************
#***********  1.CLEANUP  ***************
#***************************************
#Finding the suspicous IDs
aa=(data.groupBy([' TimeSt', 'Latitude', 'Longitude']).agg(collect_list("_ID").alias("_ID2")).where(size("_ID2") > 1)).select(explode("_ID2").alias("_ID"))
data=data.join(aa, data._ID == aa._ID, "left_anti").drop(aa._ID) #Removing the suspicous IDs
#***************************************
#***********  1.CLEANUP  ***************
#***************************************
#Finding the suspicous IDs
aa=(data.groupBy([' TimeSt', 'Latitude', 'Longitude']).agg(collect_list("_ID").alias("_ID2")).where(size("_ID2") > 1)).select(explode("_ID2").alias("_ID"))
data=data.join(aa, data._ID == aa._ID, "left_anti").drop(aa._ID) #Removing the suspicous IDs
#***************************************
#***********  2.LABEL  *****************
#***************************************
#defining a function for calculating distance between two points
def dis(lat1,lon1,lat2,lon2):
        R=6371
        lon1=toRadians(lon1)
        lat1=toRadians(lat1)
        lon2=toRadians(lon2)
        lat2=toRadians(lat2)
        a=sin((lat1-lat2)/2)**2+cos(lat1)*cos(lat2)*sin((lon1-lon2)/2)**2
        c=2*atan2(sqrt(a),sqrt(1-a))
        d=R*c
        return d
		
poi = poi.select('POIID',col(" Latitude").alias("poiLatitude"), col("Longitude").alias("poiLongitude"))
data=data.crossJoin(poi)
data=data.withColumn("Poidistance", dis(data['Latitude'],data['Longitude'], data['poiLatitude'],data['poiLongitude']))

data2=data.groupBy('_ID').min('Poidistance')
data=data.join(data2,(data['_ID'] == data2['_ID']) & (data['Poidistance'] == data2['min(Poidistance)'])).drop(data2._ID)
data=data.drop('Poidistance')
#***************************************
#*************  3.ANALYSIS  ************
#***************************************
#Average and Standard deviation
analysis1=data.groupBy('POIID').agg(avg("min(Poidistance)").alias('AVG'), stddev("min(Poidistance)").alias('STD'))
analysis1.show()
#Radius and Density
#Average and Standard deviation
analysis1=data.groupBy('POIID').agg(avg("min(Poidistance)").alias('AVG'), stddev("min(Poidistance)").alias('STD'))
analysis1.show()
#Radius and Density
analysis2=data.groupBy('POIID').agg(max("min(Poidistance)").alias('Radius'), count("min(Poidistance)").alias('Count'))
analysis2=analysis2.withColumn('density',analysis2['Count']/(analysis2['Radius']**2*np.pi))
analysis2.show()

print("Execution completed")