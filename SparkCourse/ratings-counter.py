from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("ml-100k/u.data") #creating RDD data set here
ratings = lines.map(lambda x: x.split()[2]) # this map function will split on the 
											# basis of white space and then pick the second item
											# it will basically pick the rating value and put that
											# into the ratings.
											# ratings is our new RDD.
result = ratings.countByValue() # it is used for counting the ratings by
								# value
# lines and ratings are RDDs whereas result is plain old python object
sortedResults = collections.OrderedDict(sorted(result.items())) # it iwll sort 
							# the items so obtained from ratings.countByValue()
# we use the collections package to sort those key value pair
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
