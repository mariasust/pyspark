from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def get_scores(w):
    subreddit=w["subreddit"]
    score=w["score"]
    return subreddit,(1,score)

def add_pairs(w1,w2):
    count = w1[0] + w2[0]
    score = w1[1] + w2[1]
    return count,score

def calculate_avg(w):
    average = w[1][1] / w[1][0]
    return w[0],average

def get_key(w):
    return w[0]

def main(inputs, output):
    text = sc.textFile(inputs)
    json_value = text.map(json.loads).map(get_scores)
    pair_values = json_value.reduceByKey(add_pairs).coalesce(1)
    outdata = pair_values.sortBy(get_key).map(calculate_avg).map(json.dumps)


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit score')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
