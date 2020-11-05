#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[2]:


from pyspark import SparkContext, SparkConf
import  snap


# In[3]:


def mapper1(line): 
    vector = line.split("\n")
    maplist = []
    for item in vector:
        node = item.split("\t")
        #print(node[0], node[1])
        maplist.append( (node[0],node[1]) )
    return maplist

def mapper2(line):
    maplist = []
    #print(line)
    out_degree = len(line[2])
    maplist.append( (line[0], line[1], out_degree, line[2]) )
    return maplist

def mapper3(line):
    maplist = []
    if in_link[int(line[0])] == 0:
        maplist.append( (line[0], 0.0) )
    for j in line[3]:
        rj = B * line[1] / line[2]
        if in_link[int(j)] == 0:
            rj = 0
        maplist.append( (j, rj) )
    return maplist

def mapper4(line):
    maplist = []
    maplist.append( (line[0], line[1:]) )
    return maplist

def mapper5(line):
    maplist = []
    if out_link[int(line[0])] != 0:
        new_rank = line[1][1]
        maplist.append( (line[0], new_rank, line[1][0][1], line[1][0][2]) )
    return maplist


# In[7]:


# setting
conf = SparkConf().setMaster("local").setAppName("MDA1").set("spark.default.parallelism", 4)

# check if sc was used
try:
    sc.stop()
    sc = SparkContext(conf=conf)
except:
    sc = SparkContext(conf=conf)
file = sc.textFile("p2p-Gnutella04.txt").filter(lambda line: '#' not in line)

    
# basic information
# Number of nodes : 10876
# Value of parameter : 0.8
# Calculate in_link and out_link
N = 10876
B = 0.8
in_link = [0]*2*N
out_link = [0]*2*N



file_txt = file.collect()
for item in file_txt:
    item = item.split("\t")
    out_link[int(item[0])] = 1
    in_link[int(item[1])] = 1

# mapper1 : (from_1, to_1), (from_1, to_2), ...
# groupByKey + map : (from_1, init_rank, [to_1, to_2, ...]), ...
# if in_link[from_1] == 0, init_rank = 0; else init_rank = 1/N
page = file.flatMap(mapper1).groupByKey().map(lambda x : ( x[0], 1/N if in_link[int(x[0])] else 0, list(x[1])  ) )

# mapper2 : (from_1, rank, out_degree_1, [to_1, to_2, ...]), ...
rdd_old = page.flatMap(mapper2)

# warning!! : There might be some to_id that doesn't exist in original from_id
# if the node doesn't have outlink then there is no reason to memorize it to the next loop, the pagerank of these kind of nodes
# only help us to calculate S
for i in range(20):    
    # mapper3 + reduceByKey : (j, rank_j_plum) ; haven't calculate S yet
    rdd_plum = rdd_old.flatMap(mapper3).reduceByKey(lambda x, y: x+y)
    
    # mapValues : calculate S and add (1-S)/N to modify rank_j
    S = rdd_plum.values().sum()
    rdd_new = rdd_plum.mapValues(lambda x: x + (1-S)/N )

    # rearrange rdd_old such that it only have one key and one value (Since we want to use join but also preserve original data)
    # update rank_i in rdd_old
    # last round we don't need to map since we already have the answers, so just skip
    if i != 19:
        rdd_tmp = rdd_old.flatMap(mapper4).leftOuterJoin(rdd_new) 
        rdd_old = rdd_tmp.flatMap(mapper5)
    

rdd_new = rdd_new.sortBy(lambda a: a[1], False)
output = rdd_new.take(10)

# file writing
f=open("output.txt","w+")
for item in output[:-1]:
    f.write( str(item[0]) + "\t" + str(item[1])[:17] + "\n" )
f.write( str(output[-1][0]) + "\t" + str(output[-1][1])[:17] )
f.close()


sc.stop()



