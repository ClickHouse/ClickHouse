# General Questions

## Why Not Use Something Like MapReduce?

We can refer to systems like MapReduce as distributed computing systems in which the reduce operation is based on a distributed sort. The most common opensource solution of this kind is [Apache Hadoop](http://hadoop.apache.org), while Yandex internally uses it's own MapReduce implementation — YT.

The systems of this kind are not suitable for online queries due to their high latency. In other words, they can't be used as the back-end for a web interface.

Distributed sorting isn't the best way to perform reduce operations if the result of the operation and all the intermediate results (if there are any) are located in the RAM of a single server, which is usually the case for online queries. In such a case, a hash table is the optimal way to perform reduce operations. A common approach to optimizing MapReduce tasks is pre-aggregation (partial reduce) using a hash table in RAM. The user performs this optimization manually.
Distributed sorting is one of the main causes of reduced performance when running simple MapReduce tasks.

Most MapReduce implementations allow executing any code on the cluster. But a declarative query language is better suited to OLAP in order to run experiments quickly. For example, Hadoop has Hive and Pig. Also consider Cloudera Impala, Shark (outdated) for Spark, and Spark SQL, Presto, and Apache Drill. Performance when running such tasks is highly sub-optimal compared to specialized systems, but relatively high latency makes it unrealistic to use these systems as the backend for a web interface.
