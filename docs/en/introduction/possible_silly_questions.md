# Everything you were afraid to ask

## Why not use something like MapReduce?

We can refer to systems like map-reduce as distributed computing systems in which the reduce operation is based on distributed sorting. In this sense, they include Hadoop and YT (Yandex proprietary technology).

These systems aren't appropriate for online queries due to their high latency. In other words, they can't be used as the back-end for a web interface.
These types of systems aren't useful for real-time data updates.
Distributed sorting isn't the best way to perform reduce operations if the result of the operation and all the intermediate results (if there are any) are located in the RAM of a single server, which is usually the case for online queries. In such a case, a hash table is the optimal way to perform reduce operations. A common approach to optimizing map-reduce tasks is pre-aggregation (partial reduce) using a hash table in RAM. The user performs this optimization manually.
Distributed sorting is one of the main causes of reduced performance when running simple map-reduce tasks.

Systems like map-reduce allow executing any code on the cluster. But a declarative query language is better suited to OLAP in order to run experiments quickly. For example, Hadoop has Hive and Pig. Also consider Cloudera Impala, Shark (outdated) for Spark, and Spark SQL, Presto, and Apache Drill. Performance when running such tasks is highly sub-optimal compared to specialized systems, but relatively high latency makes it unrealistic to use these systems as the backend for a web interface.

YT allows storing groups of columns separately. But YT can't be considered a true column-based system because it doesn't have fixed-length data types (for efficiently storing numbers without extra "garbage"), and also due to its lack of a vector engine. Tasks are performed  in YT using custom code in streaming mode, so they cannot be optimized enough (up to hundreds of millions of rows per second per server). "Dynamic table sorting" is under development in YT using MergeTree, strict value typing, and a query language similar to SQL. Dynamically sorted tables are not appropriate for OLAP tasks because the data is stored by row. The YT query language is still under development, so we can't yet rely on this functionality. YT developers are considering using dynamically sorted tables in OLTP and Key-Value scenarios.

