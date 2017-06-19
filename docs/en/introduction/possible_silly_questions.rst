Possible silly questions
------------------------

Why not to use systems like MapReduce?
""""""""""""""""""""""""""""""""""""""

Systems like map-reduce are distributed computing systems, where the reduce phase is performed using distributed sorting.
Regarding this aspect, map-reduce is similar to other systems like YAMR, Hadoop, YT.

These systems are not suitable for online queries because of latency, So they can't be used in backend-level for web interface.
Systems like this also are not suitable for real-time updates.
Distributed sorting is not optimal solution for reduce operations, if the result of the operation and all intermediate results, shall they exist, fit in operational memory of a single server, as usually happens in case of online analytical queries.
In this case the optimal way to perform reduce operations is by using a hash-table. A common optimization method for map-reduce tasks is combine operation (partial reduce) which uses hash-tables in memory. This optimization is done by the user manually.
Distributed sorting is the main reason for long latencies of simple map-reduce jobs.

Systems similar to map-reduce enable running any code on the cluster. But for OLAP use-cases declarative query languages are better suited as they allow to carry out investigations faster. For example, for Hadoop there are Hive and Pig. There are others: Cloudera Impala, Shark (deprecated) and Spark SQL for Spark, Presto, Apache Drill.
However, performance of such tasks is highly sub-optimal compared to the performance of specialized systems and relatively high latency does not allow the use of these systems as a backend for the web interface.

YT allows you to store separate groups of columns. But YT is not a truly columnar storage system, as the system has no fixed length data types (so you can efficiently store a number without "garbage"), and there is no vector engine. Tasks in YT are performed by arbitrary code in streaming mode, so can not be sufficiently optimized (up to hundreds of millions of lines per second per server). In 2014-2016 YT is to develop "dynamic table sorting" functionality  using Merge Tree, strongly typed values ​​and SQL-like language support. Dynamically sorted tables are not suited for OLAP tasks, since the data is stored in rows. Query language development in YT is still in incubating phase, which does not allow it to focus on this functionality. YT developers are considering dynamically sorted tables for use in OLTP and Key-Value scenarios.
