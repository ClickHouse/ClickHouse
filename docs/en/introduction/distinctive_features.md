# Distinctive Features of ClickHouse

## True Column-oriented DBMS

In a true column-oriented DBMS, no extra data is stored with the values. Among other things, this means that constant-length values must be supported, to avoid storing their length "number" next to the values. As an example, a billion UInt8-type values should actually consume around 1 GB uncompressed, or this will strongly affect the CPU use. It is very important to store data compactly (without any "garbage") even when uncompressed, since the speed of decompression (CPU usage) depends mainly on the volume of uncompressed data.

This is worth noting because there are systems that can store values of separate columns separately, but that can't effectively process analytical queries due to their optimization for other scenarios. Examples are HBase, BigTable, Cassandra, and HyperTable. In these systems, you will get throughput around a hundred thousand rows per second, but not hundreds of millions of rows per second.

It's also worth noting that ClickHouse is a database management system, not a single database. ClickHouse allows creating tables and databases in runtime, loading data, and running queries without reconfiguring and restarting the server.

## Data Compression

Some column-oriented DBMSs (InfiniDB CE and MonetDB) do not use data compression. However, data compression does play a key role in achieving excellent performance.

## Disk Storage of Data

Many column-oriented DBMSs (such as SAP HANA and Google PowerDrill) can only work in RAM. This approach encourages more budgeting for hardware than is actually needed for real-time analysis. ClickHouse is designed to work on normal hard drives, which means the cost per GB of data storage is low, but SSD b additional RAM is also fully used if available.

## Parallel Processing on Multiple Cores

Large queries are parallelized in a natural way, taking all the necessary resources from what is available on the server.

## Distributed Processing on Multiple Servers

Almost none of the columnar DBMSs listed above have support for distributed processing.
In ClickHouse, data can reside on different shards. Each shard can be a group of replicas that are used for fault tolerance. The query is processed on all the shards in parallel. This is transparent for the user.

## SQL Support

ClickHouse supports a declarative query language based on SQL that is identical to the SQL standard in many cases.
Supported queries include GROUP BY, ORDER BY, subqueries in FROM, IN, and JOIN clauses, and scalar subqueries.
Dependent subqueries and window functions are not supported.

## Vector Engine

Data is not only stored by columns, but is processed by vectors (parts of columns). This allows us to achieve high CPU performance.

## Real-time Data Updates

ClickHouse supports primary key tables. In order to quickly perform queries on the range of the primary key, the data is sorted incrementally using the merge tree. Due to this, data can continually be added to the table. There is no locking when adding data.

## Index

Physical sorting of data by primary key allows you to get data for specific key values or ranges of values with low latency of less than several dozen milliseconds.

## Suitable for Online Queries

Low latency means queries can be processed without delay and without preparing the response ahead of time, so a query can be processed while the user interface page is loading. In other words, in online mode.

## Support for Approximated Calculations

ClickHouse provides various ways to change the precision of calculations for improved performance:

1. The system contains aggregate functions for approximated calculation of the number of various values, medians, and quantiles.
2. Supports running a query based on a part (sample) of data and getting an approximated result. In this case, proportionally less data is retrieved from the disk.
3. Supports running an aggregation for a limited number of random keys, instead of for all keys. Under certain conditions for key distribution in the data, this provides a reasonably accurate result while using fewer resources.

## Data replication and data integrity support

Uses asynchronous multimaster replication. After being written to any available replica, data is distributed to all the remaining replicas in the background. The system maintains identical data on different replicas. Recovery after most failures is performed automatically, and in complex cases — semi-automatically.

For more information, see the section [Data replication](../operations/table_engines/replication.md#table_engines-replication).
