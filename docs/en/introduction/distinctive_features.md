# Distinctive features of ClickHouse

## True column-oriented DBMS.

In a true column-oriented DBMS, there isn't any "garbage" stored with the values. For example, constant-length values must be supported, to avoid storing their length "number" next to the values. As an example, a billion UInt8-type values should actually consume around 1 GB uncompressed, or this will strongly affect the CPU use. It is very important to store data compactly (without any "garbage") even when uncompressed, since the speed of decompression (CPU usage) depends mainly on the volume of uncompressed data.

This is worth noting because there are systems that can store values of separate columns separately, but that can't effectively process analytical queries due to their optimization for other scenarios. Example are HBase, BigTable, Cassandra, and HyperTable. In these systems, you will get throughput around a hundred thousand rows per second, but not hundreds of millions of rows per second.

Also note that ClickHouse is a DBMS, not a single database. ClickHouse allows creating tables and databases in runtime, loading data, and running queries without reconfiguring and restarting the server.

## Data compression

Some column-oriented DBMSs (InfiniDB CE and MonetDB) do not use data compression. However, data compression really improves performance.

## Disk storage of data.

Many column-oriented DBMSs (such as SAP HANA and Google PowerDrill) can only work in RAM. But even on thousands of servers, the RAM is too small for storing all the pageviews and sessions in Yandex.Metrica.

## Parallel processing on multiple cores.

Large queries are parallelized in a natural way.

## Distributed processing on multiple servers.

Almost none of the columnar DBMSs listed above have support for distributed processing.
In ClickHouse, data can reside on different shards. Each shard can be a group of replicas that are used for fault tolerance. The query is processed on all the shards in parallel. This is transparent for the user.

## SQL support

If you are familiar with standard SQL, we can't really talk about SQL support.
All the functions have different names.
However, this is a declarative query language based on SQL that can't be differentiated from SQL in many instances.
Support for JOINs. Subqueries are supported in FROM, IN, and JOIN clauses, as well as scalar subqueries.
Dependent subqueries are not supported.

## Vector engine

Data is not only stored by columns, but is processed by vectors – parts of columns. This allows us to achieve high CPU performance.

## Real-time data updates

ClickHouse supports primary key tables. In order to quickly perform queries on the range of the primary key, the data is sorted incrementally using the merge tree. Due to this, data can continually be added to the table. There is no locking when adding data.

## Indexes

Having a primary key allows, for example, extracting data for specific clients (Metrica counters) for a specific time range, with low latency less than several dozen milliseconds.

## Suitable for online queries

This lets us use the system as the back-end for a web interface. Low latency means queries can be processed without delay, while the Yandex.Metrica interface page is loading. In other words, in online mode.

## Support for approximated calculations.

1. The system contains aggregate functions for approximated calculation of the number of various values, medians, and quantiles.
2. Supports running a query based on a part (sample) of data and getting an approximated result. In this case, proportionally less data is retrieved from the disk.
3. Supports running an aggregation for a limited number of random keys, instead of for all keys. Under certain conditions for key distribution in the data, this provides a reasonably accurate result while using fewer resources.

## Data replication and support for data integrity on replicas

Uses asynchronous multimaster replication. After being written to any available replica, data is distributed to all the remaining replicas. The system maintains identical data on different replicas. Data is restored automatically after a failure, or using a "button" for complex cases.
For more information, see the section [Data replication](../table_engines/replication.md#table_engines-replication).

