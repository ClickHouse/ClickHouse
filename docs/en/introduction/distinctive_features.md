# Distinctive Features of ClickHouse

## True Column-oriented DBMS

In a true column-oriented DBMS, there is no excessive data stored with the values. For example, this means that constant-length values must be supported, to avoid storing their length as additional integer next to the values. In this case, a billion UInt8 values should actually consume around 1 GB uncompressed, or this will strongly affect the CPU use. It is very important to store data compactly even when uncompressed, since the speed of decompression (CPU usage) depends mainly on the volume of uncompressed data.

This is worth noting because there are systems that can store values of different columns separately, but that can't effectively process analytical queries due to their optimization for other scenarios. Examples are HBase, BigTable, Cassandra, and HyperTable. In these systems, you will get throughput around a hundred thousand rows per second, but not hundreds of millions of rows per second.

Also note that ClickHouse is a database management system, not a single database. ClickHouse allows creating tables and databases in runtime, loading data, and running queries without reconfiguring and restarting the server.

## Data Compression

Some column-oriented DBMSs (InfiniDB CE and MonetDB) do not use data compression. However, data compression is crucial to achieve excellent performance.

## Disk Storage of Data

Many column-oriented DBMSs (such as SAP HANA and Google PowerDrill) can only work in RAM. This approach stimulates the allocation of a larger hardware budget than is actually necessary for real-time analysis. ClickHouse is designed to work on regular hard drives, which ensures low cost of ownership per gigabyte of data, but SSD and additional RAM are also utilized fully if available.

## Parallel Processing on Multiple Cores

Large queries are parallelized in a natural way, utilizing all necessary resources that are available on the current server.

## Distributed Processing on Multiple Servers

Almost none of the columnar DBMSs mentioned above have support for distributed query processing.
In ClickHouse, data can reside on different shards. Each shard can be a group of replicas that are used for fault tolerance. The query is processed on all the shards in parallel. This is transparent for the user.

## SQL Support

If you are familiar with standard SQL, we can't really talk about SQL support.
All the functions have different names.
However, this is a declarative query language based on SQL that can't be differentiated from SQL in many instances.
JOINs are supported. Subqueries are supported in FROM, IN, and JOIN clauses, as well as scalar subqueries.
Dependent subqueries are not supported.

ClickHouse supports declarative query language that is based on SQL and complies to SQL standard in many cases.
GROUP BY, ORDER BY, scalar subqueries and subqueries in FROM, IN and JOIN clauses are supported.
Correlated subqueries and window functions are not supported.

## Vector Engine

Data is not only stored by columns, but is also processed by vectors (parts of columns). This allows to achieve high CPU efficiency.

## Real-time Data Updates

ClickHouse supports tables with a primary key. In order to quickly perform queries on the range of the primary key, the data is sorted incrementally using the merge tree. Due to this, data can continually be added to the table. No locks are taken when new data is ingested.

## Index

Having a data physically sorted by primary key makes it possible to extract data for it's specific values or value ranges with low latency, less than few dozen milliseconds.

## Suitable for Online Queries

Low latency means that queries can be processed without delay and without trying to prepare answer in advance, right at the same moment while user interface page is loading. In other words, online.

## Support for Approximated Calculations

ClickHouse provides various ways to trade accuracy for performance:

1. Aggregate functions for approximated calculation of the number of distinct values, medians, and quantiles.
2. Running a query based on a part (sample) of data and getting an approximated result. In this case, proportionally less data is retrieved from the disk.
3. Running an aggregation for a limited number of random keys, instead of for all keys. Under certain conditions for key distribution in the data, this provides a reasonably accurate result while using fewer resources.

## Data Replication and Integrity

ClickHouse uses asynchronous multimaster replication. After being written to any available replica, data is distributed to all the other replicas in background. The system maintains identical data on different replicas. Data is restored automatically after most failures, or semiautomatically in complicated cases.

For more information, see the [Data replication](../operations/table_engines/replication.md#table_engines-replication) section.

