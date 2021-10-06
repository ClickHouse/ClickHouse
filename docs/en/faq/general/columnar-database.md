---
title: What is a columnar database?
toc_hidden: true
toc_priority: 101
---

# What Is a Columnar Database? {#what-is-a-columnar-database}

A columnar database stores data of each column independently. This allows to read data from disks only for those columns that are used in any given query. The cost is that operations that affect whole rows become proportionally more expensive. The synonym for a columnar database is a column-oriented database management system. ClickHouse is a typical example of such a system.

Key columnar database advantages are:

-   Queries that use only a few columns out of many.
-   Aggregating queries against large volumes of data.
-   Column-wise data compression.

Here is the illustration of the difference between traditional row-oriented systems and columnar databases when building reports:

**Traditional row-oriented**
![Traditional row-oriented](https://clickhouse.tech/docs/en/images/row-oriented.gif#)

**Columnar**
![Columnar](https://clickhouse.tech/docs/en/images/column-oriented.gif#)

A columnar database is a preferred choice for analytical applications because it allows to have many columns in a table just in case, but do not pay the cost for unused columns on read query execution time. Column-oriented databases are designed for big data processing because and data warehousing, they often natively scale using distributed clusters of low-cost hardware to increase throughput. ClickHouse does it with combination of [distributed](../../engines/table-engines/special/distributed.md) and [replicated](../../engines/table-engines/mergetree-family/replication.md) tables.
