---
toc_priority: 0
toc_title: Overview
---

# What Is ClickHouse? {#what-is-clickhouse}

ClickHouse is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).

In a “normal” row-oriented DBMS, data is stored in this order:

| Row | WatchID     | JavaEnable | Title              | GoodEvent | EventTime           |
|-----|-------------|------------|--------------------|-----------|---------------------|
| \#0 | 89354350662 | 1          | Investor Relations | 1         | 2016-05-18 05:19:20 |
| \#1 | 90329509958 | 0          | Contact us         | 1         | 2016-05-18 08:10:20 |
| \#2 | 89953706054 | 1          | Mission            | 1         | 2016-05-18 07:38:00 |
| \#N | …           | …          | …                  | …         | …                   |

In other words, all the values related to a row are physically stored next to each other.

Examples of a row-oriented DBMS are MySQL, Postgres, and MS SQL Server.

In a column-oriented DBMS, data is stored like this:

| Row:        | \#0                 | \#1                 | \#2                 | \#N |
|-------------|---------------------|---------------------|---------------------|-----|
| WatchID:    | 89354350662         | 90329509958         | 89953706054         | …   |
| JavaEnable: | 1                   | 0                   | 1                   | …   |
| Title:      | Investor Relations  | Contact us          | Mission             | …   |
| GoodEvent:  | 1                   | 1                   | 1                   | …   |
| EventTime:  | 2016-05-18 05:19:20 | 2016-05-18 08:10:20 | 2016-05-18 07:38:00 | …   |

These examples only show the order that data is arranged in. The values from different columns are stored separately, and data from the same column is stored together.

Examples of a column-oriented DBMS: Vertica, Paraccel (Actian Matrix and Amazon Redshift), Sybase IQ, Exasol, Infobright, InfiniDB, MonetDB (VectorWise and Actian Vector), LucidDB, SAP HANA, Google Dremel, Google PowerDrill, Druid, and kdb+.

Different orders for storing data are better suited to different scenarios. The data access scenario refers to what queries are made, how often, and in what proportion; how much data is read for each type of query – rows, columns, and bytes; the relationship between reading and updating data; the working size of the data and how locally it is used; whether transactions are used, and how isolated they are; requirements for data replication and logical integrity; requirements for latency and throughput for each type of query, and so on.

The higher the load on the system, the more important it is to customize the system set up to match the requirements of the usage scenario, and the more fine grained this customization becomes. There is no system that is equally well-suited to significantly different scenarios. If a system is adaptable to a wide set of scenarios, under a high load, the system will handle all the scenarios equally poorly, or will work well for just one or few of possible scenarios.

## Key Properties of OLAP Scenario {#key-properties-of-olap-scenario}

-   The vast majority of requests are for read access.
-   Data is updated in fairly large batches (\> 1000 rows), not by single rows; or it is not updated at all.
-   Data is added to the DB but is not modified.
-   For reads, quite a large number of rows are extracted from the DB, but only a small subset of columns.
-   Tables are “wide,” meaning they contain a large number of columns.
-   Queries are relatively rare (usually hundreds of queries per server or less per second).
-   For simple queries, latencies around 50 ms are allowed.
-   Column values are fairly small: numbers and short strings (for example, 60 bytes per URL).
-   Requires high throughput when processing a single query (up to billions of rows per second per server).
-   Transactions are not necessary.
-   Low requirements for data consistency.
-   There is one large table per query. All tables are small, except for one.
-   A query result is significantly smaller than the source data. In other words, data is filtered or aggregated, so the result fits in a single server’s RAM.

It is easy to see that the OLAP scenario is very different from other popular scenarios (such as OLTP or Key-Value access). So it doesn’t make sense to try to use OLTP or a Key-Value DB for processing analytical queries if you want to get decent performance. For example, if you try to use MongoDB or Redis for analytics, you will get very poor performance compared to OLAP databases.

## Why Column-Oriented Databases Work Better in the OLAP Scenario {#why-column-oriented-databases-work-better-in-the-olap-scenario}

Column-oriented databases are better suited to OLAP scenarios: they are at least 100 times faster in processing most queries. The reasons are explained in detail below, but the fact is easier to demonstrate visually:

**Row-oriented DBMS**

![Row-oriented](images/row-oriented.gif#)

**Column-oriented DBMS**

![Column-oriented](images/column-oriented.gif#)

See the difference?

### Input/output {#inputoutput}

1.  For an analytical query, only a small number of table columns need to be read. In a column-oriented database, you can read just the data you need. For example, if you need 5 columns out of 100, you can expect a 20-fold reduction in I/O.
2.  Since data is read in packets, it is easier to compress. Data in columns is also easier to compress. This further reduces the I/O volume.
3.  Due to the reduced I/O, more data fits in the system cache.

For example, the query “count the number of records for each advertising platform” requires reading one “advertising platform ID” column, which takes up 1 byte uncompressed. If most of the traffic was not from advertising platforms, you can expect at least 10-fold compression of this column. When using a quick compression algorithm, data decompression is possible at a speed of at least several gigabytes of uncompressed data per second. In other words, this query can be processed at a speed of approximately several billion rows per second on a single server. This speed is actually achieved in practice.

### CPU {#cpu}

Since executing a query requires processing a large number of rows, it helps to dispatch all operations for entire vectors instead of for separate rows, or to implement the query engine so that there is almost no dispatching cost. If you don’t do this, with any half-decent disk subsystem, the query interpreter inevitably stalls the CPU. It makes sense to both store data in columns and process it, when possible, by columns.

There are two ways to do this:

1.  A vector engine. All operations are written for vectors, instead of for separate values. This means you don’t need to call operations very often, and dispatching costs are negligible. Operation code contains an optimized internal cycle.

2.  Code generation. The code generated for the query has all the indirect calls in it.

This is not done in “normal” databases, because it doesn’t make sense when running simple queries. However, there are exceptions. For example, MemSQL uses code generation to reduce latency when processing SQL queries. (For comparison, analytical DBMSs require optimization of throughput, not latency.)

Note that for CPU efficiency, the query language must be declarative (SQL or MDX), or at least a vector (J, K). The query should only contain implicit loops, allowing for optimization.

{## [Original article](https://clickhouse.tech/docs/en/) ##}
