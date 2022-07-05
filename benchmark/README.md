# ClickBench: a Benchmark For Analytical Databases

## Overview

The benchmark represents typical workload in the following areas: click stream and traffic analysis, web analytics, machine-generated data, structured logs and events data. It covers the typical queries in ad-hoc analytics and realtime dashboards.

The dataset from this benchmark is obtained from the real traffic recording of one of the world largest web analysis platform. It has been anonymized while keeping all the important distributions of the data. The set of queries was improvised to reflect the realistic workloads, while the queries are not directly from production.

The main goals of this benchmark are:

### Reproducibility

Every test can be easily reproduced in near 20 minutes in semi-automated way. The test setup is documented and using inexpensive cloud VMs. The test process is documented in a form of a shell script, covering installation of every system, loading of the data, running the workload and collecting the result numbers. The dataset is published in multiple formats. 

### Compatibility

The tables and queries are using mostly standard SQL and require minimum or no adaptation for most of SQL DBMS. The dataset has been filtered to avoid caveats with parsing and loading.

### Diversity

The benchmark process is easy enough to cover a wide range of systems. It includes: modern and historical self-managed OLAP DBMS; traditional OLTP DBMS are included for comparison baseline; managed databases as a service offerings are included, as well as serverless cloud-native databases; some NoSQL, document and specialized time-series databases are included as well for a reference, even if they should not be comparable on the same workload.

### Realism

The dataset is derived from real production data. The realistic data distributions allow to correctly account for compression, indices, codecs, custom data structures, etc. which is not possible with most of the random dataset generators. The workload consists of 43 queries and can test the effiency of full scan and filtered scan, as well as index lookups, and main relational operations. It can test various aspects of hardware as well: some queries require high storage throughput; some queries benefit from large number of CPU cores and some benefit from single-core speed; some queries benefit from main memory bandwidth.   

## Limitations

The limitations of this benchmark allow to keep it easy to reproduce and to include more systems in the comparison. The benchmark represents only a subset of all possible workloads and scenarios. While it aims to be as fair as possible, the focus on a specific subset of workloads may give advantage to the systems specialized on that workloads.

The following limitations should be acknowledged:

1. The dataset is represented by one flat table. This is not representative for classical data warehouses which are using a normalized star or snowflake data model. The systems for classical data warehouses may get unfair disadvantage on this benchmark.

2. The table consists of exactly 99 997 497 records. This is rather small by modern standard, but allows to perform tests in reasonable time.

3. While this benchmark allows testing distributed systems, and it includes multi-node and serverless cloud-native setups, most of the results so far have been obtained on a single node setup. 

4. The benchmark runs queries one after another and does not test a workload with concurrent requests; neither does it test for system capacity. Every query is run only a few times and this allows some variability in the results. 

6. Many setups and systems are different enough to make direct comparison tricky. It is not possible to test the efficiency of storage use for in-memory databases, or the time of data loading for stateless query engines. The goal of the benchmark is to give the numbers for comparison and let you derive the conclusions on your own.

TLDR: *All Benchmarks Are Bastards*.

## Rules and Contribution

### How to add a new result

To introduce a new system, simply copy-paste one of the directories and edit the files accordingly:
- `benchmark.sh`: this is the main script to run the benchmark on a fresh VM; Ubuntu 22.04 or newer should be used by default, or any other system if this is specified in the comments. The script is not necessarily can be run in a fully automated manner - it is recommended to always copy-paste the commands one by one and observe the results. For managed databases, if the setup requires clicking on the UI, write a `README.md` instead.
- `README.md`: contains comments and observations if needed. For managed databases, it can describe the setup procedure instead of a shell script.
- `create.sql`: a CREATE TABLE statement. If it's a NoSQL system, another file like `wtf.json` can be presented.
- `queries.sql`: contains 43 queries to run;
- `run.sh`: a loop of running the queries; every query is run three times; if it's a database with local on-disk storage, the first query should be run after dropping the page cache;
- `results`: put the .txt files with the results for every hardware configuration there.

To introduce a new result for existing system on different hardware configuration, add a new file to `results`.

To introduce a new result for existing system with different usage scenario, either copy-paste the whole directory and name it differently (e.g. `timescaledb`, `timescaledb-compression`) or add a new file to the `results` directory.

### Installation and fine-tuning

The systems can be installed or used in any reasonable way: from binary distribution, from Docker container, from package manager, or compiled - whatever is more natural and simple or gives better results.

It's better to use the default settings and avoid fine-tuning. Configuration changes can be applied if it is considered strictly necessary and documented.

Fine-tuning and optimization for the benchmark is not recommended but allowed. In this case, add the results on vanilla configuration and fine-tuned configuration separately.

### Indexing

The benchmark table has one index - the primary key. The primary key not necessary to be unique. The index of the primary key can be made clustered / ordered / partitioned / sharded.

Manual creation of other indices is not recommended, although if the system create indexes automatically, it is considered ok.

### Preaggregation

Creation of preaggregated tables or indices, projections or materialized views is not recommended for the purpose of this benchmark. Although you can add results on fine-tuned setup for the reference, but they will be out of competition.

If a system is of "multidimensional OLAP" kind, so always or implicitly doing preaggregations, it can be added for comparison.

### Caching

If the system contains a cache for query results, it should be disabled.

If the system performs caching for source data, it is ok. If the cache can be flushed, it should be flushed before the first run of every query.

If the system contains a cache for intermediate data, it should be disabled if this cache is located near the end of the query execution pipeline, thus similar to a query result cache. 

### Incomplete Results

Many systems cannot run full benchmark suite successfully due to OOMs, crashes or unsupported queries. The partial results should be included nevertheless. Put `null` for the missing numbers.

### If The Results Cannot Be Published

Some vendors don't allow publishing the benchmark results due to the infamous [DeWitt Clause](https://cube.dev/blog/dewitt-clause-or-can-you-benchmark-a-database). Most of them still allow to use the system for benchmarks. In this case, please submit the full information about installation and reproduction, but without the `results` directory. A `.gitignore` file can be added to prevent accidental publishing.

We allow both open-source and proprietary systems in our benchmark, as well as managed services, even if registration, credit card or salesperson call is required - you still can submit the testing description if you don't violate the ToS.

Please let us know if some results were published by mistake by opening an issue on GitHub.

### If a Mistake Or Misrepresentation Is Found

It is easy to accidentally misrepresent some systems. While acting in a good faith, the authors admit their lack of deep knowledge of most systems. Please send a pull request to correct the mistakes.

### Results Usage And Scoreboards

The results can be used for comparison of various systems, but always take them with a grain of salt due to vast amount of caveats and hidden details. Always reference the original benchmark and this text.

We allow but not recommend to create scoreboards from this benchmark or tell that one system is better (faster, cheaper, etc) than another.

## History and Motivation

The benchmark has been created in October 2013 for evaluating various DBMS to use for a web analytics system. It has been made by taking a 1/50th of one week of production pageviews (a.k.a. "hits") data and taking the first one billion, one hundred million and ten million records from it. It has been run on a 3-node cluster of Xeon E2650v2 with 128 GiB RAM, 8x6TB HDD in md-RAID-6 and 10 Gbit network in a private datacenter in Finland.

The following systems were tested in 2013: ClickHouse, MonetDB, InfiniDB, Infobright, LucidDB, Vertica, Hive and MySQL. To ensure fairness, the benchmark has been conducted by a person without ClickHouse experience. ClickHouse has been selected for production usage by the results of this benchmark.

The benchmark continued to be occasionally used privately until 2016, when the results has been published with the ClickHouse release in open-source. While the results were made public, the datasets were not, as they contain customer data.

We needed to publish the dataset to facilitate open-source development and testing, but it was not possible to do as is. In 2019, `clickhouse-obfuscator` tool has been introduced to anonymize the data, and the dataset has been published. Read more about the challenge of data obfuscation [here](https://habr.com/en/company/yandex/blog/485096/).

More systems were included in the benchmark over time: Greenplum, MemSQL (now SingleStore), OmniSci (now HeavyAI), DuckDB, PostgreSQL and TimescaleDB.

In [2021](https://clickhouse.com/blog/introducing-click-house-inc/) the original cluster for benchmark stopped to be used, and we were unable to add new results without rerunning the old results on a different hardware. Rerunning the old results appeared to be difficult: due to natural churn of the software the old step-by-step instructions become stale.

The original benchmark dataset included many details that were natural for ClickHouse and web analytics data but hard for other systems: unsigned integers (not supported by standard SQL), strings with zero bytes, fixed length string data types, etc. Only ClickHouse is being able to load the dataset as is, while most other databases require non-trivial adjustments to the data and queries.

The idea of the new benchmark is:
- normalize the dataset to a "common denominator", so it can be loaded to most of the systems without a hassle.
- normalize the queries to use only standard SQL - they will not use any advantages of ClickHouse, but will be runnable on every system.
- ideally make it automated. At least make it simple - runnable by a short shell script that can be run by copy-pasting a few commands in the terminal, in worst case.
- run everything on widely available cloud VMs and allow to record the results from various type of instances.

The benchmark is created and used by ClickHouse team. It can be surprising, but we [did not perform](https://clickhouse.com/blog/clickhouse-over-the-years-with-benchmarks/) any specific optimizations in ClickHouse for the queries in the benchmark, which allowed to keep some reasonable sense of fairness with respect to other systems.

Now the new benchmark is easy to use and the results for any system can be reproduced in around 20 minutes.

We also introduced the [Hardware Benchmark](https://clickhouse.com/benchmark/hardware/) for testing servers and VMs.

## Systems Included

- [x] ClickHouse
- [ ] ClickHouse operating like "Athena"
- [x] clickhouse-local without data loading
- [x] MySQL InnoDB
- [x] MySQL MyISAM
- [ ] MariaDB
- [x] MariaDB ColumnStore
- [x] MemSQL/SingleStore
- [x] PostgreSQL
- [x] Greenplum
- [x] TimescaleDB
- [x] Citus
- [x] Vertica (without publishing)
- [x] QuestDB
- [x] DuckDB
- [x] MonetDB
- [x] mapD/Omnisci/HeavyAI
- [x] Databend
- [ ] Doris/PALO
- [x] Druid
- [ ] Pinot
- [x] CrateDB
- [ ] Spark SQL
- [ ] Starrocks
- [ ] ShitholeDB
- [ ] Hive
- [ ] Impala
- [ ] Hyper
- [x] SQLite
- [x] Redshift
- [x] Redshift Serverless
- [ ] Presto/Trino
- [x] Amazon Athena
- [x] Bigquery (without publishing)
- [x] Snowflake
- [ ] Rockset
- [ ] CockroachDB
- [ ] CockroachDB Serverless
- [ ] Databricks
- [ ] Planetscale (without publishing)
- [ ] TiDB (TiFlash)
- [x] Amazon RDS Aurora for MySQL
- [x] Amazon RDS Aurora for Postgres
- [ ] InfluxDB
- [ ] VictoriaMetrics
- [ ] TDEngine
- [ ] MongoDB
- [ ] Cassandra
- [ ] ScyllaDB
- [ ] Elasticsearch
- [ ] Apache Ignite
- [ ] Infobright
- [ ] Actian Vector
- [ ] Manticore Search
- [x] Vertica (without publishing)
- [ ] Azure Synapse
- [ ] Starburst Galaxy
- [ ] MS SQL Server with Column Store Index (without publishing)
- [ ] Dremio (without publishing) 
- [ ] Exasol
- [ ] LocustDB
- [ ] EventQL
- [ ] Apache Drill

By default, all tests are run on c6a.4xlarge VM in AWS with 500 GB gp2.

Please help us add more systems and running the benchmarks on more types of VMs. 

## Similar Projects

There are many alternative benchmarks applicable to OLAP DBMS with their own advantages and disadvantages.

### Brown University Mgbench

https://github.com/crottyan/mgbench

A new analytical benchmark for machine-generated log data. By Andrew Crottyan from Brown University.

Advantages:
- somewhat realistic dataset;
- diverse set of queries;
- good coverage of systems;
- easy to reproduce;

Disadvantages:
- very small dataset size;
- favors in-memory databases;
- mostly abandoned.

### UC Berkeley AMPLab Big Data Benchmark

https://amplab.cs.berkeley.edu/benchmark/

Poor coverage of queries that are too simple. The benchmark is abandoned. 

### Mark Litwinschik's NYC Taxi

https://tech.marksblogg.com/benchmarks.html

Advantages:
- real-world dataset;
- good coverage of systems; many unusual entries;
- contains a story for every benchmark entry;

Disadvantages:
- unreasonably small set of queries: 4 mostly trivial queries don't represent any realistic workload and are subjects for overoptimizations;
- compares different systems on different hardware;
- many results are outdated;
- no automated or easy way to reproduce the results;
- while many results are performed independently of corporations or academia, some benchmark entries may have been sponsored;
- the dataset is not readily available for downloads: originally 1.1 billion records are used, while it's more 4 billion records in 2022.

### Database-like ops Benchmark from h2o.ai

https://h2oai.github.io/db-benchmark/

A benchmark for dataframe libraries and embedded databases. Good coverage of dataframe libraries and a few full featured DBMS as well.

### A benchmark for querying large JSON datasets

https://colab.research.google.com/github/dcmoura/spyql/blob/master/notebooks/json_benchmark.ipynb

A good benchmark for command line tools for processing semistructured data. Can be used to test DBMS as well.

### Star Schema Benchmark

Pat O'Neil, Betty O'Neil, Xuedong Chen
https://www.cs.umb.edu/~poneil/StarSchemaB.PDF

It is a simplified version of TPC-H.

Advantages:
- well specified;
- popular in academia;

Disadvantages:
- represents a classic data warehouse schema;
- database generator produces random distributions that are not realistic and the benchmark does not allow to capture the difference in various optimizations that matter on real world data;
- many research systems in academia targeting for this benchmark which makes many aspects of it exhausted;

### TPC-H

A benchmark suite from Transaction Processing Council - one of the oldest organization specialized in DBMS benchmarks.

Advantages:
- well specified;

Disadvantages:
- requires official certification;
- represents a classic data warehouse schema;
- database generator produces random distributions that are not realistic and the benchmark does not allow to capture the difference in various optimizations that matter on real world data;
- many systems are targeting for this benchmark which makes many aspects of it exhausted;

### TPC-DS

More advanced than TPC-H, focused on complex ad-hoc queries. Requires official certification as well.

Advantages:
- extensive collection of complex queries.

Disadvantages:
- requires official certification;
- official results have only sparse coverage of systems;
- biased towards complex queries over many tables.

### A benchmark on Ontime Flight Data

Introduced by Vadim Tkachenko from Percona [in 2009](https://www.percona.com/blog/2009/10/02/analyzing-air-traffic-performance-with-infobright-and-monetdb/).

Based on the US Bureau of Transportation Statistics open data.

Advantages:
- real-world dataset;

Disadvantages:
- not widely used;
- the set of queries is not standardized;
- the table contains too much redundancy;

### TSBS

Time Series Benchmark Suite. https://github.com/timescale/tsbs
Originally from InfluxDB, and supported by TimescaleDB.

Advantages:
- a benchmark for time-series scenarios

Disadvantages:
- not applicable for scenarios with data analytics. 

### STAC

https://www.stacresearch.com/

Disadvantages:
- requires paid membership. 

### More

If you know more well-defined, realistic and reproducible benchmarks for analytical workloads, please let me know.

I collect every benchmark that includes ClickHouse [here](https://github.com/ClickHouse/ClickHouse/issues/22398).

### References and Citation.

Alexey Milovidov, 2022.
