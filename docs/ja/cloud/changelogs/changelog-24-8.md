---
slug: /ja/changelogs/24.8
title: v24.8 Changelog for Cloud
description: Fast release changelog for v24.8
keywords: [chaneglog, cloud]
---

Relevant changes for ClickHouse Cloud services based on the v24.8 release.

## Backward Incompatible Change

- Change binary serialization of Variant data type: add compact mode to avoid writing the same discriminator multiple times for granules with single variant or with only NULL values. Add MergeTree setting use_compact_variant_discriminators_serialization that is enabled by default. Note that Variant type is still experimental and backward-incompatible change in serialization should not impact you unless you have been working with support to get this feature enabled earlier. [#62774](https://github.com/ClickHouse/ClickHouse/pull/62774) (Kruglov Pavel).

- Forbid CREATE MATERIALIZED VIEW ... ENGINE Replicated*MergeTree POPULATE AS SELECT ... with Replicated databases. This specific PR is only applicable to users still using, ReplicatedMergeTree. [#63963](https://github.com/ClickHouse/ClickHouse/pull/63963) (vdimir).

- Metric KeeperOutstandingRequets was renamed to KeeperOutstandingRequests. This fixes a typo reported in [#66179](https://github.com/ClickHouse/ClickHouse/issues/66179). [#66206](https://github.com/ClickHouse/ClickHouse/pull/66206) (Robert Schulze).

- clickhouse-client and clickhouse-local now default to multi-query mode (instead single-query mode). As an example, clickhouse-client -q "SELECT 1; SELECT 2" now works, whereas users previously had to add --multiquery (or -n). The --multiquery/-n switch became obsolete. INSERT queries in multi-query statements are treated specially based on their FORMAT clause: If the FORMAT is VALUES (the most common case), the end of the INSERT statement is represented by a trailing semicolon ; at the end of the query. For all other FORMATs (e.g. CSV or JSONEachRow), the end of the INSERT statement is represented by two newlines \n\n at the end of the query. [#63898](https://github.com/ClickHouse/ClickHouse/pull/63898) (wxybear).

- In previous versions, it was possible to use an alternative syntax for LowCardinality data types by appending WithDictionary to the name of the data type. It was an initial working implementation, and it was never documented or exposed to the public. Now, it is deprecated. If you have used this syntax, you have to ALTER your tables and rename the data types to LowCardinality. [#66842](https://github.com/ClickHouse/ClickHouse/pull/66842)(Alexey Milovidov).

- Fix logical errors with storage Buffer used with distributed destination table. It's a backward incompatible change: queries using Buffer with a distributed destination table may stop working if the table appears more than once in the query (e.g., in a self-join). [#67015](https://github.com/vdimir) (vdimir).

- In previous versions, calling functions for random distributions based on the Gamma function (such as Chi-Squared, Student, Fisher) with negative arguments close to zero led to a long computation or an infinite loop. In the new version, calling these functions with zero or negative arguments will produce an exception. This closes [#67297](https://github.com/ClickHouse/ClickHouse/issues/67297). [#67326](https://github.com/ClickHouse/ClickHouse/pull/67326) (Alexey Milovidov).

- In previous versions, arrayWithConstant can be slow if asked to generate very large arrays. In the new version, it is limited to 1 GB per array. This closes [#32754](https://github.com/ClickHouse/ClickHouse/issues/32754). [#67741](https://github.com/ClickHouse/ClickHouse/pull/67741) (Alexey Milovidov).

- Fix REPLACE modifier formatting (forbid omitting brackets). [#67774](https://github.com/ClickHouse/ClickHouse/pull/67774) (Azat Khuzhin).


## New Feature

- Extend function tuple to construct named tuples in query. Introduce function tupleNames to extract names from tuples. [#54881](https://github.com/ClickHouse/ClickHouse/pull/54881) (Amos Bird).

- ASOF JOIN support for full_sorting_join algorithm Close [#54493](https://github.com/ClickHouse/ClickHouse/issues/54493). [#55051](https://github.com/ClickHouse/ClickHouse/pull/55051) (vdimir).

- A new table function, fuzzQuery, was added. This function allows you to modify a given query string with random variations. Example: SELECT query FROM fuzzQuery('SELECT 1');. [#62103](https://github.com/ClickHouse/ClickHouse/pull/62103) (pufit).

- Add new window function percent_rank. [#62747](https://github.com/ClickHouse/ClickHouse/pull/62747) (lgbo).

- Support JWT authentication in clickhouse-client. [#62829](https://github.com/ClickHouse/ClickHouse/pull/62829) (Konstantin Bogdanov).

- Add SQL functions changeYear, changeMonth, changeDay, changeHour, changeMinute, changeSecond. For example, SELECT changeMonth(toDate('2024-06-14'), 7) returns date 2024-07-14. [#63186](https://github.com/ClickHouse/ClickHouse/pull/63186) (cucumber95).

- Add system.error_log which contains history of error values from table system.errors, periodically flushed to disk. [#65381](https://github.com/ClickHouse/ClickHouse/pull/65381) (Pablo Marcos).

- Add aggregate function groupConcat. About the same as arrayStringConcat( groupArray(column), ',') Can receive 2 parameters: a string delimiter and the number of elements to be processed. [#65451](https://github.com/ClickHouse/ClickHouse/pull/65451) (Yarik Briukhovetskyi).

- Add AzureQueue storage. [#65458](https://github.com/ClickHouse/ClickHouse/pull/65458) (Kseniia Sumarokova).

- Add a new setting to disable/enable writing page index into parquet files. [#65475](https://github.com/ClickHouse/ClickHouse/pull/65475) (lgbo).

- Automatically append a wildcard * to the end of a directory path with table function file. [#66019](https://github.com/ClickHouse/ClickHouse/pull/66019) (Zhidong (David) Guo).

- Add --memory-usage option to client in non interactive mode. [#66393](https://github.com/ClickHouse/ClickHouse/pull/66393) (vdimir).

- Add _etag virtual column for S3 table engine. Fixes [#65312](https://github.com/ClickHouse/ClickHouse/issues/65312). [#65386](https://github.com/ClickHouse/ClickHouse/pull/65386) (skyoct)

- This pull request introduces Hive-style partitioning for different engines (File, URL, S3, AzureBlobStorage, HDFS). Hive-style partitioning organizes data into partitioned sub-directories, making it efficient to query and manage large datasets. Currently, it only creates virtual columns with the appropriate name and data. The follow-up PR will introduce the appropriate data filtering (performance speedup). [#65997](https://github.com/ClickHouse/ClickHouse/pull/65997) (Yarik Briukhovetskyi).

- Add function printf for spark compatiability. [#66257](https://github.com/ClickHouse/ClickHouse/pull/66257) (李扬).

- Added support for reading MULTILINESTRING geometry in WKT format using function readWKTLineString. [#67647](https://github.com/ClickHouse/ClickHouse/pull/67647) (Jacob Reckhard).

- Added a tagging (namespace) mechanism for the query cache. The same queries with different tags are considered different by the query cache. Example: SELECT 1 SETTINGS use_query_cache = 1, query_cache_tag = 'abc' and SELECT 1 SETTINGS use_query_cache = 1, query_cache_tag = 'def' now create different query cache entries. [#68235](https://github.com/ClickHouse/ClickHouse/pull/68235)(sakulali).