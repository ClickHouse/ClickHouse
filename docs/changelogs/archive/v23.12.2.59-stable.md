---
sidebar_position: 1
sidebar_label: 2024
---

# 2024 Changelog

### ClickHouse release v23.12.2.59-stable (17ab210e761) FIXME as compared to v23.12.1.1368-stable (a2faa65b080)

#### Backward Incompatible Change
* Backported in [#58389](https://github.com/ClickHouse/ClickHouse/issues/58389): The MergeTree setting `clean_deleted_rows` is deprecated, it has no effect anymore. The `CLEANUP` keyword for `OPTIMIZE` is not allowed by default (unless `allow_experimental_replacing_merge_with_cleanup` is enabled). [#58316](https://github.com/ClickHouse/ClickHouse/pull/58316) ([Alexander Tokmakov](https://github.com/tavplubix)).

#### Bug Fix (user-visible misbehavior in an official stable release)

* Flatten only true Nested type if flatten_nested=1, not all Array(Tuple) [#56132](https://github.com/ClickHouse/ClickHouse/pull/56132) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix working with read buffers in StreamingFormatExecutor [#57438](https://github.com/ClickHouse/ClickHouse/pull/57438) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix lost blobs after dropping a replica with broken detached parts [#58333](https://github.com/ClickHouse/ClickHouse/pull/58333) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Fix segfault when graphite table does not have agg function [#58453](https://github.com/ClickHouse/ClickHouse/pull/58453) ([Duc Canh Le](https://github.com/canhld94)).
* MergeTreePrefetchedReadPool disable for LIMIT only queries [#58505](https://github.com/ClickHouse/ClickHouse/pull/58505) ([Maksim Kita](https://github.com/kitaisreal)).

#### NO CL ENTRY

* NO CL ENTRY:  'Revert "Refreshable materialized views (takeover)"'. [#58296](https://github.com/ClickHouse/ClickHouse/pull/58296) ([Alexander Tokmakov](https://github.com/tavplubix)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Fix an error in the release script - it didn't allow to make 23.12. [#58288](https://github.com/ClickHouse/ClickHouse/pull/58288) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Update version_date.tsv and changelogs after v23.12.1.1368-stable [#58290](https://github.com/ClickHouse/ClickHouse/pull/58290) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Fix test_storage_s3_queue/test.py::test_drop_table [#58293](https://github.com/ClickHouse/ClickHouse/pull/58293) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Handle another case for preprocessing in Keeper [#58308](https://github.com/ClickHouse/ClickHouse/pull/58308) ([Antonio Andelic](https://github.com/antonio2368)).
* Fix test_user_valid_until [#58409](https://github.com/ClickHouse/ClickHouse/pull/58409) ([Nikolay Degterinsky](https://github.com/evillique)).

