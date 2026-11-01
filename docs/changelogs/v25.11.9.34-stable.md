---
sidebar_position: 1
sidebar_label: 2026
---

# 2026 Changelog

### ClickHouse release v25.11.9.34-stable (2e110e54d8c) FIXME as compared to v25.11.8.25-stable (4081364020e)

#### Performance Improvement
* Backported in [#96175](https://github.com/ClickHouse/ClickHouse/issues/96175): Trim size of CachedOnDiskReadBufferFromFile structure 50x. [#96098](https://github.com/ClickHouse/ClickHouse/pull/96098) ([Azat Khuzhin](https://github.com/azat)).

#### Improvement
* Backported in [#95436](https://github.com/ClickHouse/ClickHouse/issues/95436): Now `view_duration_ms` shows the time when group was active, not the sum of the threads duration in it. [#94966](https://github.com/ClickHouse/ClickHouse/pull/94966) ([Sema Checherinda](https://github.com/CheSema)).
* Backported in [#95832](https://github.com/ClickHouse/ClickHouse/issues/95832): Bump internal libxml2 to 2.15.1. [#95574](https://github.com/ClickHouse/ClickHouse/pull/95574) ([Robert Schulze](https://github.com/rschu1ze)).

#### Bug Fix (user-visible misbehavior in an official stable release)
* Backported in [#95286](https://github.com/ClickHouse/ClickHouse/issues/95286): Fix query parameter substitution in CREATE USER authentication methods when using ON CLUSTER. Query parameters in authentication methods (e.g., password) were not being replaced, causing UNKNOWN_QUERY_PARAMETER errors on remote nodes. [#92777](https://github.com/ClickHouse/ClickHouse/pull/92777) ([xiaohuanlin](https://github.com/xiaohuanlin)).
* Backported in [#95529](https://github.com/ClickHouse/ClickHouse/issues/95529): Backups could crash the server if you do many concurrent backups clashing over the same files. [#93659](https://github.com/ClickHouse/ClickHouse/pull/93659) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Backported in [#94973](https://github.com/ClickHouse/ClickHouse/issues/94973): Creating a workload in another workload that is currently in use no longer causes a crash. [#94599](https://github.com/ClickHouse/ClickHouse/pull/94599) ([Sergei Trifonov](https://github.com/serxa)).
* Backported in [#95642](https://github.com/ClickHouse/ClickHouse/issues/95642): Fix incorrect primary key and skip index pruning for predicates involving `toWeek`, `toYearWeek`, `toStartOfWeek`, `toLastDayOfWeek`, and `toDayOfWeek`, and fix exceptions in some of these functions for valid queries with `LowCardinality(String)`. [#94816](https://github.com/ClickHouse/ClickHouse/pull/94816) ([Nihal Z. Miaji](https://github.com/nihalzp)).
* Backported in [#95629](https://github.com/ClickHouse/ClickHouse/issues/95629): Fixes a crash during `ReplicatedMergeTree` startup caused by concurrent removal of `delete_tmp_*` directories. [#94892](https://github.com/ClickHouse/ClickHouse/pull/94892) ([myeongjun](https://github.com/myeongjjun)).
* Backported in [#95815](https://github.com/ClickHouse/ClickHouse/issues/95815): Fixed type inference for qualified columns from source tables when multiple joins are used with `USING` clause. Previously, subsequent joins incorrectly updated types of underlying source columns to a common supertype even when the column was not involved in that join (e.g., in `SELECT t2.a FROM t1 LEFT JOIN t2 USING (a) LEFT JOIN t3 USING (a)`, the `t2.a` column is only used by the first join, so its type should be the supertype of `t1.a` and `t2.a`, excluding `t3.a`). This could lead to logical errors or crashes when functions expected different column types than what actually appeared in the execution plan. [#95157](https://github.com/ClickHouse/ClickHouse/pull/95157) ([Vladimir Cherkasov](https://github.com/vdimir)).
* Backported in [#95425](https://github.com/ClickHouse/ClickHouse/issues/95425): Make column transform only once during getting content of manifesrt .avro list and files. [#95164](https://github.com/ClickHouse/ClickHouse/pull/95164) ([Daniil Ivanik](https://github.com/divanik)).
* Backported in [#95382](https://github.com/ClickHouse/ClickHouse/issues/95382): Fixed inaccurate memory accounting when applying large patch parts after lightweight updates. Previously, applying large patches could cause excessive memory usage and result in the server process being killed by the OOM killer. [#95231](https://github.com/ClickHouse/ClickHouse/pull/95231) ([Anton Popov](https://github.com/CurtizJ)).
* Backported in [#95572](https://github.com/ClickHouse/ClickHouse/issues/95572): The password argument of the `redis` table function now will be masked in the logs and system tables (e.g.: `query_log`). [#95325](https://github.com/ClickHouse/ClickHouse/pull/95325) ([János Benjamin Antal](https://github.com/antaljanosbenjamin)).
* Backported in [#96392](https://github.com/ClickHouse/ClickHouse/issues/96392): Fix a logical error in some cases when negative `LIMIT/OFFSET` is used in distributed queries. [#95357](https://github.com/ClickHouse/ClickHouse/pull/95357) ([Nihal Z. Miaji](https://github.com/nihalzp)).
* Backported in [#95606](https://github.com/ClickHouse/ClickHouse/issues/95606): Avoid possible crash for distributed queries in case of cancellation. [#95466](https://github.com/ClickHouse/ClickHouse/pull/95466) ([Aleksandr Musorin](https://github.com/AVMusorin)).
* Backported in [#95546](https://github.com/ClickHouse/ClickHouse/issues/95546): Fix check for encrypted disks over plain_rewritable (Fixes possible `It is not possible to register multiple plain-rewritable disks with the same object storage prefix`). [#95470](https://github.com/ClickHouse/ClickHouse/pull/95470) ([Azat Khuzhin](https://github.com/azat)).
* Backported in [#95695](https://github.com/ClickHouse/ClickHouse/issues/95695): Fixed `There was an error: Cannot obtain error message` logical error when waiting for a distributed DDL and dropping the Replicated database concurrently. Fixes [#95539](https://github.com/ClickHouse/ClickHouse/issues/95539). [#95664](https://github.com/ClickHouse/ClickHouse/pull/95664) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Backported in [#96053](https://github.com/ClickHouse/ClickHouse/issues/96053): Fix squashing partitioned delta lake data. [#95773](https://github.com/ClickHouse/ClickHouse/pull/95773) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Backported in [#96211](https://github.com/ClickHouse/ClickHouse/issues/96211): Fix skipping paths in JSON data type. Previously with `JSON(SKIP path)` all JSON keys with prefix `path` were skipped, even keys like `"pathpath"`, so it could lead to data loss for these paths during insert. Now it's fixed and only key `"path"` is skipped. [#95948](https://github.com/ClickHouse/ClickHouse/pull/95948) ([Pavel Kruglov](https://github.com/Avogar)).
* Backported in [#96078](https://github.com/ClickHouse/ClickHouse/issues/96078): Fix period for global profilers (controlled by `global_profiler_real_time_period_ns` and `global_profiler_cpu_time_period_ns`). Instead of set value, a truncated value was used, causing profiler to wakeup more than intended. [#96048](https://github.com/ClickHouse/ClickHouse/pull/96048) ([Antonio Andelic](https://github.com/antonio2368)).
* Backported in [#96155](https://github.com/ClickHouse/ClickHouse/issues/96155): Fix use-of-uninitialized-value in `formatDateTime` with non-fixed-width formatters, such as MySQL and JODA-style. [#96133](https://github.com/ClickHouse/ClickHouse/pull/96133) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Backported in [#96278](https://github.com/ClickHouse/ClickHouse/issues/96278): Fix possible deadlock in ProcessList. It can happen because of possible lock inversion if memory overcommit tracker triggers when we are adding task to cancellation checker. [#96182](https://github.com/ClickHouse/ClickHouse/pull/96182) ([Antonio Andelic](https://github.com/antonio2368)).
* Backported in [#96325](https://github.com/ClickHouse/ClickHouse/issues/96325): Fix `stack-use-after-scope` in the vector similarity index. [#96259](https://github.com/ClickHouse/ClickHouse/pull/96259) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Backported in [#96530](https://github.com/ClickHouse/ClickHouse/issues/96530): Fixes livelock in the cancellation checker thread for queries with high `max_execution_time` values. [#96450](https://github.com/ClickHouse/ClickHouse/pull/96450) ([Sergei Trifonov](https://github.com/serxa)).
* Backported in [#96625](https://github.com/ClickHouse/ClickHouse/issues/96625): Fix `LOGICAL_ERROR` exception in Parquet V3 native reader when `PREWHERE` filter column contains non-boolean UInt8 values. [#96594](https://github.com/ClickHouse/ClickHouse/pull/96594) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Backported in [#96691](https://github.com/ClickHouse/ClickHouse/issues/96691): Validate witness version in bech32 to avoid buffer overflow. [#96671](https://github.com/ClickHouse/ClickHouse/pull/96671) ([Raúl Marín](https://github.com/Algunenano)).
* Backported in [#96917](https://github.com/ClickHouse/ClickHouse/issues/96917): Fixed `ALTER TABLE DROP COLUMN` queries failing after a lightweight update was previously performed on the same column. [#96861](https://github.com/ClickHouse/ClickHouse/pull/96861) ([Anton Popov](https://github.com/CurtizJ)).
* Backported in [#96936](https://github.com/ClickHouse/ClickHouse/issues/96936): Fix `std::terminate` exception in `indexOfAssumeSorted` when called with incompatible types (e.g., `IPv4` array with integer search value). [#96877](https://github.com/ClickHouse/ClickHouse/pull/96877) ([Alexey Milovidov](https://github.com/alexey-milovidov)).

#### Build/Testing/Packaging Improvement
* Backported in [#95911](https://github.com/ClickHouse/ClickHouse/issues/95911): Use OpenSSL 3.5.5. [#95345](https://github.com/ClickHouse/ClickHouse/pull/95345) ([Konstantin Bogdanov](https://github.com/thevar1able)).
* Backported in [#96732](https://github.com/ClickHouse/ClickHouse/issues/96732): Pin third-party Docker images to specific versions. [#96500](https://github.com/ClickHouse/ClickHouse/pull/96500) ([Alexey Milovidov](https://github.com/alexey-milovidov)).

#### NO CL ENTRY

* NO CL ENTRY:  'Backport [#93827](https://github.com/ClickHouse/ClickHouse/issues/93827) to 25.11: Fix rebuilding of projections with _part_offset during merge'. [#95871](https://github.com/ClickHouse/ClickHouse/pull/95871) ([Amos Bird](https://github.com/amosbird)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Backported in [#95795](https://github.com/ClickHouse/ClickHouse/issues/95795): Mark task as done without parent ProcessList task. [#95535](https://github.com/ClickHouse/ClickHouse/pull/95535) ([Antonio Andelic](https://github.com/antonio2368)).
* Backported in [#95860](https://github.com/ClickHouse/ClickHouse/issues/95860): Fix contention in CancellationChecker thread. [#95563](https://github.com/ClickHouse/ClickHouse/pull/95563) ([Sergei Trifonov](https://github.com/serxa)).
* Backported in [#95891](https://github.com/ClickHouse/ClickHouse/issues/95891): Fix assertion failure in `applyUncommittedState` for deltas without paths. [#95846](https://github.com/ClickHouse/ClickHouse/pull/95846) ([Alexey Milovidov](https://github.com/alexey-milovidov)).

