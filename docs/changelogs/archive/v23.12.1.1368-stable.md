---
sidebar_position: 1
sidebar_label: 2023
---

# 2023 Changelog

### ClickHouse release v23.12.1.1368-stable (a2faa65b080) FIXME as compared to v23.11.1.2711-stable (05bc8ef1e02)

#### Backward Incompatible Change
* Fix check for non-deterministic functions in TTL expressions. Previously, you could create a TTL expression with non-deterministic functions in some cases, which could lead to undefined behavior later. This fixes [#37250](https://github.com/ClickHouse/ClickHouse/issues/37250). Disallow TTL expressions that don't depend on any columns of a table by default. It can be allowed back by `SET allow_suspicious_ttl_expressions = 1` or `SET compatibility = '23.11'`. Closes [#37286](https://github.com/ClickHouse/ClickHouse/issues/37286). [#51858](https://github.com/ClickHouse/ClickHouse/pull/51858) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Remove function `arrayFold` because it has a bug. This closes [#57816](https://github.com/ClickHouse/ClickHouse/issues/57816). This closes [#57458](https://github.com/ClickHouse/ClickHouse/issues/57458). [#57836](https://github.com/ClickHouse/ClickHouse/pull/57836) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Remove the feature of `is_deleted` row in ReplacingMergeTree and the `CLEANUP` modifier for the OPTIMIZE query. This fixes [#57930](https://github.com/ClickHouse/ClickHouse/issues/57930). This closes [#54988](https://github.com/ClickHouse/ClickHouse/issues/54988). This closes [#54570](https://github.com/ClickHouse/ClickHouse/issues/54570). This closes [#50346](https://github.com/ClickHouse/ClickHouse/issues/50346). This closes [#47579](https://github.com/ClickHouse/ClickHouse/issues/47579). The feature has to be removed because it is not good. We have to remove it as quickly as possible, because there is no other option. [#57932](https://github.com/ClickHouse/ClickHouse/pull/57932) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* The MergeTree setting `clean_deleted_rows` is deprecated, it has no effect anymore. The `CLEANUP` keyword for `OPTIMIZE` is not allowed by default (unless `allow_experimental_replacing_merge_with_cleanup` is enabled). [#58267](https://github.com/ClickHouse/ClickHouse/pull/58267) ([Alexander Tokmakov](https://github.com/tavplubix)).

#### New Feature
* Allow disabling of HEAD request before GET request. [#54602](https://github.com/ClickHouse/ClickHouse/pull/54602) ([Fionera](https://github.com/fionera)).
* Add a HTTP endpoint for checking if Keeper is ready to accept traffic. [#55876](https://github.com/ClickHouse/ClickHouse/pull/55876) ([Konstantin Bogdanov](https://github.com/thevar1able)).
* Add 'union' mode for schema inference. In this mode the resulting table schema is the union of all files schemas (so schema is inferred from each file). The mode of schema inference is controlled by a setting `schema_inference_mode` with 2 possible values - `default` and `union`. Closes [#55428](https://github.com/ClickHouse/ClickHouse/issues/55428). [#55892](https://github.com/ClickHouse/ClickHouse/pull/55892) ([Kruglov Pavel](https://github.com/Avogar)).
* Add new setting `input_format_csv_try_infer_numbers_from_strings` that allows to infer numbers from strings in CSV format. Closes [#56455](https://github.com/ClickHouse/ClickHouse/issues/56455). [#56859](https://github.com/ClickHouse/ClickHouse/pull/56859) ([Kruglov Pavel](https://github.com/Avogar)).
* Refreshable materialized views. [#56946](https://github.com/ClickHouse/ClickHouse/pull/56946) ([Michael Kolupaev](https://github.com/al13n321)).
* Add more warnings on the number of databases, tables. [#57375](https://github.com/ClickHouse/ClickHouse/pull/57375) ([凌涛](https://github.com/lingtaolf)).
* Added a new mutation command `ALTER TABLE <table> APPLY DELETED MASK`, which allows to enforce applying of mask written by lightweight delete and to remove rows marked as deleted from disk. [#57433](https://github.com/ClickHouse/ClickHouse/pull/57433) ([Anton Popov](https://github.com/CurtizJ)).
* Added a new SQL function `sqid` to generate Sqids (https://sqids.org/), example: `SELECT sqid(125, 126)`. [#57512](https://github.com/ClickHouse/ClickHouse/pull/57512) ([Robert Schulze](https://github.com/rschu1ze)).
* Dictionary with `HASHED_ARRAY` (and `COMPLEX_KEY_HASHED_ARRAY`) layout supports `SHARDS` similarly to `HASHED`. [#57544](https://github.com/ClickHouse/ClickHouse/pull/57544) ([vdimir](https://github.com/vdimir)).
* Add asynchronous metrics for total primary key bytes and total allocated primary key bytes in memory. [#57551](https://github.com/ClickHouse/ClickHouse/pull/57551) ([Bharat Nallan](https://github.com/bharatnc)).
* Table system.dropped_tables_parts contains parts of system.dropped_tables tables (dropped but not yet removed tables). [#57555](https://github.com/ClickHouse/ClickHouse/pull/57555) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Add `FORMAT_BYTES` as an alias for `formatReadableSize`. [#57592](https://github.com/ClickHouse/ClickHouse/pull/57592) ([Bharat Nallan](https://github.com/bharatnc)).
* Add SHA512_256 function. [#57645](https://github.com/ClickHouse/ClickHouse/pull/57645) ([Bharat Nallan](https://github.com/bharatnc)).
* Allow passing optional SESSION_TOKEN to `s3` table function. [#57850](https://github.com/ClickHouse/ClickHouse/pull/57850) ([Shani Elharrar](https://github.com/shanielh)).
* Clause `ORDER BY` now supports specifying `ALL`, meaning that ClickHouse sorts by all columns in the `SELECT` clause. Example: `SELECT col1, col2 FROM tab WHERE [...] ORDER BY ALL`. [#57875](https://github.com/ClickHouse/ClickHouse/pull/57875) ([zhongyuankai](https://github.com/zhongyuankai)).
* Added functions for punycode encoding/decoding: `punycodeEncode()` and `punycodeDecode()`. [#57969](https://github.com/ClickHouse/ClickHouse/pull/57969) ([Robert Schulze](https://github.com/rschu1ze)).
* This PR reproduces the implementation of `PASTE JOIN`, which allows users to join tables without `ON` clause. Example: ``` SQL SELECT * FROM ( SELECT number AS a FROM numbers(2) ) AS t1 PASTE JOIN ( SELECT number AS a FROM numbers(2) ORDER BY a DESC ) AS t2. [#57995](https://github.com/ClickHouse/ClickHouse/pull/57995) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).
* A handler `/binary` opens a visual viewer of symbols inside the ClickHouse binary. [#58211](https://github.com/ClickHouse/ClickHouse/pull/58211) ([Alexey Milovidov](https://github.com/alexey-milovidov)).

#### Performance Improvement
* Made copy between s3 disks using a s3-server-side copy instead of copying through the buffer. Improves `BACKUP/RESTORE` operations and `clickhouse-disks copy` command. [#56744](https://github.com/ClickHouse/ClickHouse/pull/56744) ([MikhailBurdukov](https://github.com/MikhailBurdukov)).
* HashJoin respects setting `max_joined_block_size_rows` and do not produce large blocks for `ALL JOIN`. [#56996](https://github.com/ClickHouse/ClickHouse/pull/56996) ([vdimir](https://github.com/vdimir)).
* Release memory for aggregation earlier. This may avoid unnecessary external aggregation. [#57691](https://github.com/ClickHouse/ClickHouse/pull/57691) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Improve performance of string serialization. [#57717](https://github.com/ClickHouse/ClickHouse/pull/57717) ([Maksim Kita](https://github.com/kitaisreal)).
* Support trivial count optimization for `Merge`-engine tables. [#57867](https://github.com/ClickHouse/ClickHouse/pull/57867) ([skyoct](https://github.com/skyoct)).
* Optimized aggregation in some cases. [#57872](https://github.com/ClickHouse/ClickHouse/pull/57872) ([Anton Popov](https://github.com/CurtizJ)).
* The `hasAny()` function can now take advantage of the full-text skipping indices. [#57878](https://github.com/ClickHouse/ClickHouse/pull/57878) ([Jpnock](https://github.com/Jpnock)).
* Function `if(cond, then, else)` (and its alias `cond ? : then : else`) were optimized to use branch-free evaluation. [#57885](https://github.com/ClickHouse/ClickHouse/pull/57885) ([zhanglistar](https://github.com/zhanglistar)).
* Extract non intersecting parts ranges from MergeTree table during FINAL processing. That way we can avoid additional FINAL logic for this non intersecting parts ranges. In case when amount of duplicate values with same primary key is low, performance will be almost the same as without FINAL. Improve reading performance for MergeTree FINAL when `do_not_merge_across_partitions_select_final` setting is set. [#58120](https://github.com/ClickHouse/ClickHouse/pull/58120) ([Maksim Kita](https://github.com/kitaisreal)).
* MergeTree automatically derive `do_not_merge_across_partitions_select_final` setting if partition key expression contains only columns from primary key expression. [#58218](https://github.com/ClickHouse/ClickHouse/pull/58218) ([Maksim Kita](https://github.com/kitaisreal)).
* Speedup MIN and MAX for native types. [#58231](https://github.com/ClickHouse/ClickHouse/pull/58231) ([Raúl Marín](https://github.com/Algunenano)).

#### Improvement
* Make inserts into distributed tables handle updated cluster configuration properly. When the list of cluster nodes is dynamically updated, the Directory Monitor of the distribution table cannot sense the new node, and the Directory Monitor must be re-noded to sense it. [#42826](https://github.com/ClickHouse/ClickHouse/pull/42826) ([zhongyuankai](https://github.com/zhongyuankai)).
* Replace --no-system-tables with loading virtual tables of system database lazily. [#55271](https://github.com/ClickHouse/ClickHouse/pull/55271) ([Azat Khuzhin](https://github.com/azat)).
* Clickhouse-test print case sn, current time and case name in one test case. [#55710](https://github.com/ClickHouse/ClickHouse/pull/55710) ([guoxiaolong](https://github.com/guoxiaolongzte)).
* Do not allow creating replicated table with inconsistent merge params. [#56833](https://github.com/ClickHouse/ClickHouse/pull/56833) ([Duc Canh Le](https://github.com/canhld94)).
* Implement SLRU cache policy for filesystem cache. [#57076](https://github.com/ClickHouse/ClickHouse/pull/57076) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Show uncompressed size in `system.tables`, obtained from data parts' checksums [#56618](https://github.com/ClickHouse/ClickHouse/issues/56618). [#57186](https://github.com/ClickHouse/ClickHouse/pull/57186) ([Chen Lixiang](https://github.com/chenlx0)).
* Add `skip_unavailable_shards` as a setting for `Distributed` tables that is similar to the corresponding query-level setting. Closes [#43666](https://github.com/ClickHouse/ClickHouse/issues/43666). [#57218](https://github.com/ClickHouse/ClickHouse/pull/57218) ([Gagan Goel](https://github.com/tntnatbry)).
* Function `substring()` (aliases: `substr`, `mid`) can now be used with `Enum` types. Previously, the first function argument had to be a value of type `String` or `FixedString`. This improves compatibility with 3rd party tools such as Tableau via MySQL interface. [#57277](https://github.com/ClickHouse/ClickHouse/pull/57277) ([Serge Klochkov](https://github.com/slvrtrn)).
* Better hints when a table doesn't exist. [#57342](https://github.com/ClickHouse/ClickHouse/pull/57342) ([Bharat Nallan](https://github.com/bharatnc)).
* Allow to overwrite `max_partition_size_to_drop` and `max_table_size_to_drop` server settings in query time. [#57452](https://github.com/ClickHouse/ClickHouse/pull/57452) ([Jordi Villar](https://github.com/jrdi)).
* Add support for read-only flag when connecting to the ZooKeeper server (fixes [#53749](https://github.com/ClickHouse/ClickHouse/issues/53749)). [#57479](https://github.com/ClickHouse/ClickHouse/pull/57479) ([Mikhail Koviazin](https://github.com/mkmkme)).
* Fix possible distributed sends stuck due to "No such file or directory" (during recovering batch from disk). Fix possible issues with `error_count` from `system.distribution_queue` (in case of `distributed_directory_monitor_max_sleep_time_ms` >5min). Introduce profile event to track async INSERT failures - `DistributedAsyncInsertionFailures`. [#57480](https://github.com/ClickHouse/ClickHouse/pull/57480) ([Azat Khuzhin](https://github.com/azat)).
* The limit for the number of connections per endpoint for background fetches was raised from `15` to the value of `background_fetches_pool_size` setting. - MergeTree-level setting `replicated_max_parallel_fetches_for_host` became obsolete - MergeTree-level settings `replicated_fetches_http_connection_timeout`, `replicated_fetches_http_send_timeout` and `replicated_fetches_http_receive_timeout` are moved to the Server-level. - Setting `keep_alive_timeout` is added to the list of Server-level settings. [#57523](https://github.com/ClickHouse/ClickHouse/pull/57523) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* It is now possible to refer to ALIAS column in index (non-primary-key) definitions (issue [#55650](https://github.com/ClickHouse/ClickHouse/issues/55650)). Example: `CREATE TABLE tab(col UInt32, col_alias ALIAS col + 1, INDEX idx (col_alias) TYPE minmax) ENGINE = MergeTree ORDER BY col;`. [#57546](https://github.com/ClickHouse/ClickHouse/pull/57546) ([Robert Schulze](https://github.com/rschu1ze)).
* Function `format()` now supports arbitrary argument types (instead of only `String` and `FixedString` arguments). This is important to calculate `SELECT format('The {0} to all questions is {1}', 'answer', 42)`. [#57549](https://github.com/ClickHouse/ClickHouse/pull/57549) ([Robert Schulze](https://github.com/rschu1ze)).
* Support PostgreSQL generated columns and default column values in `MaterializedPostgreSQL`. Closes [#40449](https://github.com/ClickHouse/ClickHouse/issues/40449). [#57568](https://github.com/ClickHouse/ClickHouse/pull/57568) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Allow to apply some filesystem cache config settings changes without server restart. [#57578](https://github.com/ClickHouse/ClickHouse/pull/57578) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Handle sigabrt case when getting PostgreSQl table structure with empty array. [#57618](https://github.com/ClickHouse/ClickHouse/pull/57618) ([Mike Kot (Михаил Кот)](https://github.com/myrrc)).
* Allows to use the `date_trunc()` function with the first argument not depending on the case of it. Both cases are now supported: `SELECT date_trunc('day', now())` and `SELECT date_trunc('DAY', now())`. [#57624](https://github.com/ClickHouse/ClickHouse/pull/57624) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).
* Expose the total number of errors occurred since last server as a `ClickHouseErrorMetric_ALL` metric. [#57627](https://github.com/ClickHouse/ClickHouse/pull/57627) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Allow nodes in config with from_env/from_zk and non empty element with replace=1. [#57628](https://github.com/ClickHouse/ClickHouse/pull/57628) ([Azat Khuzhin](https://github.com/azat)).
* Generate malformed output that cannot be parsed as JSON. [#57646](https://github.com/ClickHouse/ClickHouse/pull/57646) ([Julia Kartseva](https://github.com/jkartseva)).
* Consider lightweight deleted rows when selecting parts to merge if enabled. [#57648](https://github.com/ClickHouse/ClickHouse/pull/57648) ([Zhuo Qiu](https://github.com/jewelzqiu)).
* Make querying system.filesystem_cache not memory intensive. [#57687](https://github.com/ClickHouse/ClickHouse/pull/57687) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Allow IPv6 to UInt128 conversion and binary arithmetic. [#57707](https://github.com/ClickHouse/ClickHouse/pull/57707) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Support negative positional arguments. Closes [#57736](https://github.com/ClickHouse/ClickHouse/issues/57736). [#57741](https://github.com/ClickHouse/ClickHouse/pull/57741) ([flynn](https://github.com/ucasfl)).
* Add a setting for `async inserts deduplication cache` -- how long we wait for cache update. Deprecate setting `async_block_ids_cache_min_update_interval_ms`. Now cache is updated only in case of conflicts. [#57743](https://github.com/ClickHouse/ClickHouse/pull/57743) ([alesapin](https://github.com/alesapin)).
* `sleep()` function now can be cancelled with `KILL QUERY`. [#57746](https://github.com/ClickHouse/ClickHouse/pull/57746) ([Vitaly Baranov](https://github.com/vitlibar)).
* Slightly better inference of unnamed tupes in JSON formats. [#57751](https://github.com/ClickHouse/ClickHouse/pull/57751) ([Kruglov Pavel](https://github.com/Avogar)).
* Refactor UserDefinedSQL* classes to make it possible to add SQL UDF storages which are different from ZooKeeper and Disk. [#57752](https://github.com/ClickHouse/ClickHouse/pull/57752) ([Natasha Chizhonkova](https://github.com/chizhonkova)).
* Forbid `CREATE TABLE ... AS SELECT` queries for Replicated table engines in Replicated database because they are broken. Reference [#35408](https://github.com/ClickHouse/ClickHouse/issues/35408). [#57796](https://github.com/ClickHouse/ClickHouse/pull/57796) ([Nikolay Degterinsky](https://github.com/evillique)).
* Fix and improve transform query for external database, we should recursively obtain all compatible predicates. [#57888](https://github.com/ClickHouse/ClickHouse/pull/57888) ([flynn](https://github.com/ucasfl)).
* Support dynamic reloading of filesystem cache size. Closes [#57866](https://github.com/ClickHouse/ClickHouse/issues/57866). [#57897](https://github.com/ClickHouse/ClickHouse/pull/57897) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fix system.stack_trace for threads with blocked SIGRTMIN. [#57907](https://github.com/ClickHouse/ClickHouse/pull/57907) ([Azat Khuzhin](https://github.com/azat)).
* Added a new setting `readonly` which can be used to specify a s3 disk is read only. It can be useful to create a table with read only `s3_plain` type disk. [#57977](https://github.com/ClickHouse/ClickHouse/pull/57977) ([Pengyuan Bian](https://github.com/bianpengyuan)).
* Support keeper failures in quorum check. [#57986](https://github.com/ClickHouse/ClickHouse/pull/57986) ([Raúl Marín](https://github.com/Algunenano)).
* Add max/peak RSS (`MemoryResidentMax`) into system.asynchronous_metrics. [#58095](https://github.com/ClickHouse/ClickHouse/pull/58095) ([Azat Khuzhin](https://github.com/azat)).
* Fix system.stack_trace for threads with blocked SIGRTMIN (and also send signal to the threads only if it is not blocked to avoid waiting `storage_system_stack_trace_pipe_read_timeout_ms` when it does not make any sense). [#58136](https://github.com/ClickHouse/ClickHouse/pull/58136) ([Azat Khuzhin](https://github.com/azat)).
* This PR allows users to use s3 links (`https://` and `s3://`) without mentioning region if it's not default. Also find the correct region if the user mentioned the wrong one. ### Documentation entry for user-facing changes. [#58148](https://github.com/ClickHouse/ClickHouse/pull/58148) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).
* `clickhouse-format --obfuscate` will know about Settings, MergeTreeSettings, and time zones and keep their names unchanged. [#58179](https://github.com/ClickHouse/ClickHouse/pull/58179) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Added explicit `finalize()` function in `ZipArchiveWriter`. Simplify too complicated code in `ZipArchiveWriter`. This PR fixes [#58074](https://github.com/ClickHouse/ClickHouse/issues/58074). [#58202](https://github.com/ClickHouse/ClickHouse/pull/58202) ([Vitaly Baranov](https://github.com/vitlibar)).
* The primary key analysis in MergeTree tables will now be applied to predicates that include the virtual column `_part_offset` (optionally with `_part`). This feature can serve as a poor man's secondary index. [#58224](https://github.com/ClickHouse/ClickHouse/pull/58224) ([Amos Bird](https://github.com/amosbird)).
* Make caches with the same path use the same cache objects. This behaviour existed before, but was broken in https://github.com/ClickHouse/ClickHouse/pull/48805 (in 23.4). If such caches with the same path have different set of cache settings, an exception will be thrown, that this is not allowed. [#58264](https://github.com/ClickHouse/ClickHouse/pull/58264) ([Kseniia Sumarokova](https://github.com/kssenii)).

#### Build/Testing/Packaging Improvement
* Allow usage of Azure-related table engines/functions on macOS. [#51866](https://github.com/ClickHouse/ClickHouse/pull/51866) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* ClickHouse Fast Test now uses Musl instead of GLibc. [#57711](https://github.com/ClickHouse/ClickHouse/pull/57711) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Run ClickBench for every commit. This closes [#57708](https://github.com/ClickHouse/ClickHouse/issues/57708). [#57712](https://github.com/ClickHouse/ClickHouse/pull/57712) ([Alexey Milovidov](https://github.com/alexey-milovidov)).

#### Bug Fix (user-visible misbehavior in an official stable release)

* Fixed a sorting order breakage in TTL GROUP BY [#49103](https://github.com/ClickHouse/ClickHouse/pull/49103) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* fix: split lttb bucket strategy, first bucket and last bucket should only contain single point [#57003](https://github.com/ClickHouse/ClickHouse/pull/57003) ([FFish](https://github.com/wxybear)).
* Fix possible deadlock in Template format during sync after error [#57004](https://github.com/ClickHouse/ClickHouse/pull/57004) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix early stop while parsing file with skipping lots of errors [#57006](https://github.com/ClickHouse/ClickHouse/pull/57006) ([Kruglov Pavel](https://github.com/Avogar)).
* Prevent dictionary's ACL bypass via dictionary() table function [#57362](https://github.com/ClickHouse/ClickHouse/pull/57362) ([Salvatore Mesoraca](https://github.com/aiven-sal)).
* Fix another case of non-ready set. [#57423](https://github.com/ClickHouse/ClickHouse/pull/57423) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix several issues regarding PostgreSQL `array_ndims` usage. [#57436](https://github.com/ClickHouse/ClickHouse/pull/57436) ([Ryan Jacobs](https://github.com/ryanmjacobs)).
* Fix RWLock inconsistency after write lock timeout [#57454](https://github.com/ClickHouse/ClickHouse/pull/57454) ([Vitaly Baranov](https://github.com/vitlibar)).
* Fix: don't exclude ephemeral column when building pushing to view chain [#57461](https://github.com/ClickHouse/ClickHouse/pull/57461) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* MaterializedPostgreSQL: fix issue [#41922](https://github.com/ClickHouse/ClickHouse/issues/41922), add test for [#41923](https://github.com/ClickHouse/ClickHouse/issues/41923) [#57515](https://github.com/ClickHouse/ClickHouse/pull/57515) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Ignore ON CLUSTER clause in grant/revoke queries for management of replicated access entities.  [#57538](https://github.com/ClickHouse/ClickHouse/pull/57538) ([MikhailBurdukov](https://github.com/MikhailBurdukov)).
* Fix crash in clickhouse-local [#57553](https://github.com/ClickHouse/ClickHouse/pull/57553) ([Nikolay Degterinsky](https://github.com/evillique)).
* Materialize block in HashJoin for Type::EMPTY [#57564](https://github.com/ClickHouse/ClickHouse/pull/57564) ([vdimir](https://github.com/vdimir)).
* Fix possible segfault in PostgreSQLSource [#57567](https://github.com/ClickHouse/ClickHouse/pull/57567) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fix type correction in HashJoin for nested low cardinality [#57614](https://github.com/ClickHouse/ClickHouse/pull/57614) ([vdimir](https://github.com/vdimir)).
* Avoid hangs of system.stack_trace by correctly prohibit parallel read from it [#57641](https://github.com/ClickHouse/ClickHouse/pull/57641) ([Azat Khuzhin](https://github.com/azat)).
* Fix SIGSEGV for aggregation of sparse columns with any() RESPECT NULL [#57710](https://github.com/ClickHouse/ClickHouse/pull/57710) ([Azat Khuzhin](https://github.com/azat)).
* Fix unary operators parsing [#57713](https://github.com/ClickHouse/ClickHouse/pull/57713) ([Nikolay Degterinsky](https://github.com/evillique)).
* Fix RWLock inconsistency after write lock timeout (again) [#57733](https://github.com/ClickHouse/ClickHouse/pull/57733) ([Vitaly Baranov](https://github.com/vitlibar)).
* Table engine MaterializedPostgreSQL fix dependency loading [#57754](https://github.com/ClickHouse/ClickHouse/pull/57754) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fix retries for disconnected nodes for BACKUP/RESTORE ON CLUSTER [#57764](https://github.com/ClickHouse/ClickHouse/pull/57764) ([Vitaly Baranov](https://github.com/vitlibar)).
* Fix bug window functions: revert [#39631](https://github.com/ClickHouse/ClickHouse/issues/39631) [#57766](https://github.com/ClickHouse/ClickHouse/pull/57766) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix result of external aggregation in case of partially materialized projection [#57790](https://github.com/ClickHouse/ClickHouse/pull/57790) ([Anton Popov](https://github.com/CurtizJ)).
* Fix merge in aggregation functions with `*Map` combinator [#57795](https://github.com/ClickHouse/ClickHouse/pull/57795) ([Anton Popov](https://github.com/CurtizJ)).
* Disable system.kafka_consumers by default (due to possible live memory leak) [#57822](https://github.com/ClickHouse/ClickHouse/pull/57822) ([Azat Khuzhin](https://github.com/azat)).
* Fix low-cardinality keys support in MergeJoin [#57827](https://github.com/ClickHouse/ClickHouse/pull/57827) ([vdimir](https://github.com/vdimir)).
* Create consumers for Kafka tables on fly (but keep them for some period since last used) [#57829](https://github.com/ClickHouse/ClickHouse/pull/57829) ([Azat Khuzhin](https://github.com/azat)).
* InterpreterCreateQuery sample block fix [#57855](https://github.com/ClickHouse/ClickHouse/pull/57855) ([Maksim Kita](https://github.com/kitaisreal)).
* bugfix: addresses_expr ignored for psql named collections [#57874](https://github.com/ClickHouse/ClickHouse/pull/57874) ([joelynch](https://github.com/joelynch)).
* Fix invalid memory access in BLAKE3 (Rust) [#57876](https://github.com/ClickHouse/ClickHouse/pull/57876) ([Raúl Marín](https://github.com/Algunenano)).
* Resurrect `arrayFold()` [#57879](https://github.com/ClickHouse/ClickHouse/pull/57879) ([Robert Schulze](https://github.com/rschu1ze)).
* Normalize function names in CREATE INDEX [#57906](https://github.com/ClickHouse/ClickHouse/pull/57906) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Fix handling of unavailable replicas before first request happened [#57933](https://github.com/ClickHouse/ClickHouse/pull/57933) ([Nikita Taranov](https://github.com/nickitat)).
* Fix literal alias misclassification [#57988](https://github.com/ClickHouse/ClickHouse/pull/57988) ([Chen768959](https://github.com/Chen768959)).
* Revert "Fix bug window functions: revert [#39631](https://github.com/ClickHouse/ClickHouse/issues/39631)" [#58031](https://github.com/ClickHouse/ClickHouse/pull/58031) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix invalid preprocessing on Keeper [#58069](https://github.com/ClickHouse/ClickHouse/pull/58069) ([Antonio Andelic](https://github.com/antonio2368)).
* Fix Integer overflow in Poco::UTF32Encoding [#58073](https://github.com/ClickHouse/ClickHouse/pull/58073) ([Andrey Fedotov](https://github.com/anfedotoff)).
* Fix parallel replicas in presence of a scalar subquery with a big integer value [#58118](https://github.com/ClickHouse/ClickHouse/pull/58118) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix `accurateCastOrNull` for out-of-range DateTime [#58139](https://github.com/ClickHouse/ClickHouse/pull/58139) ([Andrey Zvonov](https://github.com/zvonand)).
* Fix possible PARAMETER_OUT_OF_BOUND error during subcolumns reading from wide part in MergeTree [#58175](https://github.com/ClickHouse/ClickHouse/pull/58175) ([Kruglov Pavel](https://github.com/Avogar)).
* Remove parallel parsing for JSONCompactEachRow [#58181](https://github.com/ClickHouse/ClickHouse/pull/58181) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* fix CREATE VIEW hang [#58220](https://github.com/ClickHouse/ClickHouse/pull/58220) ([Tao Wang](https://github.com/wangtZJU)).
* Fix parallel parsing for JSONCompactEachRow [#58250](https://github.com/ClickHouse/ClickHouse/pull/58250) ([Kruglov Pavel](https://github.com/Avogar)).

#### NO CL ENTRY

* NO CL ENTRY:  'Revert "Revert "Update Sentry""'. [#57694](https://github.com/ClickHouse/ClickHouse/pull/57694) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* NO CL ENTRY:  'Revert "Fix RWLock inconsistency after write lock timeout"'. [#57730](https://github.com/ClickHouse/ClickHouse/pull/57730) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* NO CL ENTRY:  'Revert "improve CI with digest for docker, build and test jobs"'. [#57903](https://github.com/ClickHouse/ClickHouse/pull/57903) ([Max K.](https://github.com/mkaynov)).
* NO CL ENTRY:  'Reapply "improve CI with digest for docker, build and test jobs"'. [#57904](https://github.com/ClickHouse/ClickHouse/pull/57904) ([Max K.](https://github.com/mkaynov)).
* NO CL ENTRY:  'Revert "Merge pull request [#56573](https://github.com/ClickHouse/ClickHouse/issues/56573) from mkmkme/mkmkme/reload-config"'. [#57909](https://github.com/ClickHouse/ClickHouse/pull/57909) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* NO CL ENTRY:  'Revert "Add system.dropped_tables_parts table"'. [#58022](https://github.com/ClickHouse/ClickHouse/pull/58022) ([Antonio Andelic](https://github.com/antonio2368)).
* NO CL ENTRY:  'Revert "Consider lightweight deleted rows when selecting parts to merge"'. [#58097](https://github.com/ClickHouse/ClickHouse/pull/58097) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* NO CL ENTRY:  'Revert "Fix leftover processes/hangs in tests"'. [#58207](https://github.com/ClickHouse/ClickHouse/pull/58207) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* NO CL ENTRY:  'Revert "Create consumers for Kafka tables on fly (but keep them for some period since last used)"'. [#58272](https://github.com/ClickHouse/ClickHouse/pull/58272) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* NO CL ENTRY:  'Revert "Implement punycode encoding/decoding"'. [#58277](https://github.com/ClickHouse/ClickHouse/pull/58277) ([Alexey Milovidov](https://github.com/alexey-milovidov)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Randomize more settings [#39663](https://github.com/ClickHouse/ClickHouse/pull/39663) ([Anton Popov](https://github.com/CurtizJ)).
* Add more tests for `compile_expressions` [#51113](https://github.com/ClickHouse/ClickHouse/pull/51113) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* [RFC] Correctly wait background threads [#52717](https://github.com/ClickHouse/ClickHouse/pull/52717) ([Azat Khuzhin](https://github.com/azat)).
* improve CI with digest for docker, build and test jobs [#56317](https://github.com/ClickHouse/ClickHouse/pull/56317) ([Max K.](https://github.com/mkaynov)).
* Prepare the introduction of more keeper faults [#56917](https://github.com/ClickHouse/ClickHouse/pull/56917) ([Raúl Marín](https://github.com/Algunenano)).
* Analyzer: Fix assert in tryReplaceAndEqualsChainsWithConstant [#57139](https://github.com/ClickHouse/ClickHouse/pull/57139) ([vdimir](https://github.com/vdimir)).
* Check what will happen if we build ClickHouse with Musl [#57180](https://github.com/ClickHouse/ClickHouse/pull/57180) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* support memory soft limit for keeper [#57271](https://github.com/ClickHouse/ClickHouse/pull/57271) ([Han Fei](https://github.com/hanfei1991)).
* Randomize disabled optimizations in CI [#57315](https://github.com/ClickHouse/ClickHouse/pull/57315) ([Raúl Marín](https://github.com/Algunenano)).
* Don't throw if noop when dropping database replica in batch [#57337](https://github.com/ClickHouse/ClickHouse/pull/57337) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Better JSON -> JSONEachRow fallback without catching exceptions [#57364](https://github.com/ClickHouse/ClickHouse/pull/57364) ([Kruglov Pavel](https://github.com/Avogar)).
* Add tests for [#48496](https://github.com/ClickHouse/ClickHouse/issues/48496) [#57414](https://github.com/ClickHouse/ClickHouse/pull/57414) ([Raúl Marín](https://github.com/Algunenano)).
* Add profile event for cache lookup in `ThreadPoolRemoteFSReader` [#57437](https://github.com/ClickHouse/ClickHouse/pull/57437) ([Nikita Taranov](https://github.com/nickitat)).
* Remove select() usage [#57467](https://github.com/ClickHouse/ClickHouse/pull/57467) ([Igor Nikonov](https://github.com/devcrafter)).
* Parallel replicas: friendly settings [#57542](https://github.com/ClickHouse/ClickHouse/pull/57542) ([Igor Nikonov](https://github.com/devcrafter)).
* Fix formatting string prompt error [#57569](https://github.com/ClickHouse/ClickHouse/pull/57569) ([skyoct](https://github.com/skyoct)).
* Tune CI scale up/down multipliers [#57572](https://github.com/ClickHouse/ClickHouse/pull/57572) ([Max K.](https://github.com/mkaynov)).
* Revert "Revert "Implemented series period detect method using pocketfft lib"" [#57574](https://github.com/ClickHouse/ClickHouse/pull/57574) ([Bhavna Jindal](https://github.com/bhavnajindal)).
* Correctly handle errors during opening query in editor in client [#57587](https://github.com/ClickHouse/ClickHouse/pull/57587) ([Azat Khuzhin](https://github.com/azat)).
* Add a test for [#55251](https://github.com/ClickHouse/ClickHouse/issues/55251) [#57588](https://github.com/ClickHouse/ClickHouse/pull/57588) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Add a test for [#48039](https://github.com/ClickHouse/ClickHouse/issues/48039) [#57593](https://github.com/ClickHouse/ClickHouse/pull/57593) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Update CHANGELOG.md [#57594](https://github.com/ClickHouse/ClickHouse/pull/57594) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Update version after release [#57595](https://github.com/ClickHouse/ClickHouse/pull/57595) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Update version_date.tsv and changelogs after v23.11.1.2711-stable [#57597](https://github.com/ClickHouse/ClickHouse/pull/57597) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Identify failed jobs in lambda and mark as steps=0 [#57600](https://github.com/ClickHouse/ClickHouse/pull/57600) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Fix flaky test: distinct in order with analyzer [#57606](https://github.com/ClickHouse/ClickHouse/pull/57606) ([Igor Nikonov](https://github.com/devcrafter)).
* CHJIT add assembly printer [#57610](https://github.com/ClickHouse/ClickHouse/pull/57610) ([Maksim Kita](https://github.com/kitaisreal)).
* Fix parsing virtual hosted S3 URI in clickhouse_backupview script [#57612](https://github.com/ClickHouse/ClickHouse/pull/57612) ([Daniel Pozo Escalona](https://github.com/danipozo)).
* Fix docs for `fileCluster` [#57613](https://github.com/ClickHouse/ClickHouse/pull/57613) ([Andrey Zvonov](https://github.com/zvonand)).
* Analyzer: Fix logical error in MultiIfToIfPass [#57622](https://github.com/ClickHouse/ClickHouse/pull/57622) ([vdimir](https://github.com/vdimir)).
* Throw more clear exception [#57626](https://github.com/ClickHouse/ClickHouse/pull/57626) ([alesapin](https://github.com/alesapin)).
* Fix "logs and exception messages formatting", part 1 [#57630](https://github.com/ClickHouse/ClickHouse/pull/57630) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix "logs and exception messages formatting", part 2 [#57632](https://github.com/ClickHouse/ClickHouse/pull/57632) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix "logs and exception messages formatting", part 3 [#57633](https://github.com/ClickHouse/ClickHouse/pull/57633) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix "logs and exception messages formatting", part 4 [#57634](https://github.com/ClickHouse/ClickHouse/pull/57634) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Remove bad test (1) [#57636](https://github.com/ClickHouse/ClickHouse/pull/57636) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Remove bad test (2) [#57637](https://github.com/ClickHouse/ClickHouse/pull/57637) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* ClickHouse Cloud promotion [#57638](https://github.com/ClickHouse/ClickHouse/pull/57638) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Remove bad test (3) [#57639](https://github.com/ClickHouse/ClickHouse/pull/57639) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Remove bad test (4) [#57640](https://github.com/ClickHouse/ClickHouse/pull/57640) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Random changes in random files [#57642](https://github.com/ClickHouse/ClickHouse/pull/57642) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Merge half of [#51113](https://github.com/ClickHouse/ClickHouse/issues/51113) [#57643](https://github.com/ClickHouse/ClickHouse/pull/57643) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Analyzer: Fix JOIN ON true with join_use_nulls [#57662](https://github.com/ClickHouse/ClickHouse/pull/57662) ([vdimir](https://github.com/vdimir)).
* Pin alpine version of integration tests helper container [#57669](https://github.com/ClickHouse/ClickHouse/pull/57669) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Add support for system.stack_trace filtering optimizations for analyzer [#57682](https://github.com/ClickHouse/ClickHouse/pull/57682) ([Azat Khuzhin](https://github.com/azat)).
* test for [#33308](https://github.com/ClickHouse/ClickHouse/issues/33308) [#57693](https://github.com/ClickHouse/ClickHouse/pull/57693) ([Denny Crane](https://github.com/den-crane)).
* support keeper memory soft limit ratio [#57699](https://github.com/ClickHouse/ClickHouse/pull/57699) ([Han Fei](https://github.com/hanfei1991)).
* Fix test_dictionaries_update_and_reload/test.py::test_reload_while_loading flakiness [#57714](https://github.com/ClickHouse/ClickHouse/pull/57714) ([Azat Khuzhin](https://github.com/azat)).
* Tune autoscale to scale for single job in the queue [#57742](https://github.com/ClickHouse/ClickHouse/pull/57742) ([Max K.](https://github.com/mkaynov)).
* Tune network memory for dockerhub proxy hosts [#57744](https://github.com/ClickHouse/ClickHouse/pull/57744) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Parallel replicas: announcement response handling improvement [#57749](https://github.com/ClickHouse/ClickHouse/pull/57749) ([Igor Nikonov](https://github.com/devcrafter)).
* Fix building Rust with Musl [#57756](https://github.com/ClickHouse/ClickHouse/pull/57756) ([Azat Khuzhin](https://github.com/azat)).
* Fix flaky test_parallel_replicas_distributed_read_from_all [#57757](https://github.com/ClickHouse/ClickHouse/pull/57757) ([Igor Nikonov](https://github.com/devcrafter)).
* Minor refactoring of toStartOfInterval() [#57761](https://github.com/ClickHouse/ClickHouse/pull/57761) ([Robert Schulze](https://github.com/rschu1ze)).
* Don't run test 02919_skip_lots_of_parsing_errors on aarch64 [#57762](https://github.com/ClickHouse/ClickHouse/pull/57762) ([Kruglov Pavel](https://github.com/Avogar)).
* More respect to `min_number_of_marks` in `ParallelReplicasReadingCoordinator` [#57763](https://github.com/ClickHouse/ClickHouse/pull/57763) ([Nikita Taranov](https://github.com/nickitat)).
* SerializationString reduce memory usage [#57787](https://github.com/ClickHouse/ClickHouse/pull/57787) ([Maksim Kita](https://github.com/kitaisreal)).
* Fix ThreadSanitizer data race in librdkafka  [#57791](https://github.com/ClickHouse/ClickHouse/pull/57791) ([Ilya Golshtein](https://github.com/ilejn)).
* Rename `system.async_loader` into `system.asynchronous_loader` [#57793](https://github.com/ClickHouse/ClickHouse/pull/57793) ([Sergei Trifonov](https://github.com/serxa)).
* Set replica number to its position in cluster definition [#57800](https://github.com/ClickHouse/ClickHouse/pull/57800) ([Nikita Taranov](https://github.com/nickitat)).
* fix clickhouse-client invocation in 02327_capnproto_protobuf_empty_messages [#57804](https://github.com/ClickHouse/ClickHouse/pull/57804) ([Mikhail Koviazin](https://github.com/mkmkme)).
* Fix flaky test_parallel_replicas_over_distributed [#57809](https://github.com/ClickHouse/ClickHouse/pull/57809) ([Igor Nikonov](https://github.com/devcrafter)).
* Revert [#57741](https://github.com/ClickHouse/ClickHouse/issues/57741) [#57811](https://github.com/ClickHouse/ClickHouse/pull/57811) ([Raúl Marín](https://github.com/Algunenano)).
* Dumb down `substring()` tests [#57821](https://github.com/ClickHouse/ClickHouse/pull/57821) ([Robert Schulze](https://github.com/rschu1ze)).
* Update version_date.tsv and changelogs after v23.11.2.11-stable [#57824](https://github.com/ClickHouse/ClickHouse/pull/57824) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Fix 02906_force_optimize_projection_name [#57826](https://github.com/ClickHouse/ClickHouse/pull/57826) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).
* ClickBench: slightly better [#57831](https://github.com/ClickHouse/ClickHouse/pull/57831) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix 02932_kill_query_sleep flakiness [#57849](https://github.com/ClickHouse/ClickHouse/pull/57849) ([Azat Khuzhin](https://github.com/azat)).
* Revert "Replace --no-system-tables with loading virtual tables of system database lazily" [#57851](https://github.com/ClickHouse/ClickHouse/pull/57851) ([Azat Khuzhin](https://github.com/azat)).
* Fix memory leak in StorageHDFS [#57860](https://github.com/ClickHouse/ClickHouse/pull/57860) ([Andrey Zvonov](https://github.com/zvonand)).
* Remove hardcoded clickhouse-client invocations from tests [#57861](https://github.com/ClickHouse/ClickHouse/pull/57861) ([Mikhail Koviazin](https://github.com/mkmkme)).
* Follow up to [#57568](https://github.com/ClickHouse/ClickHouse/issues/57568) [#57863](https://github.com/ClickHouse/ClickHouse/pull/57863) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fix assertion in HashJoin [#57873](https://github.com/ClickHouse/ClickHouse/pull/57873) ([vdimir](https://github.com/vdimir)).
* More efficient constructor for SerializationEnum [#57887](https://github.com/ClickHouse/ClickHouse/pull/57887) ([Duc Canh Le](https://github.com/canhld94)).
* Fix test_unset_skip_unavailable_shards [#57895](https://github.com/ClickHouse/ClickHouse/pull/57895) ([Raúl Marín](https://github.com/Algunenano)).
* Add argument to fill the gap in cherry-pick [#57896](https://github.com/ClickHouse/ClickHouse/pull/57896) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Delete debug logging in OutputFormatWithUTF8ValidationAdaptor [#57899](https://github.com/ClickHouse/ClickHouse/pull/57899) ([Kruglov Pavel](https://github.com/Avogar)).
* Remove heavy rust stable toolchain [#57905](https://github.com/ClickHouse/ClickHouse/pull/57905) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Improvements for 00002_log_and_exception_messages_formatting [#57910](https://github.com/ClickHouse/ClickHouse/pull/57910) ([Raúl Marín](https://github.com/Algunenano)).
* Update CHANGELOG.md [#57911](https://github.com/ClickHouse/ClickHouse/pull/57911) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* remove cruft from TablesLoader [#57938](https://github.com/ClickHouse/ClickHouse/pull/57938) ([Bharat Nallan](https://github.com/bharatnc)).
* Fix `/dashboard` work with passwords [#57948](https://github.com/ClickHouse/ClickHouse/pull/57948) ([Sergei Trifonov](https://github.com/serxa)).
* Remove wrong test [#57950](https://github.com/ClickHouse/ClickHouse/pull/57950) ([Sergei Trifonov](https://github.com/serxa)).
* Fix docker image for integration tests (fixes CI) [#57952](https://github.com/ClickHouse/ClickHouse/pull/57952) ([Azat Khuzhin](https://github.com/azat)).
* Remove C++ templates (normalizeQuery) [#57963](https://github.com/ClickHouse/ClickHouse/pull/57963) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* A small fix for dashboard [#57964](https://github.com/ClickHouse/ClickHouse/pull/57964) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Always use `pread` for reading cache segments [#57970](https://github.com/ClickHouse/ClickHouse/pull/57970) ([Nikita Taranov](https://github.com/nickitat)).
* Improve some tests [#57973](https://github.com/ClickHouse/ClickHouse/pull/57973) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Revert "Merge pull request [#57907](https://github.com/ClickHouse/ClickHouse/issues/57907) from azat/system.stack_trace-rt_tgsigqueueinfo" [#57974](https://github.com/ClickHouse/ClickHouse/pull/57974) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Add a test for [#49708](https://github.com/ClickHouse/ClickHouse/issues/49708) [#57979](https://github.com/ClickHouse/ClickHouse/pull/57979) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix style-check checkout head-ref [#57989](https://github.com/ClickHouse/ClickHouse/pull/57989) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* refine error message [#57991](https://github.com/ClickHouse/ClickHouse/pull/57991) ([Han Fei](https://github.com/hanfei1991)).
* CI for docs only fix [#57992](https://github.com/ClickHouse/ClickHouse/pull/57992) ([Max K.](https://github.com/mkaynov)).
* Replace rust's BLAKE3 with llvm's implementation [#57994](https://github.com/ClickHouse/ClickHouse/pull/57994) ([Raúl Marín](https://github.com/Algunenano)).
* Better trivial count optimization for storage `Merge` [#57996](https://github.com/ClickHouse/ClickHouse/pull/57996) ([Anton Popov](https://github.com/CurtizJ)).
* enhanced docs for `date_trunc()` [#58000](https://github.com/ClickHouse/ClickHouse/pull/58000) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).
* CI: add needs_changed_files flag for pr_info [#58003](https://github.com/ClickHouse/ClickHouse/pull/58003) ([Max K.](https://github.com/mkaynov)).
* more messages in ci [#58007](https://github.com/ClickHouse/ClickHouse/pull/58007) ([Sema Checherinda](https://github.com/CheSema)).
* Test parallel replicas with force_primary_key setting [#58010](https://github.com/ClickHouse/ClickHouse/pull/58010) ([Igor Nikonov](https://github.com/devcrafter)).
* Update 00002_log_and_exception_messages_formatting.sql [#58012](https://github.com/ClickHouse/ClickHouse/pull/58012) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Fix rare race in external sort/aggregation with temporary data in cache [#58013](https://github.com/ClickHouse/ClickHouse/pull/58013) ([Anton Popov](https://github.com/CurtizJ)).
* Fix segfault in FuzzJSON engine [#58015](https://github.com/ClickHouse/ClickHouse/pull/58015) ([Julia Kartseva](https://github.com/jkartseva)).
* fix freebsd build [#58019](https://github.com/ClickHouse/ClickHouse/pull/58019) ([Julia Kartseva](https://github.com/jkartseva)).
* Rename canUseParallelReplicas to canUseTaskBasedParallelReplicas [#58025](https://github.com/ClickHouse/ClickHouse/pull/58025) ([Raúl Marín](https://github.com/Algunenano)).
* Remove fixed tests from analyzer_tech_debt.txt [#58028](https://github.com/ClickHouse/ClickHouse/pull/58028) ([Raúl Marín](https://github.com/Algunenano)).
* More verbose errors on 00002_log_and_exception_messages_formatting [#58037](https://github.com/ClickHouse/ClickHouse/pull/58037) ([Raúl Marín](https://github.com/Algunenano)).
* Make window insert result into constant [#58045](https://github.com/ClickHouse/ClickHouse/pull/58045) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* CI: Happy new year [#58046](https://github.com/ClickHouse/ClickHouse/pull/58046) ([Raúl Marín](https://github.com/Algunenano)).
* Follow up for [#57691](https://github.com/ClickHouse/ClickHouse/issues/57691) [#58048](https://github.com/ClickHouse/ClickHouse/pull/58048) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* always run ast_fuzz and sqllancer [#58049](https://github.com/ClickHouse/ClickHouse/pull/58049) ([Max K.](https://github.com/mkaynov)).
* Add GH status for PR formating [#58050](https://github.com/ClickHouse/ClickHouse/pull/58050) ([Max K.](https://github.com/mkaynov)).
* Small improvement for SystemLogBase [#58051](https://github.com/ClickHouse/ClickHouse/pull/58051) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Bump Azure to v1.6.0 [#58052](https://github.com/ClickHouse/ClickHouse/pull/58052) ([Robert Schulze](https://github.com/rschu1ze)).
* Correct values for randomization [#58058](https://github.com/ClickHouse/ClickHouse/pull/58058) ([Anton Popov](https://github.com/CurtizJ)).
* Non post request should be readonly [#58060](https://github.com/ClickHouse/ClickHouse/pull/58060) ([San](https://github.com/santrancisco)).
* Revert "Merge pull request [#55710](https://github.com/ClickHouse/ClickHouse/issues/55710) from guoxiaolongzte/clickhouse-test... [#58066](https://github.com/ClickHouse/ClickHouse/pull/58066) ([Raúl Marín](https://github.com/Algunenano)).
* fix typo in the test 02479 [#58072](https://github.com/ClickHouse/ClickHouse/pull/58072) ([Sema Checherinda](https://github.com/CheSema)).
* Bump Azure to 1.7.2 [#58075](https://github.com/ClickHouse/ClickHouse/pull/58075) ([Robert Schulze](https://github.com/rschu1ze)).
* Fix flaky test `02567_and_consistency` [#58076](https://github.com/ClickHouse/ClickHouse/pull/58076) ([Anton Popov](https://github.com/CurtizJ)).
* Fix Tests Bugfix Validate Check [#58078](https://github.com/ClickHouse/ClickHouse/pull/58078) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Fix for nightly job for digest-ci [#58079](https://github.com/ClickHouse/ClickHouse/pull/58079) ([Max K.](https://github.com/mkaynov)).
* Test for parallel replicas with remote() [#58081](https://github.com/ClickHouse/ClickHouse/pull/58081) ([Igor Nikonov](https://github.com/devcrafter)).
* Minor cosmetic changes [#58092](https://github.com/ClickHouse/ClickHouse/pull/58092) ([Raúl Marín](https://github.com/Algunenano)).
* Reintroduce OPTIMIZE CLEANUP as no-op [#58100](https://github.com/ClickHouse/ClickHouse/pull/58100) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Add compatibility in the replication protocol for a removed feature [#58104](https://github.com/ClickHouse/ClickHouse/pull/58104) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Flaky 02922_analyzer_aggregate_nothing_type [#58105](https://github.com/ClickHouse/ClickHouse/pull/58105) ([Raúl Marín](https://github.com/Algunenano)).
* Update version_date.tsv and changelogs after v23.11.3.23-stable [#58106](https://github.com/ClickHouse/ClickHouse/pull/58106) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Limited CI on the master for docs only change [#58121](https://github.com/ClickHouse/ClickHouse/pull/58121) ([Max K.](https://github.com/mkaynov)).
* style fix [#58125](https://github.com/ClickHouse/ClickHouse/pull/58125) ([Max K.](https://github.com/mkaynov)).
* Support "do not test" label with ci.py [#58128](https://github.com/ClickHouse/ClickHouse/pull/58128) ([Max K.](https://github.com/mkaynov)).
* Use the single images list for integration tests everywhere [#58130](https://github.com/ClickHouse/ClickHouse/pull/58130) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Disable parallel replicas with IN (subquery) [#58133](https://github.com/ClickHouse/ClickHouse/pull/58133) ([Igor Nikonov](https://github.com/devcrafter)).
* Fix clang-tidy [#58134](https://github.com/ClickHouse/ClickHouse/pull/58134) ([Raúl Marín](https://github.com/Algunenano)).
* Run build report check job on build failures, fix [#58135](https://github.com/ClickHouse/ClickHouse/pull/58135) ([Max K.](https://github.com/mkaynov)).
* Fix dashboard legend sorting and rows number [#58151](https://github.com/ClickHouse/ClickHouse/pull/58151) ([Sergei Trifonov](https://github.com/serxa)).
* Remove retryStrategy assignments overwritten in ClientFactory::create() [#58163](https://github.com/ClickHouse/ClickHouse/pull/58163) ([Daniel Pozo Escalona](https://github.com/danipozo)).
* Helper improvements [#58164](https://github.com/ClickHouse/ClickHouse/pull/58164) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Pass through exceptions for reading from S3 [#58165](https://github.com/ClickHouse/ClickHouse/pull/58165) ([Azat Khuzhin](https://github.com/azat)).
* [RFC] Adjust all std::ios implementations in poco to set failbit/badbit by default [#58166](https://github.com/ClickHouse/ClickHouse/pull/58166) ([Azat Khuzhin](https://github.com/azat)).
* Add bytes_uncompressed to system.part_log [#58167](https://github.com/ClickHouse/ClickHouse/pull/58167) ([Jordi Villar](https://github.com/jrdi)).
* Update docker/test/stateful/run.sh [#58168](https://github.com/ClickHouse/ClickHouse/pull/58168) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Update 00165_jit_aggregate_functions.sql [#58169](https://github.com/ClickHouse/ClickHouse/pull/58169) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Update clickhouse-test [#58170](https://github.com/ClickHouse/ClickHouse/pull/58170) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Profile event 'ParallelReplicasUsedCount' [#58173](https://github.com/ClickHouse/ClickHouse/pull/58173) ([Igor Nikonov](https://github.com/devcrafter)).
* Fix flaky test `02719_aggregate_with_empty_string_key` [#58176](https://github.com/ClickHouse/ClickHouse/pull/58176) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix [#58171](https://github.com/ClickHouse/ClickHouse/issues/58171) [#58177](https://github.com/ClickHouse/ClickHouse/pull/58177) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Add base backup name to system.backups and system.backup_log tables [#58178](https://github.com/ClickHouse/ClickHouse/pull/58178) ([Pradeep Chhetri](https://github.com/chhetripradeep)).
* Fix use-after-move [#58182](https://github.com/ClickHouse/ClickHouse/pull/58182) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Looking at strange code [#58196](https://github.com/ClickHouse/ClickHouse/pull/58196) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix all Exception with missing arguments [#58198](https://github.com/ClickHouse/ClickHouse/pull/58198) ([Azat Khuzhin](https://github.com/azat)).
* Fix leftover processes/hangs in tests [#58200](https://github.com/ClickHouse/ClickHouse/pull/58200) ([Azat Khuzhin](https://github.com/azat)).
* Fix DWARFBlockInputFormat failing on DWARF 5 unit address ranges [#58204](https://github.com/ClickHouse/ClickHouse/pull/58204) ([Michael Kolupaev](https://github.com/al13n321)).
* Fix error in archive reader [#58206](https://github.com/ClickHouse/ClickHouse/pull/58206) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix DWARFBlockInputFormat using wrong base address sometimes [#58208](https://github.com/ClickHouse/ClickHouse/pull/58208) ([Michael Kolupaev](https://github.com/al13n321)).
* Add support for specifying query parameters in the command line in clickhouse-local [#58210](https://github.com/ClickHouse/ClickHouse/pull/58210) ([Pradeep Chhetri](https://github.com/chhetripradeep)).
* Fix leftover processes/hangs in tests (resubmit) [#58213](https://github.com/ClickHouse/ClickHouse/pull/58213) ([Azat Khuzhin](https://github.com/azat)).
* Add optimization for AND notEquals chain in logical expression optimizer [#58214](https://github.com/ClickHouse/ClickHouse/pull/58214) ([Kevin Mingtarja](https://github.com/kevinmingtarja)).
* Fix syntax and doc [#58221](https://github.com/ClickHouse/ClickHouse/pull/58221) ([San](https://github.com/santrancisco)).
* Cleanup some known short messages [#58226](https://github.com/ClickHouse/ClickHouse/pull/58226) ([Raúl Marín](https://github.com/Algunenano)).
* Some code refactoring (was an attempt to improve build time, but failed) [#58237](https://github.com/ClickHouse/ClickHouse/pull/58237) ([Azat Khuzhin](https://github.com/azat)).
* Fix perf test README [#58245](https://github.com/ClickHouse/ClickHouse/pull/58245) ([Raúl Marín](https://github.com/Algunenano)).
* [Analyzer] Add test for [#57086](https://github.com/ClickHouse/ClickHouse/issues/57086) [#58249](https://github.com/ClickHouse/ClickHouse/pull/58249) ([Raúl Marín](https://github.com/Algunenano)).
* Reintroduce compatibility with `is_deleted` on a syntax level [#58251](https://github.com/ClickHouse/ClickHouse/pull/58251) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Avoid throwing ABORTED on normal situations [#58252](https://github.com/ClickHouse/ClickHouse/pull/58252) ([Raúl Marín](https://github.com/Algunenano)).
* Remove mayBenefitFromIndexForIn [#58265](https://github.com/ClickHouse/ClickHouse/pull/58265) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Allow a few retries when committing a part during shutdown [#58269](https://github.com/ClickHouse/ClickHouse/pull/58269) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Revert [#58267](https://github.com/ClickHouse/ClickHouse/issues/58267) [#58274](https://github.com/ClickHouse/ClickHouse/pull/58274) ([Alexey Milovidov](https://github.com/alexey-milovidov)).

