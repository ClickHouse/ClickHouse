## ClickHouse release v20.4

### ClickHouse release v20.4.3.16-stable 2020-05-23

#### Bug Fix

* Removed logging from mutation finalization task if nothing was finalized. [#11109](https://github.com/ClickHouse/ClickHouse/pull/11109) ([alesapin](https://github.com/alesapin)).
* Fixed memory leak in registerDiskS3. [#11074](https://github.com/ClickHouse/ClickHouse/pull/11074) ([Pavel Kovalenko](https://github.com/Jokser)).
* Fixed the potential missed data during termination of Kafka engine table. [#11048](https://github.com/ClickHouse/ClickHouse/pull/11048) ([filimonov](https://github.com/filimonov)).
* Fixed `parseDateTime64BestEffort` argument resolution bugs. [#11038](https://github.com/ClickHouse/ClickHouse/pull/11038) ([Vasily Nemkov](https://github.com/Enmk)).
* Fixed very rare potential use-after-free error in `MergeTree` if table was not created successfully. [#10986](https://github.com/ClickHouse/ClickHouse/pull/10986), [#10970](https://github.com/ClickHouse/ClickHouse/pull/10970) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fixed metadata (relative path for rename) and data (relative path for symlink) handling for Atomic database. [#10980](https://github.com/ClickHouse/ClickHouse/pull/10980) ([Azat Khuzhin](https://github.com/azat)).
* Fixed server crash on concurrent `ALTER` and `DROP DATABASE` queries with `Atomic` database engine. [#10968](https://github.com/ClickHouse/ClickHouse/pull/10968) ([tavplubix](https://github.com/tavplubix)).
* Fixed incorrect raw data size in `getRawData()` method. [#10964](https://github.com/ClickHouse/ClickHouse/pull/10964) ([Igr](https://github.com/ObjatieGroba)).
* Fixed incompatibility of two-level aggregation between versions 20.1 and earlier. This incompatibility happens when different versions of ClickHouse are used on initiator node and remote nodes and the size of GROUP BY result is large and aggregation is performed by a single String field. It leads to several unmerged rows for a single key in result. [#10952](https://github.com/ClickHouse/ClickHouse/pull/10952) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fixed sending partially written files by the `DistributedBlockOutputStream`. [#10940](https://github.com/ClickHouse/ClickHouse/pull/10940) ([Azat Khuzhin](https://github.com/azat)).
* Fixed crash in `SELECT count(notNullIn(NULL, []))`. [#10920](https://github.com/ClickHouse/ClickHouse/pull/10920) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed the hang which was happening sometimes during `DROP` of `Kafka` table engine. (or during server restarts). [#10910](https://github.com/ClickHouse/ClickHouse/pull/10910) ([filimonov](https://github.com/filimonov)).
* Fixed the impossibility of executing multiple `ALTER RENAME` like `a TO b, c TO a`. [#10895](https://github.com/ClickHouse/ClickHouse/pull/10895) ([alesapin](https://github.com/alesapin)).
* Fixed possible race which could happen when you get result from aggregate function state from multiple thread for the same column. The only way it can happen is when you use `finalizeAggregation` function while reading from table with `Memory` engine which stores `AggregateFunction` state for `quantile*` function. [#10890](https://github.com/ClickHouse/ClickHouse/pull/10890) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed backward compatibility with tuples in Distributed tables. [#10889](https://github.com/ClickHouse/ClickHouse/pull/10889) ([Anton Popov](https://github.com/CurtizJ)).
* Fixed `SIGSEGV` in `StringHashTable` if such a key does not exist. [#10870](https://github.com/ClickHouse/ClickHouse/pull/10870) ([Azat Khuzhin](https://github.com/azat)).
* Fixed `WATCH` hangs after `LiveView` table was dropped from database with `Atomic` engine. [#10859](https://github.com/ClickHouse/ClickHouse/pull/10859) ([tavplubix](https://github.com/tavplubix)).
* Fixed bug in `ReplicatedMergeTree` which might cause some `ALTER` on `OPTIMIZE` query to hang waiting for some replica after it become inactive. [#10849](https://github.com/ClickHouse/ClickHouse/pull/10849) ([tavplubix](https://github.com/tavplubix)).
* Now constraints are updated if the column participating in `CONSTRAINT` expression was renamed. Fixes [#10844](https://github.com/ClickHouse/ClickHouse/issues/10844). [#10847](https://github.com/ClickHouse/ClickHouse/pull/10847) ([alesapin](https://github.com/alesapin)).
* Fixed potential read of uninitialized memory in cache-dictionary. [#10834](https://github.com/ClickHouse/ClickHouse/pull/10834) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fixed columns order after `Block::sortColumns()`. [#10826](https://github.com/ClickHouse/ClickHouse/pull/10826) ([Azat Khuzhin](https://github.com/azat)).
* Fixed the issue with `ODBC` bridge when no quoting of identifiers is requested. Fixes [#7984] (https://github.com/ClickHouse/ClickHouse/issues/7984). [#10821](https://github.com/ClickHouse/ClickHouse/pull/10821) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fixed `UBSan` and `MSan` report in `DateLUT`. [#10798](https://github.com/ClickHouse/ClickHouse/pull/10798) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fixed incorrect type conversion in key conditions. Fixes [#6287](https://github.com/ClickHouse/ClickHouse/issues/6287). [#10791](https://github.com/ClickHouse/ClickHouse/pull/10791) ([Andrew Onyshchuk](https://github.com/oandrew)).
* Fixed `parallel_view_processing` behavior. Now all insertions into `MATERIALIZED VIEW` without exception should be finished if exception happened. Fixes [#10241](https://github.com/ClickHouse/ClickHouse/issues/10241). [#10757](https://github.com/ClickHouse/ClickHouse/pull/10757) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed combinator `-OrNull` and `-OrDefault` when combined with `-State`. [#10741](https://github.com/ClickHouse/ClickHouse/pull/10741) ([hcz](https://github.com/hczhcz)).
* Fixed possible buffer overflow in function `h3EdgeAngle`. [#10711](https://github.com/ClickHouse/ClickHouse/pull/10711) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fixed bug which locks concurrent alters when table has a lot of parts. [#10659](https://github.com/ClickHouse/ClickHouse/pull/10659) ([alesapin](https://github.com/alesapin)).
* Fixed `nullptr` dereference in `StorageBuffer` if server was shutdown before table startup. [#10641](https://github.com/ClickHouse/ClickHouse/pull/10641) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fixed `optimize_skip_unused_shards` with `LowCardinality`. [#10611](https://github.com/ClickHouse/ClickHouse/pull/10611) ([Azat Khuzhin](https://github.com/azat)).
* Fixed handling condition variable for synchronous mutations. In some cases signals to that condition variable could be lost. [#10588](https://github.com/ClickHouse/ClickHouse/pull/10588) ([Vladimir Chebotarev](https://github.com/excitoon)).
* Fixed possible crash when `createDictionary()` is called before `loadStoredObject()` has finished. [#10587](https://github.com/ClickHouse/ClickHouse/pull/10587) ([Vitaly Baranov](https://github.com/vitlibar)).
* Fixed `SELECT` of column `ALIAS` which default expression type different from column type. [#10563](https://github.com/ClickHouse/ClickHouse/pull/10563) ([Azat Khuzhin](https://github.com/azat)).
* Implemented comparison between DateTime64 and String values. [#10560](https://github.com/ClickHouse/ClickHouse/pull/10560) ([Vasily Nemkov](https://github.com/Enmk)).
* Disable `GROUP BY` sharding_key optimization by default (`optimize_distributed_group_by_sharding_key` had been introduced and turned of by default, due to trickery of sharding_key analyzing, simple example is `if` in sharding key) and fix it for `WITH ROLLUP/CUBE/TOTALS`. [#10516](https://github.com/ClickHouse/ClickHouse/pull/10516) ([Azat Khuzhin](https://github.com/azat)).
* Fixed [#10263](https://github.com/ClickHouse/ClickHouse/issues/10263). [#10486](https://github.com/ClickHouse/ClickHouse/pull/10486) ([Azat Khuzhin](https://github.com/azat)).
* Added tests about `max_rows_to_sort` setting. [#10268](https://github.com/ClickHouse/ClickHouse/pull/10268) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Added backward compatibility for create bloom filter index. [#10551](https://github.com/ClickHouse/ClickHouse/issues/10551). [#10569](https://github.com/ClickHouse/ClickHouse/pull/10569) ([Winter Zhang](https://github.com/zhang2014)).

### ClickHouse release v20.4.2.9, 2020-05-12

#### Backward Incompatible Change
* System tables (e.g. system.query_log, system.trace_log, system.metric_log) are using compact data part format for parts smaller than 10 MiB in size. Compact data part format is supported since version 20.3. If you are going to downgrade to version less than 20.3, you should manually delete table data for system logs in `/var/lib/clickhouse/data/system/`.
* When string comparison involves FixedString and compared arguments are of different sizes, do comparison as if smaller string is padded to the length of the larger. This is intented for SQL compatibility if we imagine that FixedString data type corresponds to SQL CHAR. This closes [#9272](https://github.com/ClickHouse/ClickHouse/issues/9272). [#10363](https://github.com/ClickHouse/ClickHouse/pull/10363) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Make SHOW CREATE TABLE multiline. Now it is more readable and more like MySQL. [#10049](https://github.com/ClickHouse/ClickHouse/pull/10049) ([Azat Khuzhin](https://github.com/azat))
* Added a setting `validate_polygons` that is used in `pointInPolygon` function and enabled by default. [#9857](https://github.com/ClickHouse/ClickHouse/pull/9857) ([alexey-milovidov](https://github.com/alexey-milovidov))

#### New Feature
* Add support for secured connection from ClickHouse to Zookeeper [#10184](https://github.com/ClickHouse/ClickHouse/pull/10184) ([Konstantin Lebedev](https://github.com/xzkostyan))
* Support custom HTTP handlers. See ISSUES-5436 for description. [#7572](https://github.com/ClickHouse/ClickHouse/pull/7572) ([Winter Zhang](https://github.com/zhang2014))
* Add MessagePack Input/Output format. [#9889](https://github.com/ClickHouse/ClickHouse/pull/9889) ([Kruglov Pavel](https://github.com/Avogar))
* Add Regexp input format. [#9196](https://github.com/ClickHouse/ClickHouse/pull/9196) ([Kruglov Pavel](https://github.com/Avogar))
* Added output format `Markdown` for embedding tables in markdown documents. [#10317](https://github.com/ClickHouse/ClickHouse/pull/10317) ([Kruglov Pavel](https://github.com/Avogar))
* Added support for custom settings section in dictionaries. Also fixes issue [#2829](https://github.com/ClickHouse/ClickHouse/issues/2829). [#10137](https://github.com/ClickHouse/ClickHouse/pull/10137) ([Artem Streltsov](https://github.com/kekekekule))
* Added custom settings support in DDL-queries for CREATE DICTIONARY [#10465](https://github.com/ClickHouse/ClickHouse/pull/10465) ([Artem Streltsov](https://github.com/kekekekule))
* Add simple server-wide memory profiler that will collect allocation contexts when server memory usage becomes higher than the next allocation threshold. [#10444](https://github.com/ClickHouse/ClickHouse/pull/10444) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add setting `always_fetch_merged_part` which restrict replica to merge parts by itself and always prefer dowloading from other replicas. [#10379](https://github.com/ClickHouse/ClickHouse/pull/10379) ([alesapin](https://github.com/alesapin))
* Add function JSONExtractKeysAndValuesRaw which extracts raw data from JSON objects [#10378](https://github.com/ClickHouse/ClickHouse/pull/10378) ([hcz](https://github.com/hczhcz))
* Add memory usage from OS to `system.asynchronous_metrics`. [#10361](https://github.com/ClickHouse/ClickHouse/pull/10361) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added generic variants for functions `least` and `greatest`. Now they work with arbitrary number of arguments of arbitrary types. This fixes [#4767](https://github.com/ClickHouse/ClickHouse/issues/4767) [#10318](https://github.com/ClickHouse/ClickHouse/pull/10318) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Now ClickHouse controls timeouts of dictionary sources on its side. Two new settings added to cache dictionary configuration: `strict_max_lifetime_seconds`, which is `max_lifetime` by default, and `query_wait_timeout_milliseconds`, which is one minute by default. The first settings is also useful with `allow_read_expired_keys` settings (to forbid reading very expired keys). [#10337](https://github.com/ClickHouse/ClickHouse/pull/10337) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Add log_queries_min_type to filter which entries will be written to query_log [#10053](https://github.com/ClickHouse/ClickHouse/pull/10053) ([Azat Khuzhin](https://github.com/azat))
* Added function `isConstant`. This function checks whether its argument is constant expression and returns 1 or 0. It is intended for development, debugging and demonstration purposes. [#10198](https://github.com/ClickHouse/ClickHouse/pull/10198) ([alexey-milovidov](https://github.com/alexey-milovidov))
* add joinGetOrNull to return NULL when key is missing instead of returning the default value. [#10094](https://github.com/ClickHouse/ClickHouse/pull/10094) ([Amos Bird](https://github.com/amosbird))
* Consider `NULL` to be equal to `NULL` in `IN` operator, if the option `transform_null_in` is set. [#10085](https://github.com/ClickHouse/ClickHouse/pull/10085) ([achimbab](https://github.com/achimbab))
* Add `ALTER TABLE ... RENAME COLUMN` for MergeTree table engines family. [#9948](https://github.com/ClickHouse/ClickHouse/pull/9948) ([alesapin](https://github.com/alesapin))
* Support parallel distributed INSERT SELECT. [#9759](https://github.com/ClickHouse/ClickHouse/pull/9759) ([vxider](https://github.com/Vxider))
* Add ability to query Distributed over Distributed (w/o `distributed_group_by_no_merge`) ... [#9923](https://github.com/ClickHouse/ClickHouse/pull/9923) ([Azat Khuzhin](https://github.com/azat))
* Add function `arrayReduceInRanges` which aggregates array elements in given ranges. [#9598](https://github.com/ClickHouse/ClickHouse/pull/9598) ([hcz](https://github.com/hczhcz))
* Add Dictionary Status on prometheus exporter. [#9622](https://github.com/ClickHouse/ClickHouse/pull/9622) ([Guillaume Tassery](https://github.com/YiuRULE))
* Add function arrayAUC [#8698](https://github.com/ClickHouse/ClickHouse/pull/8698) ([taiyang-li](https://github.com/taiyang-li))
* Support `DROP VIEW` statement for better TPC-H compatibility. [#9831](https://github.com/ClickHouse/ClickHouse/pull/9831) ([Amos Bird](https://github.com/amosbird))
* Add 'strict_order' option to windowFunnel() [#9773](https://github.com/ClickHouse/ClickHouse/pull/9773) ([achimbab](https://github.com/achimbab))
* Support `DATE` and `TIMESTAMP` SQL operators, e.g. `SELECT date '2001-01-01'` [#9691](https://github.com/ClickHouse/ClickHouse/pull/9691) ([Artem Zuikov](https://github.com/4ertus2))

#### Experimental Feature
* Added experimental database engine Atomic. It supports non-blocking `DROP` and `RENAME TABLE` queries and atomic `EXCHANGE TABLES t1 AND t2` query [#7512](https://github.com/ClickHouse/ClickHouse/pull/7512) ([tavplubix](https://github.com/tavplubix))
* Initial support for ReplicatedMergeTree over S3 (it works in suboptimal way) [#10126](https://github.com/ClickHouse/ClickHouse/pull/10126) ([Pavel Kovalenko](https://github.com/Jokser))

#### Bug Fix
* Fixed incorrect scalar results inside inner query of `MATERIALIZED VIEW` in case if this query contained dependent table [#10603](https://github.com/ClickHouse/ClickHouse/pull/10603) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fixed bug, which caused HTTP requests to get stuck on client closing connection when `readonly=2` and `cancel_http_readonly_queries_on_client_close=1`. [#10684](https://github.com/ClickHouse/ClickHouse/pull/10684) ([tavplubix](https://github.com/tavplubix))
* Fix segfault in StorageBuffer when exception is thrown on server startup. Fixes [#10550](https://github.com/ClickHouse/ClickHouse/issues/10550) [#10609](https://github.com/ClickHouse/ClickHouse/pull/10609) ([tavplubix](https://github.com/tavplubix))
* The query`SYSTEM DROP DNS CACHE` now also drops caches used to check if user is allowed to connect from some IP addresses [#10608](https://github.com/ClickHouse/ClickHouse/pull/10608) ([tavplubix](https://github.com/tavplubix))
* Fix usage of multiple `IN` operators with an identical set in one query. Fixes [#10539](https://github.com/ClickHouse/ClickHouse/issues/10539) [#10686](https://github.com/ClickHouse/ClickHouse/pull/10686) ([Anton Popov](https://github.com/CurtizJ))
* Fix crash in `generateRandom` with nested types. Fixes [#10583](https://github.com/ClickHouse/ClickHouse/issues/10583). [#10734](https://github.com/ClickHouse/ClickHouse/pull/10734) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix data corruption for `LowCardinality(FixedString)` key column in `SummingMergeTree` which could have happened after merge. Fixes [#10489](https://github.com/ClickHouse/ClickHouse/issues/10489). [#10721](https://github.com/ClickHouse/ClickHouse/pull/10721) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix logic for aggregation_memory_efficient_merge_threads setting. [#10667](https://github.com/ClickHouse/ClickHouse/pull/10667) ([palasonic1](https://github.com/palasonic1))
* Fix disappearing totals. Totals could have being filtered if query had `JOIN` or subquery with external `WHERE` condition. Fixes [#10674](https://github.com/ClickHouse/ClickHouse/issues/10674) [#10698](https://github.com/ClickHouse/ClickHouse/pull/10698) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix the lack of parallel execution of remote queries with `distributed_aggregation_memory_efficient` enabled. Fixes [#10655](https://github.com/ClickHouse/ClickHouse/issues/10655) [#10664](https://github.com/ClickHouse/ClickHouse/pull/10664) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix possible incorrect number of rows for queries with `LIMIT`. Fixes [#10566](https://github.com/ClickHouse/ClickHouse/issues/10566), [#10709](https://github.com/ClickHouse/ClickHouse/issues/10709) [#10660](https://github.com/ClickHouse/ClickHouse/pull/10660) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix index corruption, which may occur in some cases after merging compact parts into another compact part. [#10531](https://github.com/ClickHouse/ClickHouse/pull/10531) ([Anton Popov](https://github.com/CurtizJ))
* Fix the situation, when mutation finished all parts, but hung up in `is_done=0`. [#10526](https://github.com/ClickHouse/ClickHouse/pull/10526) ([alesapin](https://github.com/alesapin))
* Fix overflow at beginning of unix epoch for timezones with fractional offset from UTC. Fixes [#9335](https://github.com/ClickHouse/ClickHouse/issues/9335). [#10513](https://github.com/ClickHouse/ClickHouse/pull/10513) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Better diagnostics for input formats. Fixes [#10204](https://github.com/ClickHouse/ClickHouse/issues/10204) [#10418](https://github.com/ClickHouse/ClickHouse/pull/10418) ([tavplubix](https://github.com/tavplubix))
* Fix numeric overflow in `simpleLinearRegression()` over large integers [#10474](https://github.com/ClickHouse/ClickHouse/pull/10474) ([hcz](https://github.com/hczhcz))
* Fix use-after-free in Distributed shutdown, avoid waiting for sending all batches [#10491](https://github.com/ClickHouse/ClickHouse/pull/10491) ([Azat Khuzhin](https://github.com/azat))
* Add CA certificates to clickhouse-server docker image [#10476](https://github.com/ClickHouse/ClickHouse/pull/10476) ([filimonov](https://github.com/filimonov))
* Fix a rare endless loop that might have occurred when using the `addressToLine` function or AggregateFunctionState columns. [#10466](https://github.com/ClickHouse/ClickHouse/pull/10466) ([Alexander Kuzmenkov](https://github.com/akuzm))
* Handle zookeeper "no node error" during distributed query [#10050](https://github.com/ClickHouse/ClickHouse/pull/10050) ([Daniel Chen](https://github.com/Phantomape))
* Fix bug when server cannot attach table after column's default was altered. [#10441](https://github.com/ClickHouse/ClickHouse/pull/10441) ([alesapin](https://github.com/alesapin))
* Implicitly cast the default expression type to the column type for the ALIAS columns [#10563](https://github.com/ClickHouse/ClickHouse/pull/10563) ([Azat Khuzhin](https://github.com/azat))
* Don't remove metadata directory if `ATTACH DATABASE` fails [#10442](https://github.com/ClickHouse/ClickHouse/pull/10442) ([Winter Zhang](https://github.com/zhang2014))
* Avoid dependency on system tzdata. Fixes loading of `Africa/Casablanca` timezone on CentOS 8. Fixes [#10211](https://github.com/ClickHouse/ClickHouse/issues/10211) [#10425](https://github.com/ClickHouse/ClickHouse/pull/10425) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix some issues if data is inserted with quorum and then gets deleted (DROP PARTITION, TTL, etc.). It led to stuck of INSERTs or false-positive exceptions in SELECTs. Fixes [#9946](https://github.com/ClickHouse/ClickHouse/issues/9946) [#10188](https://github.com/ClickHouse/ClickHouse/pull/10188) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Check the number and type of arguments when creating BloomFilter index [#9623](https://github.com/ClickHouse/ClickHouse/issues/9623) [#10431](https://github.com/ClickHouse/ClickHouse/pull/10431) ([Winter Zhang](https://github.com/zhang2014))
* Prefer `fallback_to_stale_replicas` over `skip_unavailable_shards`, otherwise when both settings specified and there are no up-to-date replicas the query will fail (patch from @alex-zaitsev ) [#10422](https://github.com/ClickHouse/ClickHouse/pull/10422) ([Azat Khuzhin](https://github.com/azat))
* Fix the issue when a query with ARRAY JOIN, ORDER BY and LIMIT may return incomplete result. Fixes [#10226](https://github.com/ClickHouse/ClickHouse/issues/10226). [#10427](https://github.com/ClickHouse/ClickHouse/pull/10427) ([Vadim Plakhtinskiy](https://github.com/VadimPlh))
* Add database name to dictionary name after DETACH/ATTACH. Fixes system.dictionaries table and `SYSTEM RELOAD` query [#10415](https://github.com/ClickHouse/ClickHouse/pull/10415) ([Azat Khuzhin](https://github.com/azat))
* Fix possible incorrect result for extremes in processors pipeline. [#10131](https://github.com/ClickHouse/ClickHouse/pull/10131) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix possible segfault when the setting `distributed_group_by_no_merge` is enabled (introduced in 20.3.7.46 by [#10131](https://github.com/ClickHouse/ClickHouse/issues/10131)). [#10399](https://github.com/ClickHouse/ClickHouse/pull/10399) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix wrong flattening of `Array(Tuple(...))` data types. Fixes [#10259](https://github.com/ClickHouse/ClickHouse/issues/10259) [#10390](https://github.com/ClickHouse/ClickHouse/pull/10390) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix column names of constants inside JOIN that may clash with names of constants outside of JOIN [#9950](https://github.com/ClickHouse/ClickHouse/pull/9950) ([Alexander Kuzmenkov](https://github.com/akuzm))
* Fix order of columns after Block::sortColumns() [#10826](https://github.com/ClickHouse/ClickHouse/pull/10826) ([Azat Khuzhin](https://github.com/azat))
* Fix possible `Pipeline stuck` error in `ConcatProcessor` which may happen in remote query. [#10381](https://github.com/ClickHouse/ClickHouse/pull/10381) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Don't make disk reservations for aggregations. Fixes [#9241](https://github.com/ClickHouse/ClickHouse/issues/9241) [#10375](https://github.com/ClickHouse/ClickHouse/pull/10375) ([Azat Khuzhin](https://github.com/azat))
* Fix wrong behaviour of datetime functions for timezones that has altered between positive and negative offsets from UTC (e.g. Pacific/Kiritimati). Fixes [#7202](https://github.com/ClickHouse/ClickHouse/issues/7202) [#10369](https://github.com/ClickHouse/ClickHouse/pull/10369) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Avoid infinite loop in `dictIsIn` function. Fixes #515 [#10365](https://github.com/ClickHouse/ClickHouse/pull/10365) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Disable GROUP BY sharding_key optimization by default and fix it for WITH ROLLUP/CUBE/TOTALS [#10516](https://github.com/ClickHouse/ClickHouse/pull/10516) ([Azat Khuzhin](https://github.com/azat))
* Check for error code when checking parts and don't mark part as broken if the error is like "not enough memory". Fixes [#6269](https://github.com/ClickHouse/ClickHouse/issues/6269) [#10364](https://github.com/ClickHouse/ClickHouse/pull/10364) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Show information about not loaded dictionaries in system tables. [#10234](https://github.com/ClickHouse/ClickHouse/pull/10234) ([Vitaly Baranov](https://github.com/vitlibar))
* Fix nullptr dereference in StorageBuffer if server was shutdown before table startup. [#10641](https://github.com/ClickHouse/ClickHouse/pull/10641) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed `DROP` vs `OPTIMIZE` race in `ReplicatedMergeTree`. `DROP` could left some garbage in replica path in ZooKeeper if there was concurrent `OPTIMIZE` query. [#10312](https://github.com/ClickHouse/ClickHouse/pull/10312) ([tavplubix](https://github.com/tavplubix))
* Fix 'Logical error: CROSS JOIN has expressions' error for queries with comma and names joins mix. Fixes [#9910](https://github.com/ClickHouse/ClickHouse/issues/9910) [#10311](https://github.com/ClickHouse/ClickHouse/pull/10311) ([Artem Zuikov](https://github.com/4ertus2))
* Fix queries with `max_bytes_before_external_group_by`. [#10302](https://github.com/ClickHouse/ClickHouse/pull/10302) ([Artem Zuikov](https://github.com/4ertus2))
* Fix the issue with limiting maximum recursion depth in parser in certain cases. This fixes [#10283](https://github.com/ClickHouse/ClickHouse/issues/10283) This fix may introduce minor incompatibility: long and deep queries via clickhouse-client may refuse to work, and you should adjust settings `max_query_size` and `max_parser_depth` accordingly. [#10295](https://github.com/ClickHouse/ClickHouse/pull/10295) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Allow to use `count(*)` with multiple JOINs. Fixes [#9853](https://github.com/ClickHouse/ClickHouse/issues/9853) [#10291](https://github.com/ClickHouse/ClickHouse/pull/10291) ([Artem Zuikov](https://github.com/4ertus2))
* Fix error `Pipeline stuck` with `max_rows_to_group_by` and `group_by_overflow_mode = 'break'`. [#10279](https://github.com/ClickHouse/ClickHouse/pull/10279) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix 'Cannot add column' error while creating `range_hashed` dictionary using DDL query. Fixes [#10093](https://github.com/ClickHouse/ClickHouse/issues/10093). [#10235](https://github.com/ClickHouse/ClickHouse/pull/10235) ([alesapin](https://github.com/alesapin))
* Fix rare possible exception `Cannot drain connections: cancel first`. [#10239](https://github.com/ClickHouse/ClickHouse/pull/10239) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fixed bug where ClickHouse would throw "Unknown function lambda." error message when user tries to run ALTER UPDATE/DELETE on tables with ENGINE = Replicated*. Check for nondeterministic functions now handles lambda expressions correctly. [#10237](https://github.com/ClickHouse/ClickHouse/pull/10237) ([Alexander Kazakov](https://github.com/Akazz))
* Fixed reasonably rare segfault in StorageSystemTables that happens when SELECT ... FROM system.tables is run on a database with Lazy engine. [#10209](https://github.com/ClickHouse/ClickHouse/pull/10209) ([Alexander Kazakov](https://github.com/Akazz))
* Fix possible infinite query execution when the query actually should stop on LIMIT, while reading from infinite source like `system.numbers` or `system.zeros`. [#10206](https://github.com/ClickHouse/ClickHouse/pull/10206) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fixed "generateRandom" function for Date type. This fixes [#9973](https://github.com/ClickHouse/ClickHouse/issues/9973). Fix an edge case when dates with year 2106 are inserted to MergeTree tables with old-style partitioning but partitions are named with year 1970. [#10218](https://github.com/ClickHouse/ClickHouse/pull/10218) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Convert types if the table definition of a View does not correspond to the SELECT query. This fixes [#10180](https://github.com/ClickHouse/ClickHouse/issues/10180) and [#10022](https://github.com/ClickHouse/ClickHouse/issues/10022) [#10217](https://github.com/ClickHouse/ClickHouse/pull/10217) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix `parseDateTimeBestEffort` for strings in RFC-2822 when day of week is Tuesday or Thursday. This fixes [#10082](https://github.com/ClickHouse/ClickHouse/issues/10082) [#10214](https://github.com/ClickHouse/ClickHouse/pull/10214) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix column names of constants inside JOIN that may clash with names of constants outside of JOIN. [#10207](https://github.com/ClickHouse/ClickHouse/pull/10207) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix move-to-prewhere optimization in presense of arrayJoin functions (in certain cases). This fixes [#10092](https://github.com/ClickHouse/ClickHouse/issues/10092) [#10195](https://github.com/ClickHouse/ClickHouse/pull/10195) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix issue with separator appearing in SCRAMBLE for native mysql-connector-java (JDBC) [#10140](https://github.com/ClickHouse/ClickHouse/pull/10140) ([BohuTANG](https://github.com/BohuTANG))
* Fix using the current database for an access checking when the database isn't specified. [#10192](https://github.com/ClickHouse/ClickHouse/pull/10192) ([Vitaly Baranov](https://github.com/vitlibar))
* Fix ALTER of tables with compact parts. [#10130](https://github.com/ClickHouse/ClickHouse/pull/10130) ([Anton Popov](https://github.com/CurtizJ))
* Add the ability to relax the restriction on non-deterministic functions usage in mutations with `allow_nondeterministic_mutations` setting. [#10186](https://github.com/ClickHouse/ClickHouse/pull/10186) ([filimonov](https://github.com/filimonov))
* Fix `DROP TABLE` invoked for dictionary [#10165](https://github.com/ClickHouse/ClickHouse/pull/10165) ([Azat Khuzhin](https://github.com/azat))
* Convert blocks if structure does not match when doing `INSERT` into Distributed table [#10135](https://github.com/ClickHouse/ClickHouse/pull/10135) ([Azat Khuzhin](https://github.com/azat))
* The number of rows was logged incorrectly (as sum across all parts) when inserted block is split by parts with partition key. [#10138](https://github.com/ClickHouse/ClickHouse/pull/10138) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add some arguments check and support identifier arguments for MySQL Database Engine [#10077](https://github.com/ClickHouse/ClickHouse/pull/10077) ([Winter Zhang](https://github.com/zhang2014))
* Fix incorrect `index_granularity_bytes` check while creating new replica. Fixes [#10098](https://github.com/ClickHouse/ClickHouse/issues/10098). [#10121](https://github.com/ClickHouse/ClickHouse/pull/10121) ([alesapin](https://github.com/alesapin))
* Fix bug in `CHECK TABLE` query when table contain skip indices. [#10068](https://github.com/ClickHouse/ClickHouse/pull/10068) ([alesapin](https://github.com/alesapin))
* Fix Distributed-over-Distributed with the only one shard in a nested table [#9997](https://github.com/ClickHouse/ClickHouse/pull/9997) ([Azat Khuzhin](https://github.com/azat))
* Fix possible rows loss for queries with `JOIN` and `UNION ALL`. Fixes [#9826](https://github.com/ClickHouse/ClickHouse/issues/9826), [#10113](https://github.com/ClickHouse/ClickHouse/issues/10113). ... [#10099](https://github.com/ClickHouse/ClickHouse/pull/10099) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix bug in dictionary when local clickhouse server is used as source. It may caused memory corruption if types in dictionary and source are not compatible. [#10071](https://github.com/ClickHouse/ClickHouse/pull/10071) ([alesapin](https://github.com/alesapin))
* Fixed replicated tables startup when updating from an old ClickHouse version where `/table/replicas/replica_name/metadata` node doesn't exist. Fixes [#10037](https://github.com/ClickHouse/ClickHouse/issues/10037). [#10095](https://github.com/ClickHouse/ClickHouse/pull/10095) ([alesapin](https://github.com/alesapin))
* Fix error `Cannot clone block with columns because block has 0 columns ... While executing GroupingAggregatedTransform`. It happened when setting `distributed_aggregation_memory_efficient` was enabled, and distributed query read aggregating data with mixed single and two-level aggregation from different shards.  [#10063](https://github.com/ClickHouse/ClickHouse/pull/10063) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix deadlock when database with materialized view failed attach at start [#10054](https://github.com/ClickHouse/ClickHouse/pull/10054) ([Azat Khuzhin](https://github.com/azat))
* Fix a segmentation fault that could occur in GROUP BY over string keys containing trailing zero bytes ([#8636](https://github.com/ClickHouse/ClickHouse/issues/8636), [#8925](https://github.com/ClickHouse/ClickHouse/issues/8925)). ... [#10025](https://github.com/ClickHouse/ClickHouse/pull/10025) ([Alexander Kuzmenkov](https://github.com/akuzm))
* Fix wrong results of distributed queries when alias could override qualified column name. Fixes [#9672](https://github.com/ClickHouse/ClickHouse/issues/9672) [#9714](https://github.com/ClickHouse/ClickHouse/issues/9714) [#9972](https://github.com/ClickHouse/ClickHouse/pull/9972) ([Artem Zuikov](https://github.com/4ertus2))
* Fix possible deadlock in `SYSTEM RESTART REPLICAS` [#9955](https://github.com/ClickHouse/ClickHouse/pull/9955) ([tavplubix](https://github.com/tavplubix))
* Fix the number of threads used for remote query execution (performance regression, since 20.3). This happened when query from `Distributed` table was executed simultaneously on local and remote shards. Fixes [#9965](https://github.com/ClickHouse/ClickHouse/issues/9965) [#9971](https://github.com/ClickHouse/ClickHouse/pull/9971) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fixed `DeleteOnDestroy` logic in `ATTACH PART` which could lead to automatic removal of attached part and added few tests [#9410](https://github.com/ClickHouse/ClickHouse/pull/9410) ([Vladimir Chebotarev](https://github.com/excitoon))
* Fix a bug with `ON CLUSTER` DDL queries freezing on server startup. [#9927](https://github.com/ClickHouse/ClickHouse/pull/9927) ([Gagan Arneja](https://github.com/garneja))
* Fix bug in which the necessary tables weren't retrieved at one of the processing stages of queries to some databases. Fixes [#9699](https://github.com/ClickHouse/ClickHouse/issues/9699). [#9949](https://github.com/ClickHouse/ClickHouse/pull/9949) ([achulkov2](https://github.com/achulkov2))
* Fix 'Not found column in block' error when `JOIN` appears with `TOTALS`. Fixes [#9839](https://github.com/ClickHouse/ClickHouse/issues/9839) [#9939](https://github.com/ClickHouse/ClickHouse/pull/9939) ([Artem Zuikov](https://github.com/4ertus2))
* Fix parsing multiple hosts set in the CREATE USER command [#9924](https://github.com/ClickHouse/ClickHouse/pull/9924) ([Vitaly Baranov](https://github.com/vitlibar))
* Fix `TRUNCATE` for Join table engine ([#9917](https://github.com/ClickHouse/ClickHouse/issues/9917)). [#9920](https://github.com/ClickHouse/ClickHouse/pull/9920) ([Amos Bird](https://github.com/amosbird))
* Fix race condition between drop and optimize in `ReplicatedMergeTree`. [#9901](https://github.com/ClickHouse/ClickHouse/pull/9901) ([alesapin](https://github.com/alesapin))
* Fix `DISTINCT` for Distributed when `optimize_skip_unused_shards` is set. [#9808](https://github.com/ClickHouse/ClickHouse/pull/9808) ([Azat Khuzhin](https://github.com/azat))
* Fix "scalar doesn't exist" error in ALTERs ([#9878](https://github.com/ClickHouse/ClickHouse/issues/9878)). ... [#9904](https://github.com/ClickHouse/ClickHouse/pull/9904) ([Amos Bird](https://github.com/amosbird))
* Fix error with qualified names in `distributed_product_mode=\'local\'`. Fixes [#4756](https://github.com/ClickHouse/ClickHouse/issues/4756) [#9891](https://github.com/ClickHouse/ClickHouse/pull/9891) ([Artem Zuikov](https://github.com/4ertus2))
* For INSERT queries shards now do clamp the settings from the initiator to their constraints instead of throwing an exception. This fix allows to send INSERT queries to a shard with another constraints. This change improves fix [#9447](https://github.com/ClickHouse/ClickHouse/issues/9447). [#9852](https://github.com/ClickHouse/ClickHouse/pull/9852) ([Vitaly Baranov](https://github.com/vitlibar))
* Add some retries when commiting offsets to Kafka broker, since it can reject commit if during `offsets.commit.timeout.ms` there were no enough replicas available for the `__consumer_offsets` topic [#9884](https://github.com/ClickHouse/ClickHouse/pull/9884) ([filimonov](https://github.com/filimonov))
* Fix Distributed engine behavior when virtual columns of the underlying table used in `WHERE` [#9847](https://github.com/ClickHouse/ClickHouse/pull/9847) ([Azat Khuzhin](https://github.com/azat))
* Fixed some cases when timezone of the function argument wasn't used properly. [#9574](https://github.com/ClickHouse/ClickHouse/pull/9574) ([Vasily Nemkov](https://github.com/Enmk))
* Fix 'Different expressions with the same alias' error when query has PREWHERE and WHERE on distributed table and `SET distributed_product_mode = 'local'`. [#9871](https://github.com/ClickHouse/ClickHouse/pull/9871) ([Artem Zuikov](https://github.com/4ertus2))
* Fix mutations excessive memory consumption for tables with a composite primary key. This fixes [#9850](https://github.com/ClickHouse/ClickHouse/issues/9850). [#9860](https://github.com/ClickHouse/ClickHouse/pull/9860) ([alesapin](https://github.com/alesapin))
* Fix calculating grants for introspection functions from the setting `allow_introspection_functions`. [#9840](https://github.com/ClickHouse/ClickHouse/pull/9840) ([Vitaly Baranov](https://github.com/vitlibar))
* Fix max_distributed_connections (w/ and w/o Processors) [#9673](https://github.com/ClickHouse/ClickHouse/pull/9673) ([Azat Khuzhin](https://github.com/azat))
* Fix possible exception `Got 0 in totals chunk, expected 1` on client. It happened for queries with `JOIN` in case if right joined table had zero rows. Example: `select * from system.one t1 join system.one t2 on t1.dummy = t2.dummy limit 0 FORMAT TabSeparated;`. Fixes [#9777](https://github.com/ClickHouse/ClickHouse/issues/9777). ... [#9823](https://github.com/ClickHouse/ClickHouse/pull/9823) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix 'COMMA to CROSS JOIN rewriter is not enabled or cannot rewrite query' error in case of subqueries with COMMA JOIN out of tables lists (i.e. in WHERE). Fixes [#9782](https://github.com/ClickHouse/ClickHouse/issues/9782) [#9830](https://github.com/ClickHouse/ClickHouse/pull/9830) ([Artem Zuikov](https://github.com/4ertus2))
* Fix server crashing when `optimize_skip_unused_shards` is set and expression for key can't be converted to its field type [#9804](https://github.com/ClickHouse/ClickHouse/pull/9804) ([Azat Khuzhin](https://github.com/azat))
* Fix empty string handling in `splitByString`. [#9767](https://github.com/ClickHouse/ClickHouse/pull/9767) ([hcz](https://github.com/hczhcz))
* Fix broken `ALTER TABLE DELETE COLUMN` query for compact parts. [#9779](https://github.com/ClickHouse/ClickHouse/pull/9779) ([alesapin](https://github.com/alesapin))
* Fixed missing `rows_before_limit_at_least` for queries over http (with processors pipeline). Fixes [#9730](https://github.com/ClickHouse/ClickHouse/issues/9730) [#9757](https://github.com/ClickHouse/ClickHouse/pull/9757) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix excessive memory consumption in `ALTER` queries (mutations). This fixes [#9533](https://github.com/ClickHouse/ClickHouse/issues/9533) and [#9670](https://github.com/ClickHouse/ClickHouse/issues/9670). [#9754](https://github.com/ClickHouse/ClickHouse/pull/9754) ([alesapin](https://github.com/alesapin))
* Fix possible permanent "Cannot schedule a task" error. [#9154](https://github.com/ClickHouse/ClickHouse/pull/9154) ([Azat Khuzhin](https://github.com/azat))
* Fix bug in backquoting in external dictionaries DDL. Fixes [#9619](https://github.com/ClickHouse/ClickHouse/issues/9619). [#9734](https://github.com/ClickHouse/ClickHouse/pull/9734) ([alesapin](https://github.com/alesapin))
* Fixed data race in `text_log`. It does not correspond to any real bug. [#9726](https://github.com/ClickHouse/ClickHouse/pull/9726) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix bug in a replication that doesn't allow replication to work if the user has executed mutations on the previous version. This fixes [#9645](https://github.com/ClickHouse/ClickHouse/issues/9645). [#9652](https://github.com/ClickHouse/ClickHouse/pull/9652) ([alesapin](https://github.com/alesapin))
* Fixed incorrect internal function names for `sumKahan` and `sumWithOverflow`. It led to exception while using this functions in remote queries. [#9636](https://github.com/ClickHouse/ClickHouse/pull/9636) ([Azat Khuzhin](https://github.com/azat))
* Add setting `use_compact_format_in_distributed_parts_names` which allows to write files for `INSERT` queries into `Distributed` table with more compact format. This fixes [#9647](https://github.com/ClickHouse/ClickHouse/issues/9647). [#9653](https://github.com/ClickHouse/ClickHouse/pull/9653) ([alesapin](https://github.com/alesapin))
* Fix RIGHT and FULL JOIN with LowCardinality in JOIN keys. [#9610](https://github.com/ClickHouse/ClickHouse/pull/9610) ([Artem Zuikov](https://github.com/4ertus2))
* Fix possible exceptions `Size of filter doesn't match size of column` and `Invalid number of rows in Chunk` in `MergeTreeRangeReader`. They could appear while executing `PREWHERE` in some cases. [#9612](https://github.com/ClickHouse/ClickHouse/pull/9612) ([Anton Popov](https://github.com/CurtizJ))
* Allow `ALTER ON CLUSTER` of Distributed tables with internal replication. This fixes [#3268](https://github.com/ClickHouse/ClickHouse/issues/3268) [#9617](https://github.com/ClickHouse/ClickHouse/pull/9617) ([shinoi2](https://github.com/shinoi2))
* Fix issue when timezone was not preserved if you write a simple arithmetic expression like `time + 1` (in contrast to an expression like `time + INTERVAL 1 SECOND`). This fixes [#5743](https://github.com/ClickHouse/ClickHouse/issues/5743) [#9323](https://github.com/ClickHouse/ClickHouse/pull/9323) ([alexey-milovidov](https://github.com/alexey-milovidov))

#### Improvement
* Use time zone when comparing DateTime with string literal. This fixes [#5206](https://github.com/ClickHouse/ClickHouse/issues/5206). [#10515](https://github.com/ClickHouse/ClickHouse/pull/10515) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Print verbose diagnostic info if Decimal value cannot be parsed from text input format. [#10205](https://github.com/ClickHouse/ClickHouse/pull/10205) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add tasks/memory metrics for distributed/buffer schedule pools [#10449](https://github.com/ClickHouse/ClickHouse/pull/10449) ([Azat Khuzhin](https://github.com/azat))
* Display result as soon as it's ready for SELECT DISTINCT queries in clickhouse-local and HTTP interface. This fixes [#8951](https://github.com/ClickHouse/ClickHouse/issues/8951) [#9559](https://github.com/ClickHouse/ClickHouse/pull/9559) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Allow to use `SAMPLE OFFSET` query instead of `cityHash64(PRIMARY KEY) % N == n` for splitting in `clickhouse-copier`. To use this feature, pass `--experimental-use-sample-offset 1` as a command line argument. [#10414](https://github.com/ClickHouse/ClickHouse/pull/10414) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Allow to parse BOM in TSV if the first column cannot contain BOM in its value. This fixes [#10301](https://github.com/ClickHouse/ClickHouse/issues/10301) [#10424](https://github.com/ClickHouse/ClickHouse/pull/10424) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add Avro nested fields insert support [#10354](https://github.com/ClickHouse/ClickHouse/pull/10354) ([Andrew Onyshchuk](https://github.com/oandrew))
* Allowed to alter column in non-modifying data mode when the same type is specified. [#10382](https://github.com/ClickHouse/ClickHouse/pull/10382) ([Vladimir Chebotarev](https://github.com/excitoon))
* Auto `distributed_group_by_no_merge` on GROUP BY sharding key (if `optimize_skip_unused_shards` is set) [#10341](https://github.com/ClickHouse/ClickHouse/pull/10341) ([Azat Khuzhin](https://github.com/azat))
* Optimize queries with LIMIT/LIMIT BY/ORDER BY for distributed with GROUP BY sharding_key [#10373](https://github.com/ClickHouse/ClickHouse/pull/10373) ([Azat Khuzhin](https://github.com/azat))
* Added a setting `max_server_memory_usage` to limit total memory usage of the server. The metric `MemoryTracking` is now calculated without a drift. The setting `max_memory_usage_for_all_queries` is now obsolete and does nothing. This closes [#10293](https://github.com/ClickHouse/ClickHouse/issues/10293). [#10362](https://github.com/ClickHouse/ClickHouse/pull/10362) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add config option `system_tables_lazy_load`. If it's set to false, then system tables with logs are loaded at the server startup. [Alexander Burmak](https://github.com/Alex-Burmak), [Svyatoslav Tkhon Il Pak](https://github.com/DeifyTheGod), [#9642](https://github.com/ClickHouse/ClickHouse/pull/9642) [#10359](https://github.com/ClickHouse/ClickHouse/pull/10359) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Use background thread pool (background_schedule_pool_size) for distributed sends [#10263](https://github.com/ClickHouse/ClickHouse/pull/10263) ([Azat Khuzhin](https://github.com/azat))
* Use background thread pool for background buffer flushes. [#10315](https://github.com/ClickHouse/ClickHouse/pull/10315) ([Azat Khuzhin](https://github.com/azat))
* Support for one special case of removing incompletely written parts. This fixes [#9940](https://github.com/ClickHouse/ClickHouse/issues/9940). [#10221](https://github.com/ClickHouse/ClickHouse/pull/10221) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Use isInjective() over manual list of such functions for GROUP BY optimization. [#10342](https://github.com/ClickHouse/ClickHouse/pull/10342) ([Azat Khuzhin](https://github.com/azat))
* Avoid printing error message in log if client sends RST packet immediately on connect. It is typical behaviour of IPVS balancer with keepalived and VRRP. This fixes [#1851](https://github.com/ClickHouse/ClickHouse/issues/1851) [#10274](https://github.com/ClickHouse/ClickHouse/pull/10274) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Allow to parse `+inf` for floating point types. This closes [#1839](https://github.com/ClickHouse/ClickHouse/issues/1839) [#10272](https://github.com/ClickHouse/ClickHouse/pull/10272) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Implemented `generateRandom` table function for Nested types. This closes [#9903](https://github.com/ClickHouse/ClickHouse/issues/9903) [#10219](https://github.com/ClickHouse/ClickHouse/pull/10219) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Provide `max_allowed_packed` in MySQL compatibility interface that will help some clients to communicate with ClickHouse via MySQL protocol. [#10199](https://github.com/ClickHouse/ClickHouse/pull/10199) ([BohuTANG](https://github.com/BohuTANG))
* Allow literals for GLOBAL IN (i.e. `SELECT * FROM remote('localhost', system.one) WHERE dummy global in (0)`) [#10196](https://github.com/ClickHouse/ClickHouse/pull/10196) ([Azat Khuzhin](https://github.com/azat))
* Fix various small issues in interactive mode of clickhouse-client [#10194](https://github.com/ClickHouse/ClickHouse/pull/10194) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Avoid superfluous dictionaries load (system.tables, DROP/SHOW CREATE TABLE) [#10164](https://github.com/ClickHouse/ClickHouse/pull/10164) ([Azat Khuzhin](https://github.com/azat))
* Update to RWLock: timeout parameter for getLock() + implementation reworked to be phase fair [#10073](https://github.com/ClickHouse/ClickHouse/pull/10073) ([Alexander Kazakov](https://github.com/Akazz))
* Enhanced compatibility with native mysql-connector-java(JDBC) [#10021](https://github.com/ClickHouse/ClickHouse/pull/10021) ([BohuTANG](https://github.com/BohuTANG))
* The function `toString` is considered monotonic and can be used for index analysis even when applied in tautological cases with String or LowCardinality(String) argument. [#10110](https://github.com/ClickHouse/ClickHouse/pull/10110) ([Amos Bird](https://github.com/amosbird))
* Add `ON CLUSTER` clause support to commands `{CREATE|DROP} USER/ROLE/ROW POLICY/SETTINGS PROFILE/QUOTA`, `GRANT`. [#9811](https://github.com/ClickHouse/ClickHouse/pull/9811) ([Vitaly Baranov](https://github.com/vitlibar))
* Virtual hosted-style support for S3 URI [#9998](https://github.com/ClickHouse/ClickHouse/pull/9998) ([Pavel Kovalenko](https://github.com/Jokser))
* Now layout type for dictionaries with no arguments can be specified without round brackets in dictionaries DDL-queries. Fixes [#10057](https://github.com/ClickHouse/ClickHouse/issues/10057). [#10064](https://github.com/ClickHouse/ClickHouse/pull/10064) ([alesapin](https://github.com/alesapin))
* Add ability to use number ranges with leading zeros in filepath [#9989](https://github.com/ClickHouse/ClickHouse/pull/9989) ([Olga Khvostikova](https://github.com/stavrolia))
* Better memory usage in CROSS JOIN. [#10029](https://github.com/ClickHouse/ClickHouse/pull/10029) ([Artem Zuikov](https://github.com/4ertus2))
* Try to connect to all shards in cluster when getting structure of remote table and skip_unavailable_shards is set. [#7278](https://github.com/ClickHouse/ClickHouse/pull/7278) ([nvartolomei](https://github.com/nvartolomei))
* Add `total_rows`/`total_bytes` into the `system.tables` table. [#9919](https://github.com/ClickHouse/ClickHouse/pull/9919) ([Azat Khuzhin](https://github.com/azat))
* System log tables now use polymorpic parts by default. [#9905](https://github.com/ClickHouse/ClickHouse/pull/9905) ([Anton Popov](https://github.com/CurtizJ))
* Add type column into system.settings/merge_tree_settings [#9909](https://github.com/ClickHouse/ClickHouse/pull/9909) ([Azat Khuzhin](https://github.com/azat))
* Check for available CPU instructions at server startup as early as possible. [#9888](https://github.com/ClickHouse/ClickHouse/pull/9888) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Remove `ORDER BY` stage from mutations because we read from a single ordered part in a single thread. Also add check that the rows in mutation are ordered by sorting key and this order is not violated. [#9886](https://github.com/ClickHouse/ClickHouse/pull/9886) ([alesapin](https://github.com/alesapin))
* Implement operator LIKE for FixedString at left hand side. This is needed to better support TPC-DS queries. [#9890](https://github.com/ClickHouse/ClickHouse/pull/9890) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add `force_optimize_skip_unused_shards_no_nested` that will disable `force_optimize_skip_unused_shards` for nested Distributed table [#9812](https://github.com/ClickHouse/ClickHouse/pull/9812) ([Azat Khuzhin](https://github.com/azat))
* Now columns size is calculated only once for MergeTree data parts. [#9827](https://github.com/ClickHouse/ClickHouse/pull/9827) ([alesapin](https://github.com/alesapin))
* Evaluate constant expressions for `optimize_skip_unused_shards` (i.e. `SELECT * FROM foo_dist WHERE key=xxHash32(0)`) [#8846](https://github.com/ClickHouse/ClickHouse/pull/8846) ([Azat Khuzhin](https://github.com/azat))
* Check for using `Date` or `DateTime` column from TTL expressions was removed. [#9967](https://github.com/ClickHouse/ClickHouse/pull/9967) ([Vladimir Chebotarev](https://github.com/excitoon))
* DiskS3 hard links optimal implementation. [#9760](https://github.com/ClickHouse/ClickHouse/pull/9760) ([Pavel Kovalenko](https://github.com/Jokser))
* If `set multiple_joins_rewriter_version = 2` enables second version of multiple JOIN rewrites that keeps not clashed column names as is. It supports multiple JOINs with `USING` and allow `select *` for JOINs with subqueries. [#9739](https://github.com/ClickHouse/ClickHouse/pull/9739) ([Artem Zuikov](https://github.com/4ertus2))
* Implementation of "non-blocking" alter for StorageMergeTree [#9606](https://github.com/ClickHouse/ClickHouse/pull/9606) ([alesapin](https://github.com/alesapin))
* Add MergeTree full support for DiskS3 [#9646](https://github.com/ClickHouse/ClickHouse/pull/9646) ([Pavel Kovalenko](https://github.com/Jokser))
* Extend `splitByString` to support empty strings as separators. [#9742](https://github.com/ClickHouse/ClickHouse/pull/9742) ([hcz](https://github.com/hczhcz))
* Add a `timestamp_ns` column to `system.trace_log`. It contains a high-definition timestamp of the trace event, and allows to build timelines of thread profiles ("flame charts"). [#9696](https://github.com/ClickHouse/ClickHouse/pull/9696) ([Alexander Kuzmenkov](https://github.com/akuzm))
* When the setting `send_logs_level` is enabled, avoid intermixing of log messages and query progress. [#9634](https://github.com/ClickHouse/ClickHouse/pull/9634) ([Azat Khuzhin](https://github.com/azat))
* Added support of `MATERIALIZE TTL IN PARTITION`. [#9581](https://github.com/ClickHouse/ClickHouse/pull/9581) ([Vladimir Chebotarev](https://github.com/excitoon))
* Support complex types inside Avro nested fields [#10502](https://github.com/ClickHouse/ClickHouse/pull/10502) ([Andrew Onyshchuk](https://github.com/oandrew))

#### Performance Improvement
* Better insert logic for right table for Partial MergeJoin. [#10467](https://github.com/ClickHouse/ClickHouse/pull/10467) ([Artem Zuikov](https://github.com/4ertus2))
* Improved performance of row-oriented formats (more than 10% for CSV and more than 35% for Avro in case of narrow tables). [#10503](https://github.com/ClickHouse/ClickHouse/pull/10503) ([Andrew Onyshchuk](https://github.com/oandrew))
* Improved performance of queries with explicitly defined sets at right side of IN operator and tuples on the left side. [#10385](https://github.com/ClickHouse/ClickHouse/pull/10385) ([Anton Popov](https://github.com/CurtizJ))
* Use less memory for hash table in HashJoin. [#10416](https://github.com/ClickHouse/ClickHouse/pull/10416) ([Artem Zuikov](https://github.com/4ertus2))
* Special HashJoin over StorageDictionary. Allow rewrite `dictGet()` functions with JOINs. It's not backward incompatible itself but could uncover [#8400](https://github.com/ClickHouse/ClickHouse/issues/8400) on some installations. [#10133](https://github.com/ClickHouse/ClickHouse/pull/10133) ([Artem Zuikov](https://github.com/4ertus2))
* Enable parallel insert of materialized view when its target table supports. [#10052](https://github.com/ClickHouse/ClickHouse/pull/10052) ([vxider](https://github.com/Vxider))
* Improved performance of index analysis with monotonic functions. [#9607](https://github.com/ClickHouse/ClickHouse/pull/9607)[#10026](https://github.com/ClickHouse/ClickHouse/pull/10026) ([Anton Popov](https://github.com/CurtizJ))
* Using SSE2 or SSE4.2 SIMD intrinsics to speed up tokenization in bloom filters. [#9968](https://github.com/ClickHouse/ClickHouse/pull/9968) ([Vasily Nemkov](https://github.com/Enmk))
* Improved performance of queries with explicitly defined sets at right side of `IN` operator. This fixes performance regression in version 20.3. [#9740](https://github.com/ClickHouse/ClickHouse/pull/9740) ([Anton Popov](https://github.com/CurtizJ))
* Now clickhouse-copier splits each partition in number of pieces and copies them independently. [#9075](https://github.com/ClickHouse/ClickHouse/pull/9075) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Adding more aggregation methods. For example TPC-H query 1 will now pick `FixedHashMap<UInt16, AggregateDataPtr>` and gets 25% performance gain [#9829](https://github.com/ClickHouse/ClickHouse/pull/9829) ([Amos Bird](https://github.com/amosbird))
* Use single row counter for multiple streams in pre-limit transform. This helps to avoid uniting pipeline streams in queries with `limit` but without `order by` (like `select f(x) from (select x from t limit 1000000000)`) and use multiple threads for further processing. [#9602](https://github.com/ClickHouse/ClickHouse/pull/9602) ([Nikolai Kochetov](https://github.com/KochetovNicolai))

#### Build/Testing/Packaging Improvement
* Use a fork of AWS SDK libraries from ClickHouse-Extras [#10527](https://github.com/ClickHouse/ClickHouse/pull/10527) ([Pavel Kovalenko](https://github.com/Jokser))
* Add integration tests for new ALTER RENAME COLUMN query. [#10654](https://github.com/ClickHouse/ClickHouse/pull/10654) ([vzakaznikov](https://github.com/vzakaznikov))
* Fix possible signed integer overflow in invocation of function `now64` with wrong arguments. This fixes [#8973](https://github.com/ClickHouse/ClickHouse/issues/8973) [#10511](https://github.com/ClickHouse/ClickHouse/pull/10511) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Split fuzzer and sanitizer configurations to make build config compatible with Oss-fuzz. [#10494](https://github.com/ClickHouse/ClickHouse/pull/10494) ([kyprizel](https://github.com/kyprizel))
* Fixes for clang-tidy on clang-10. [#10420](https://github.com/ClickHouse/ClickHouse/pull/10420) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Display absolute paths in error messages. Otherwise KDevelop fails to navigate to correct file and opens a new file instead. [#10434](https://github.com/ClickHouse/ClickHouse/pull/10434) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added `ASAN_OPTIONS` environment variable to investigate errors in CI stress tests with Address sanitizer. [#10440](https://github.com/ClickHouse/ClickHouse/pull/10440) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Enable ThinLTO for clang builds (experimental). [#10435](https://github.com/ClickHouse/ClickHouse/pull/10435) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Remove accidential dependency on Z3 that may be introduced if the system has Z3 solver installed. [#10426](https://github.com/ClickHouse/ClickHouse/pull/10426) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Move integration tests docker files to docker/ directory. [#10335](https://github.com/ClickHouse/ClickHouse/pull/10335) ([Ilya Yatsishin](https://github.com/qoega))
* Allow to use `clang-10` in CI. It ensures that [#10238](https://github.com/ClickHouse/ClickHouse/issues/10238) is fixed. [#10384](https://github.com/ClickHouse/ClickHouse/pull/10384) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Update OpenSSL to upstream master. Fixed the issue when TLS connections may fail with the message `OpenSSL SSL_read: error:14094438:SSL routines:ssl3_read_bytes:tlsv1 alert internal error` and `SSL Exception: error:2400006E:random number generator::error retrieving entropy`. The issue was present in version 20.1. [#8956](https://github.com/ClickHouse/ClickHouse/pull/8956) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix clang-10 build. https://github.com/ClickHouse/ClickHouse/issues/10238 [#10370](https://github.com/ClickHouse/ClickHouse/pull/10370) ([Amos Bird](https://github.com/amosbird))
* Add performance test for [Parallel INSERT for materialized view](https://github.com/ClickHouse/ClickHouse/pull/10052). [#10345](https://github.com/ClickHouse/ClickHouse/pull/10345) ([vxider](https://github.com/Vxider))
* Fix flaky test `test_settings_constraints_distributed.test_insert_clamps_settings`. [#10346](https://github.com/ClickHouse/ClickHouse/pull/10346) ([Vitaly Baranov](https://github.com/vitlibar))
* Add util to test results upload in CI ClickHouse [#10330](https://github.com/ClickHouse/ClickHouse/pull/10330) ([Ilya Yatsishin](https://github.com/qoega))
* Convert test results to JSONEachRow format in junit_to_html tool [#10323](https://github.com/ClickHouse/ClickHouse/pull/10323) ([Ilya Yatsishin](https://github.com/qoega))
* Update cctz. [#10215](https://github.com/ClickHouse/ClickHouse/pull/10215) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Allow to create HTML report from the purest JUnit XML report. [#10247](https://github.com/ClickHouse/ClickHouse/pull/10247) ([Ilya Yatsishin](https://github.com/qoega))
* Update the check for minimal compiler version. Fix the root cause of the issue [#10250](https://github.com/ClickHouse/ClickHouse/issues/10250) [#10256](https://github.com/ClickHouse/ClickHouse/pull/10256) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Initial support for live view tables over distributed [#10179](https://github.com/ClickHouse/ClickHouse/pull/10179) ([vzakaznikov](https://github.com/vzakaznikov))
* Fix (false) MSan report in MergeTreeIndexFullText. The issue first appeared in [#9968](https://github.com/ClickHouse/ClickHouse/issues/9968). [#10801](https://github.com/ClickHouse/ClickHouse/pull/10801) ([alexey-milovidov](https://github.com/alexey-milovidov))
* clickhouse-docker-util [#10151](https://github.com/ClickHouse/ClickHouse/pull/10151) ([filimonov](https://github.com/filimonov))
* Update pdqsort to recent version [#10171](https://github.com/ClickHouse/ClickHouse/pull/10171) ([Ivan](https://github.com/abyss7))
* Update libdivide to v3.0 [#10169](https://github.com/ClickHouse/ClickHouse/pull/10169) ([Ivan](https://github.com/abyss7))
* Add check with enabled polymorphic parts. [#10086](https://github.com/ClickHouse/ClickHouse/pull/10086) ([Anton Popov](https://github.com/CurtizJ))
* Add cross-compile build for FreeBSD. This fixes [#9465](https://github.com/ClickHouse/ClickHouse/issues/9465) [#9643](https://github.com/ClickHouse/ClickHouse/pull/9643) ([Ivan](https://github.com/abyss7))
* Add performance test for [#6924](https://github.com/ClickHouse/ClickHouse/issues/6924) [#6980](https://github.com/ClickHouse/ClickHouse/pull/6980) ([filimonov](https://github.com/filimonov))
* Add support of `/dev/null` in the `File` engine for better performance testing [#8455](https://github.com/ClickHouse/ClickHouse/pull/8455) ([Amos Bird](https://github.com/amosbird))
* Move all folders inside /dbms one level up [#9974](https://github.com/ClickHouse/ClickHouse/pull/9974) ([Ivan](https://github.com/abyss7))
* Add a test that checks that read from MergeTree with single thread is performed in order. Addition to [#9670](https://github.com/ClickHouse/ClickHouse/issues/9670) [#9762](https://github.com/ClickHouse/ClickHouse/pull/9762) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix the `00964_live_view_watch_events_heartbeat.py` test to avoid race condition. [#9944](https://github.com/ClickHouse/ClickHouse/pull/9944) ([vzakaznikov](https://github.com/vzakaznikov))
* Fix integration test `test_settings_constraints` [#9962](https://github.com/ClickHouse/ClickHouse/pull/9962) ([Vitaly Baranov](https://github.com/vitlibar))
* Every function in its own file, part 12. [#9922](https://github.com/ClickHouse/ClickHouse/pull/9922) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added performance test for the case of extremely slow analysis of array of tuples. [#9872](https://github.com/ClickHouse/ClickHouse/pull/9872) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Update zstd to 1.4.4. It has some minor improvements in performance and compression ratio. If you run replicas with different versions of ClickHouse you may see reasonable error messages `Data after merge is not byte-identical to data on another replicas.` with explanation. These messages are Ok and you should not worry. [#10663](https://github.com/ClickHouse/ClickHouse/pull/10663) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix TSan report in `system.stack_trace`. [#9832](https://github.com/ClickHouse/ClickHouse/pull/9832) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Removed dependency on `clock_getres`. [#9833](https://github.com/ClickHouse/ClickHouse/pull/9833) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added identifier names check with clang-tidy. [#9799](https://github.com/ClickHouse/ClickHouse/pull/9799) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Update "builder" docker image. This image is not used in CI but is useful for developers. [#9809](https://github.com/ClickHouse/ClickHouse/pull/9809) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Remove old `performance-test` tool that is no longer used in CI. `clickhouse-performance-test` is great but now we are using way superior tool that is doing comparison testing with sophisticated statistical formulas to achieve confident results regardless to various changes in environment. [#9796](https://github.com/ClickHouse/ClickHouse/pull/9796) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added most of clang-static-analyzer checks. [#9765](https://github.com/ClickHouse/ClickHouse/pull/9765) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Update Poco to 1.9.3 in preparation for MongoDB URI support. [#6892](https://github.com/ClickHouse/ClickHouse/pull/6892) ([Alexander Kuzmenkov](https://github.com/akuzm))
* Fix build with `-DUSE_STATIC_LIBRARIES=0 -DENABLE_JEMALLOC=0` [#9651](https://github.com/ClickHouse/ClickHouse/pull/9651) ([Artem Zuikov](https://github.com/4ertus2))
* For change log script, if merge commit was cherry-picked to release branch, take PR name from commit description. [#9708](https://github.com/ClickHouse/ClickHouse/pull/9708) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Support `vX.X-conflicts` tag in backport script. [#9705](https://github.com/ClickHouse/ClickHouse/pull/9705) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix `auto-label` for backporting script. [#9685](https://github.com/ClickHouse/ClickHouse/pull/9685) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Use libc++ in Darwin cross-build to make it consistent with native build. [#9665](https://github.com/ClickHouse/ClickHouse/pull/9665) ([Hui Wang](https://github.com/huiwang))
* Fix flacky test `01017_uniqCombined_memory_usage`. Continuation of [#7236](https://github.com/ClickHouse/ClickHouse/issues/7236). [#9667](https://github.com/ClickHouse/ClickHouse/pull/9667) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix build for native MacOS Clang compiler [#9649](https://github.com/ClickHouse/ClickHouse/pull/9649) ([Ivan](https://github.com/abyss7))
* Allow to add various glitches around `pthread_mutex_lock`, `pthread_mutex_unlock` functions. [#9635](https://github.com/ClickHouse/ClickHouse/pull/9635) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add support for `clang-tidy` in `packager` script. [#9625](https://github.com/ClickHouse/ClickHouse/pull/9625) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add ability to use unbundled msgpack. [#10168](https://github.com/ClickHouse/ClickHouse/pull/10168) ([Azat Khuzhin](https://github.com/azat))


## ClickHouse release v20.3

### ClickHouse release v20.3.10.75-lts 2020-05-23

#### Bug Fix

* Removed logging from mutation finalization task if nothing was finalized. [#11109](https://github.com/ClickHouse/ClickHouse/pull/11109) ([alesapin](https://github.com/alesapin)).
* Fixed `parseDateTime64BestEffort` argument resolution bugs. [#11038](https://github.com/ClickHouse/ClickHouse/pull/11038) ([Vasily Nemkov](https://github.com/Enmk)).
* Fixed incorrect raw data size in method `getRawData()`. [#10964](https://github.com/ClickHouse/ClickHouse/pull/10964) ([Igr](https://github.com/ObjatieGroba)).
* Fixed incompatibility of two-level aggregation between versions 20.1 and earlier. This incompatibility happens when different versions of ClickHouse are used on initiator node and remote nodes and the size of `GROUP BY` result is large and aggregation is performed by a single `String` field. It leads to several unmerged rows for a single key in result. [#10952](https://github.com/ClickHouse/ClickHouse/pull/10952) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fixed backward compatibility with tuples in `Distributed` tables. [#10889](https://github.com/ClickHouse/ClickHouse/pull/10889) ([Anton Popov](https://github.com/CurtizJ)).
* Fixed `SIGSEGV` in `StringHashTable` if such a key does not exist. [#10870](https://github.com/ClickHouse/ClickHouse/pull/10870) ([Azat Khuzhin](https://github.com/azat)).
* Fixed bug in `ReplicatedMergeTree` which might cause some `ALTER` on `OPTIMIZE` query to hang waiting for some replica after it become inactive. [#10849](https://github.com/ClickHouse/ClickHouse/pull/10849) ([tavplubix](https://github.com/tavplubix)).
* Fixed columns order after `Block::sortColumns()`. [#10826](https://github.com/ClickHouse/ClickHouse/pull/10826) ([Azat Khuzhin](https://github.com/azat)).
* Fixed the issue with `ODBC` bridge when no quoting of identifiers is requested. Fixes [#7984] (https://github.com/ClickHouse/ClickHouse/issues/7984). [#10821](https://github.com/ClickHouse/ClickHouse/pull/10821) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fixed `UBSan` and `MSan` report in `DateLUT`. [#10798](https://github.com/ClickHouse/ClickHouse/pull/10798) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fixed incorrect type conversion in key conditions. Fixes [#6287](https://github.com/ClickHouse/ClickHouse/issues/6287). [#10791](https://github.com/ClickHouse/ClickHouse/pull/10791) ([Andrew Onyshchuk](https://github.com/oandrew))
* Fixed `parallel_view_processing` behavior. Now all insertions into `MATERIALIZED VIEW` without exception should be finished if exception happened. Fixes [#10241](https://github.com/ClickHouse/ClickHouse/issues/10241). [#10757](https://github.com/ClickHouse/ClickHouse/pull/10757) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed combinator -`OrNull` and `-OrDefault` when combined with `-State`. [#10741](https://github.com/ClickHouse/ClickHouse/pull/10741) ([hcz](https://github.com/hczhcz)).
* Fixed crash in `generateRandom` with nested types. Fixes [#10583](https://github.com/ClickHouse/ClickHouse/issues/10583). [#10734](https://github.com/ClickHouse/ClickHouse/pull/10734) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed data corruption for `LowCardinality(FixedString)` key column in `SummingMergeTree` which could have happened after merge. Fixes [#10489](https://github.com/ClickHouse/ClickHouse/issues/10489). [#10721](https://github.com/ClickHouse/ClickHouse/pull/10721) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed possible buffer overflow in function `h3EdgeAngle`. [#10711](https://github.com/ClickHouse/ClickHouse/pull/10711) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fixed disappearing totals. Totals could have being filtered if query had had join or subquery with external where condition. Fixes [#10674](https://github.com/ClickHouse/ClickHouse/issues/10674). [#10698](https://github.com/ClickHouse/ClickHouse/pull/10698) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed multiple usages of `IN` operator with the identical set in one query. [#10686](https://github.com/ClickHouse/ClickHouse/pull/10686) ([Anton Popov](https://github.com/CurtizJ)).
* Fixed bug, which causes http requests stuck on client close when `readonly=2` and `cancel_http_readonly_queries_on_client_close=1`. Fixes [#7939](https://github.com/ClickHouse/ClickHouse/issues/7939), [#7019](https://github.com/ClickHouse/ClickHouse/issues/7019), [#7736](https://github.com/ClickHouse/ClickHouse/issues/7736), [#7091](https://github.com/ClickHouse/ClickHouse/issues/7091). [#10684](https://github.com/ClickHouse/ClickHouse/pull/10684) ([tavplubix](https://github.com/tavplubix)).
* Fixed order of parameters in `AggregateTransform` constructor. [#10667](https://github.com/ClickHouse/ClickHouse/pull/10667) ([palasonic1](https://github.com/palasonic1)).
* Fixed the lack of parallel execution of remote queries with `distributed_aggregation_memory_efficient` enabled. Fixes [#10655](https://github.com/ClickHouse/ClickHouse/issues/10655). [#10664](https://github.com/ClickHouse/ClickHouse/pull/10664) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed possible incorrect number of rows for queries with `LIMIT`. Fixes [#10566](https://github.com/ClickHouse/ClickHouse/issues/10566), [#10709](https://github.com/ClickHouse/ClickHouse/issues/10709). [#10660](https://github.com/ClickHouse/ClickHouse/pull/10660) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed a bug which locks concurrent alters when table has a lot of parts. [#10659](https://github.com/ClickHouse/ClickHouse/pull/10659) ([alesapin](https://github.com/alesapin)).
* Fixed a bug when on `SYSTEM DROP DNS CACHE` query also drop caches, which are used to check if user is allowed to connect from some IP addresses. [#10608](https://github.com/ClickHouse/ClickHouse/pull/10608) ([tavplubix](https://github.com/tavplubix)).
* Fixed incorrect scalar results inside inner query of `MATERIALIZED VIEW` in case if this query contained dependent table. [#10603](https://github.com/ClickHouse/ClickHouse/pull/10603) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed `SELECT` of column `ALIAS` which default expression type different from column type. [#10563](https://github.com/ClickHouse/ClickHouse/pull/10563) ([Azat Khuzhin](https://github.com/azat)).
* Implemented comparison between DateTime64 and String values. [#10560](https://github.com/ClickHouse/ClickHouse/pull/10560) ([Vasily Nemkov](https://github.com/Enmk)).
* Fixed index corruption, which may accur in some cases after merge compact parts into another compact part. [#10531](https://github.com/ClickHouse/ClickHouse/pull/10531) ([Anton Popov](https://github.com/CurtizJ)).
* Fixed the situation, when mutation finished all parts, but hung up in `is_done=0`. [#10526](https://github.com/ClickHouse/ClickHouse/pull/10526) ([alesapin](https://github.com/alesapin)).
* Fixed overflow at beginning of unix epoch for timezones with fractional offset from `UTC`. This fixes [#9335](https://github.com/ClickHouse/ClickHouse/issues/9335). [#10513](https://github.com/ClickHouse/ClickHouse/pull/10513) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fixed improper shutdown of `Distributed` storage. [#10491](https://github.com/ClickHouse/ClickHouse/pull/10491) ([Azat Khuzhin](https://github.com/azat)).
* Fixed numeric overflow in `simpleLinearRegression` over large integers. [#10474](https://github.com/ClickHouse/ClickHouse/pull/10474) ([hcz](https://github.com/hczhcz)).


#### Build/Testing/Packaging Improvement

* Fix UBSan report in LZ4 library. [#10631](https://github.com/ClickHouse/ClickHouse/pull/10631) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fix clang-10 build. https://github.com/ClickHouse/ClickHouse/issues/10238. [#10370](https://github.com/ClickHouse/ClickHouse/pull/10370) ([Amos Bird](https://github.com/amosbird)).
* Added failing tests about `max_rows_to_sort` setting. [#10268](https://github.com/ClickHouse/ClickHouse/pull/10268) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Added some improvements in printing diagnostic info in input formats. Fixes [#10204](https://github.com/ClickHouse/ClickHouse/issues/10204). [#10418](https://github.com/ClickHouse/ClickHouse/pull/10418) ([tavplubix](https://github.com/tavplubix)).
* Added CA certificates to clickhouse-server docker image. [#10476](https://github.com/ClickHouse/ClickHouse/pull/10476) ([filimonov](https://github.com/filimonov)).

#### Bug fix

* #10551. [#10569](https://github.com/ClickHouse/ClickHouse/pull/10569) ([Winter Zhang](https://github.com/zhang2014)).


### ClickHouse release v20.3.8.53, 2020-04-23

#### Bug Fix
* Fixed wrong behaviour of datetime functions for timezones that has altered between positive and negative offsets from UTC (e.g. Pacific/Kiritimati). This fixes [#7202](https://github.com/ClickHouse/ClickHouse/issues/7202) [#10369](https://github.com/ClickHouse/ClickHouse/pull/10369) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix possible segfault with `distributed_group_by_no_merge` enabled (introduced in 20.3.7.46 by [#10131](https://github.com/ClickHouse/ClickHouse/issues/10131)). [#10399](https://github.com/ClickHouse/ClickHouse/pull/10399) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix wrong flattening of `Array(Tuple(...))` data types. This fixes [#10259](https://github.com/ClickHouse/ClickHouse/issues/10259) [#10390](https://github.com/ClickHouse/ClickHouse/pull/10390) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Drop disks reservation in Aggregator. This fixes bug in disk space reservation, which may cause big external aggregation to fail even if it could be completed successfully [#10375](https://github.com/ClickHouse/ClickHouse/pull/10375) ([Azat Khuzhin](https://github.com/azat))
* Fixed `DROP` vs `OPTIMIZE` race in `ReplicatedMergeTree`. `DROP` could left some garbage in replica path in ZooKeeper if there was concurrent `OPTIMIZE` query. [#10312](https://github.com/ClickHouse/ClickHouse/pull/10312) ([tavplubix](https://github.com/tavplubix))
* Fix bug when server cannot attach table after column default was altered. [#10441](https://github.com/ClickHouse/ClickHouse/pull/10441) ([alesapin](https://github.com/alesapin))
* Do not remove metadata directory when attach database fails before loading tables.  [#10442](https://github.com/ClickHouse/ClickHouse/pull/10442) ([Winter Zhang](https://github.com/zhang2014))
* Fixed several bugs when some data was inserted with quorum, then deleted somehow (DROP PARTITION, TTL) and this leaded to the stuck of INSERTs or false-positive exceptions in SELECTs. This fixes [#9946](https://github.com/ClickHouse/ClickHouse/issues/9946) [#10188](https://github.com/ClickHouse/ClickHouse/pull/10188) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Fix possible `Pipeline stuck` error in `ConcatProcessor` which could have happened in remote query. [#10381](https://github.com/ClickHouse/ClickHouse/pull/10381) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fixed wrong behavior in HashTable that caused compilation error when trying to read HashMap from buffer. [#10386](https://github.com/ClickHouse/ClickHouse/pull/10386) ([palasonic1](https://github.com/palasonic1))
* Allow to use `count(*)` with multiple JOINs. Fixes [#9853](https://github.com/ClickHouse/ClickHouse/issues/9853) [#10291](https://github.com/ClickHouse/ClickHouse/pull/10291) ([Artem Zuikov](https://github.com/4ertus2))
* Prefer `fallback_to_stale_replicas` over `skip_unavailable_shards`, otherwise when both settings specified and there are no up-to-date replicas the query will fail (patch from @alex-zaitsev). Fixes: [#2564](https://github.com/ClickHouse/ClickHouse/issues/2564). [#10422](https://github.com/ClickHouse/ClickHouse/pull/10422) ([Azat Khuzhin](https://github.com/azat))
* Fix the issue when a query with ARRAY JOIN, ORDER BY and LIMIT may return incomplete result. This fixes [#10226](https://github.com/ClickHouse/ClickHouse/issues/10226). Author: [Vadim Plakhtinskiy](https://github.com/VadimPlh). [#10427](https://github.com/ClickHouse/ClickHouse/pull/10427) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Check the number and type of arguments when creating BloomFilter index [#9623](https://github.com/ClickHouse/ClickHouse/issues/9623) [#10431](https://github.com/ClickHouse/ClickHouse/pull/10431) ([Winter Zhang](https://github.com/zhang2014))

#### Performance Improvement
* Improved performance of queries with explicitly defined sets at right side of `IN` operator and tuples in the left side. This fixes performance regression in version 20.3. [#9740](https://github.com/ClickHouse/ClickHouse/pull/9740), [#10385](https://github.com/ClickHouse/ClickHouse/pull/10385) ([Anton Popov](https://github.com/CurtizJ))

### ClickHouse release v20.3.7.46, 2020-04-17

#### Bug Fix

* Fix `Logical error: CROSS JOIN has expressions` error for queries with comma and names joins mix. [#10311](https://github.com/ClickHouse/ClickHouse/pull/10311) ([Artem Zuikov](https://github.com/4ertus2)).
* Fix queries with `max_bytes_before_external_group_by`. [#10302](https://github.com/ClickHouse/ClickHouse/pull/10302) ([Artem Zuikov](https://github.com/4ertus2)).
* Fix move-to-prewhere optimization in presense of arrayJoin functions (in certain cases). This fixes [#10092](https://github.com/ClickHouse/ClickHouse/issues/10092). [#10195](https://github.com/ClickHouse/ClickHouse/pull/10195) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Add the ability to relax the restriction on non-deterministic functions usage in mutations with `allow_nondeterministic_mutations` setting. [#10186](https://github.com/ClickHouse/ClickHouse/pull/10186) ([filimonov](https://github.com/filimonov)).

### ClickHouse release v20.3.6.40, 2020-04-16

#### New Feature

* Added function `isConstant`. This function checks whether its argument is constant expression and returns 1 or 0. It is intended for development, debugging and demonstration purposes. [#10198](https://github.com/ClickHouse/ClickHouse/pull/10198) ([alexey-milovidov](https://github.com/alexey-milovidov)).

#### Bug Fix

* Fix error `Pipeline stuck` with `max_rows_to_group_by` and `group_by_overflow_mode = 'break'`. [#10279](https://github.com/ClickHouse/ClickHouse/pull/10279) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix rare possible exception `Cannot drain connections: cancel first`. [#10239](https://github.com/ClickHouse/ClickHouse/pull/10239) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed bug where ClickHouse would throw "Unknown function lambda." error message when user tries to run ALTER UPDATE/DELETE on tables with ENGINE = Replicated*. Check for nondeterministic functions now handles lambda expressions correctly. [#10237](https://github.com/ClickHouse/ClickHouse/pull/10237) ([Alexander Kazakov](https://github.com/Akazz)).
* Fixed "generateRandom" function for Date type. This fixes [#9973](https://github.com/ClickHouse/ClickHouse/issues/9973). Fix an edge case when dates with year 2106 are inserted to MergeTree tables with old-style partitioning but partitions are named with year 1970. [#10218](https://github.com/ClickHouse/ClickHouse/pull/10218) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Convert types if the table definition of a View does not correspond to the SELECT query. This fixes [#10180](https://github.com/ClickHouse/ClickHouse/issues/10180) and [#10022](https://github.com/ClickHouse/ClickHouse/issues/10022). [#10217](https://github.com/ClickHouse/ClickHouse/pull/10217) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fix `parseDateTimeBestEffort` for strings in RFC-2822 when day of week is Tuesday or Thursday. This fixes [#10082](https://github.com/ClickHouse/ClickHouse/issues/10082). [#10214](https://github.com/ClickHouse/ClickHouse/pull/10214) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fix column names of constants inside JOIN that may clash with names of constants outside of JOIN. [#10207](https://github.com/ClickHouse/ClickHouse/pull/10207) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fix possible inifinite query execution when the query actually should stop on LIMIT, while reading from infinite source like `system.numbers` or `system.zeros`. [#10206](https://github.com/ClickHouse/ClickHouse/pull/10206) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix using the current database for access checking when the database isn't specified. [#10192](https://github.com/ClickHouse/ClickHouse/pull/10192) ([Vitaly Baranov](https://github.com/vitlibar)).
* Convert blocks if structure does not match on INSERT into Distributed(). [#10135](https://github.com/ClickHouse/ClickHouse/pull/10135) ([Azat Khuzhin](https://github.com/azat)).
* Fix possible incorrect result for extremes in processors pipeline. [#10131](https://github.com/ClickHouse/ClickHouse/pull/10131) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix some kinds of alters with compact parts. [#10130](https://github.com/ClickHouse/ClickHouse/pull/10130) ([Anton Popov](https://github.com/CurtizJ)).
* Fix incorrect `index_granularity_bytes` check while creating new replica. Fixes [#10098](https://github.com/ClickHouse/ClickHouse/issues/10098). [#10121](https://github.com/ClickHouse/ClickHouse/pull/10121) ([alesapin](https://github.com/alesapin)).
* Fix SIGSEGV on INSERT into Distributed table when its structure differs from the underlying tables. [#10105](https://github.com/ClickHouse/ClickHouse/pull/10105) ([Azat Khuzhin](https://github.com/azat)).
* Fix possible rows loss for queries with `JOIN` and `UNION ALL`. Fixes [#9826](https://github.com/ClickHouse/ClickHouse/issues/9826), [#10113](https://github.com/ClickHouse/ClickHouse/issues/10113). [#10099](https://github.com/ClickHouse/ClickHouse/pull/10099) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed replicated tables startup when updating from an old ClickHouse version where `/table/replicas/replica_name/metadata` node doesn't exist. Fixes [#10037](https://github.com/ClickHouse/ClickHouse/issues/10037). [#10095](https://github.com/ClickHouse/ClickHouse/pull/10095) ([alesapin](https://github.com/alesapin)).
* Add some arguments check and support identifier arguments for MySQL Database Engine. [#10077](https://github.com/ClickHouse/ClickHouse/pull/10077) ([Winter Zhang](https://github.com/zhang2014)).
* Fix bug in clickhouse dictionary source from localhost clickhouse server. The bug may lead to memory corruption if types in dictionary and source are not compatible. [#10071](https://github.com/ClickHouse/ClickHouse/pull/10071) ([alesapin](https://github.com/alesapin)).
* Fix bug in `CHECK TABLE` query when table contain skip indices. [#10068](https://github.com/ClickHouse/ClickHouse/pull/10068) ([alesapin](https://github.com/alesapin)).
* Fix error `Cannot clone block with columns because block has 0 columns ... While executing GroupingAggregatedTransform`. It happened when setting `distributed_aggregation_memory_efficient` was enabled, and distributed query read aggregating data with different level from different shards (mixed single and two level aggregation). [#10063](https://github.com/ClickHouse/ClickHouse/pull/10063) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix a segmentation fault that could occur in GROUP BY over string keys containing trailing zero bytes ([#8636](https://github.com/ClickHouse/ClickHouse/issues/8636), [#8925](https://github.com/ClickHouse/ClickHouse/issues/8925)). [#10025](https://github.com/ClickHouse/ClickHouse/pull/10025) ([Alexander Kuzmenkov](https://github.com/akuzm)).
* Fix the number of threads used for remote query execution (performance regression, since 20.3). This happened when query from `Distributed` table was executed simultaneously on local and remote shards. Fixes [#9965](https://github.com/ClickHouse/ClickHouse/issues/9965). [#9971](https://github.com/ClickHouse/ClickHouse/pull/9971) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix bug in which the necessary tables weren't retrieved at one of the processing stages of queries to some databases. Fixes [#9699](https://github.com/ClickHouse/ClickHouse/issues/9699). [#9949](https://github.com/ClickHouse/ClickHouse/pull/9949) ([achulkov2](https://github.com/achulkov2)).
* Fix 'Not found column in block' error when `JOIN` appears with `TOTALS`. Fixes [#9839](https://github.com/ClickHouse/ClickHouse/issues/9839). [#9939](https://github.com/ClickHouse/ClickHouse/pull/9939) ([Artem Zuikov](https://github.com/4ertus2)).
* Fix a bug with `ON CLUSTER` DDL queries freezing on server startup. [#9927](https://github.com/ClickHouse/ClickHouse/pull/9927) ([Gagan Arneja](https://github.com/garneja)).
* Fix parsing multiple hosts set in the CREATE USER command, e.g. `CREATE USER user6 HOST NAME REGEXP 'lo.?*host', NAME REGEXP 'lo*host'`. [#9924](https://github.com/ClickHouse/ClickHouse/pull/9924) ([Vitaly Baranov](https://github.com/vitlibar)).
* Fix `TRUNCATE` for Join table engine ([#9917](https://github.com/ClickHouse/ClickHouse/issues/9917)). [#9920](https://github.com/ClickHouse/ClickHouse/pull/9920) ([Amos Bird](https://github.com/amosbird)).
* Fix "scalar doesn't exist" error in ALTERs ([#9878](https://github.com/ClickHouse/ClickHouse/issues/9878)). [#9904](https://github.com/ClickHouse/ClickHouse/pull/9904) ([Amos Bird](https://github.com/amosbird)).
* Fix race condition between drop and optimize in `ReplicatedMergeTree`. [#9901](https://github.com/ClickHouse/ClickHouse/pull/9901) ([alesapin](https://github.com/alesapin)).
* Fix error with qualified names in `distributed_product_mode='local'`. Fixes [#4756](https://github.com/ClickHouse/ClickHouse/issues/4756). [#9891](https://github.com/ClickHouse/ClickHouse/pull/9891) ([Artem Zuikov](https://github.com/4ertus2)).
* Fix calculating grants for introspection functions from the setting 'allow_introspection_functions'. [#9840](https://github.com/ClickHouse/ClickHouse/pull/9840) ([Vitaly Baranov](https://github.com/vitlibar)).

#### Build/Testing/Packaging Improvement

* Fix integration test `test_settings_constraints`. [#9962](https://github.com/ClickHouse/ClickHouse/pull/9962) ([Vitaly Baranov](https://github.com/vitlibar)).
* Removed dependency on `clock_getres`. [#9833](https://github.com/ClickHouse/ClickHouse/pull/9833) ([alexey-milovidov](https://github.com/alexey-milovidov)).


### ClickHouse release v20.3.5.21, 2020-03-27

#### Bug Fix

* Fix 'Different expressions with the same alias' error when query has PREWHERE and WHERE on distributed table and `SET distributed_product_mode = 'local'`. [#9871](https://github.com/ClickHouse/ClickHouse/pull/9871) ([Artem Zuikov](https://github.com/4ertus2)).
* Fix mutations excessive memory consumption for tables with a composite primary key. This fixes [#9850](https://github.com/ClickHouse/ClickHouse/issues/9850). [#9860](https://github.com/ClickHouse/ClickHouse/pull/9860) ([alesapin](https://github.com/alesapin)).
* For INSERT queries shard now clamps the settings got from the initiator to the shard's constaints instead of throwing an exception. This fix allows to send INSERT queries to a shard with another constraints. This change improves fix [#9447](https://github.com/ClickHouse/ClickHouse/issues/9447). [#9852](https://github.com/ClickHouse/ClickHouse/pull/9852) ([Vitaly Baranov](https://github.com/vitlibar)).
* Fix 'COMMA to CROSS JOIN rewriter is not enabled or cannot rewrite query' error in case of subqueries with COMMA JOIN out of tables lists (i.e. in WHERE). Fixes [#9782](https://github.com/ClickHouse/ClickHouse/issues/9782). [#9830](https://github.com/ClickHouse/ClickHouse/pull/9830) ([Artem Zuikov](https://github.com/4ertus2)).
* Fix possible exception `Got 0 in totals chunk, expected 1` on client. It happened for queries with `JOIN` in case if right joined table had zero rows. Example: `select * from system.one t1 join system.one t2 on t1.dummy = t2.dummy limit 0 FORMAT TabSeparated;`. Fixes [#9777](https://github.com/ClickHouse/ClickHouse/issues/9777). [#9823](https://github.com/ClickHouse/ClickHouse/pull/9823) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix SIGSEGV with optimize_skip_unused_shards when type cannot be converted. [#9804](https://github.com/ClickHouse/ClickHouse/pull/9804) ([Azat Khuzhin](https://github.com/azat)).
* Fix broken `ALTER TABLE DELETE COLUMN` query for compact parts. [#9779](https://github.com/ClickHouse/ClickHouse/pull/9779) ([alesapin](https://github.com/alesapin)).
* Fix max_distributed_connections (w/ and w/o Processors). [#9673](https://github.com/ClickHouse/ClickHouse/pull/9673) ([Azat Khuzhin](https://github.com/azat)).
* Fixed a few cases when timezone of the function argument wasn't used properly. [#9574](https://github.com/ClickHouse/ClickHouse/pull/9574) ([Vasily Nemkov](https://github.com/Enmk)).

#### Improvement

* Remove order by stage from mutations because we read from a single ordered part in a single thread. Also add check that the order of rows in mutation is ordered in sorting key order and this order is not violated. [#9886](https://github.com/ClickHouse/ClickHouse/pull/9886) ([alesapin](https://github.com/alesapin)).


### ClickHouse release v20.3.4.10, 2020-03-20

#### Bug Fix
* This release also contains all bug fixes from 20.1.8.41
* Fix missing `rows_before_limit_at_least` for queries over http (with processors pipeline). This fixes [#9730](https://github.com/ClickHouse/ClickHouse/issues/9730). [#9757](https://github.com/ClickHouse/ClickHouse/pull/9757) ([Nikolai Kochetov](https://github.com/KochetovNicolai))


### ClickHouse release v20.3.3.6, 2020-03-17

#### Bug Fix
* This release also contains all bug fixes from 20.1.7.38
* Fix bug in a replication that doesn't allow replication to work if the user has executed mutations on the previous version. This fixes [#9645](https://github.com/ClickHouse/ClickHouse/issues/9645). [#9652](https://github.com/ClickHouse/ClickHouse/pull/9652) ([alesapin](https://github.com/alesapin)). It makes version 20.3 backward compatible again.
* Add setting `use_compact_format_in_distributed_parts_names` which allows to write files for `INSERT` queries into `Distributed` table with more compact format. This fixes [#9647](https://github.com/ClickHouse/ClickHouse/issues/9647). [#9653](https://github.com/ClickHouse/ClickHouse/pull/9653) ([alesapin](https://github.com/alesapin)). It makes version 20.3 backward compatible again.

### ClickHouse release v20.3.2.1, 2020-03-12

#### Backward Incompatible Change

* Fixed the issue `file name too long` when sending data for `Distributed` tables for a large number of replicas. Fixed the issue that replica credentials were exposed in the server log. The format of directory name on disk was changed to `[shard{shard_index}[_replica{replica_index}]]`. [#8911](https://github.com/ClickHouse/ClickHouse/pull/8911) ([Mikhail Korotov](https://github.com/millb)) After you upgrade to the new version, you will not be able to downgrade without manual intervention, because old server version does not recognize the new directory format. If you want to downgrade, you have to manually rename the corresponding directories to the old format. This change is relevant only if you have used asynchronous `INSERT`s to `Distributed` tables. In the version 20.3.3 we will introduce a setting that will allow you to enable the new format gradually.
* Changed the format of replication log entries for mutation commands. You have to wait for old mutations to process before installing the new version.
* Implement simple memory profiler that dumps stacktraces to `system.trace_log` every N bytes over soft allocation limit [#8765](https://github.com/ClickHouse/ClickHouse/pull/8765) ([Ivan](https://github.com/abyss7)) [#9472](https://github.com/ClickHouse/ClickHouse/pull/9472) ([alexey-milovidov](https://github.com/alexey-milovidov)) The column of `system.trace_log` was renamed from `timer_type` to `trace_type`. This will require changes in third-party performance analysis and flamegraph processing tools.
* Use OS thread id everywhere instead of internal thread number. This fixes [#7477](https://github.com/ClickHouse/ClickHouse/issues/7477) Old `clickhouse-client` cannot receive logs that are send from the server when the setting `send_logs_level` is enabled, because the names and types of the structured log messages were changed. On the other hand, different server versions can send logs with different types to each other. When you don't use the `send_logs_level` setting, you should not care. [#8954](https://github.com/ClickHouse/ClickHouse/pull/8954) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Remove `indexHint` function [#9542](https://github.com/ClickHouse/ClickHouse/pull/9542) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Remove `findClusterIndex`, `findClusterValue` functions. This fixes [#8641](https://github.com/ClickHouse/ClickHouse/issues/8641). If you were using these functions, send an email to `clickhouse-feedback@yandex-team.com` [#9543](https://github.com/ClickHouse/ClickHouse/pull/9543) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Now it's not allowed to create columns or add columns with `SELECT` subquery as default expression. [#9481](https://github.com/ClickHouse/ClickHouse/pull/9481) ([alesapin](https://github.com/alesapin))
* Require aliases for subqueries in JOIN. [#9274](https://github.com/ClickHouse/ClickHouse/pull/9274) ([Artem Zuikov](https://github.com/4ertus2))
* Improved `ALTER MODIFY/ADD` queries logic. Now you cannot `ADD` column without type, `MODIFY` default expression doesn't change type of column and `MODIFY` type doesn't loose default expression value. Fixes [#8669](https://github.com/ClickHouse/ClickHouse/issues/8669). [#9227](https://github.com/ClickHouse/ClickHouse/pull/9227) ([alesapin](https://github.com/alesapin))
* Require server to be restarted to apply the changes in logging configuration. This is a temporary workaround to avoid the bug where the server logs to a deleted log file (see [#8696](https://github.com/ClickHouse/ClickHouse/issues/8696)). [#8707](https://github.com/ClickHouse/ClickHouse/pull/8707) ([Alexander Kuzmenkov](https://github.com/akuzm))
* The setting `experimental_use_processors` is enabled by default. This setting enables usage of the new query pipeline. This is internal refactoring and we expect no visible changes. If you will see any issues, set it to back zero. [#8768](https://github.com/ClickHouse/ClickHouse/pull/8768) ([alexey-milovidov](https://github.com/alexey-milovidov))

#### New Feature
* Add `Avro` and `AvroConfluent` input/output formats [#8571](https://github.com/ClickHouse/ClickHouse/pull/8571) ([Andrew Onyshchuk](https://github.com/oandrew)) [#8957](https://github.com/ClickHouse/ClickHouse/pull/8957) ([Andrew Onyshchuk](https://github.com/oandrew)) [#8717](https://github.com/ClickHouse/ClickHouse/pull/8717) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Multi-threaded and non-blocking updates of expired keys in `cache` dictionaries (with optional permission to read old ones). [#8303](https://github.com/ClickHouse/ClickHouse/pull/8303) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Add query `ALTER ... MATERIALIZE TTL`. It runs mutation that forces to remove expired data by TTL and recalculates meta-information about TTL in all parts. [#8775](https://github.com/ClickHouse/ClickHouse/pull/8775) ([Anton Popov](https://github.com/CurtizJ))
* Switch from HashJoin to MergeJoin (on disk) if needed [#9082](https://github.com/ClickHouse/ClickHouse/pull/9082) ([Artem Zuikov](https://github.com/4ertus2))
* Added `MOVE PARTITION` command for `ALTER TABLE` [#4729](https://github.com/ClickHouse/ClickHouse/issues/4729) [#6168](https://github.com/ClickHouse/ClickHouse/pull/6168) ([Guillaume Tassery](https://github.com/YiuRULE))
* Reloading storage configuration from configuration file on the fly. [#8594](https://github.com/ClickHouse/ClickHouse/pull/8594) ([Vladimir Chebotarev](https://github.com/excitoon))
* Allowed to change `storage_policy` to not less rich one. [#8107](https://github.com/ClickHouse/ClickHouse/pull/8107) ([Vladimir Chebotarev](https://github.com/excitoon))
* Added support for globs/wildcards for S3 storage and table function. [#8851](https://github.com/ClickHouse/ClickHouse/pull/8851) ([Vladimir Chebotarev](https://github.com/excitoon))
* Implement `bitAnd`, `bitOr`, `bitXor`, `bitNot` for `FixedString(N)` datatype. [#9091](https://github.com/ClickHouse/ClickHouse/pull/9091) ([Guillaume Tassery](https://github.com/YiuRULE))
* Added function `bitCount`. This fixes [#8702](https://github.com/ClickHouse/ClickHouse/issues/8702). [#8708](https://github.com/ClickHouse/ClickHouse/pull/8708) ([alexey-milovidov](https://github.com/alexey-milovidov)) [#8749](https://github.com/ClickHouse/ClickHouse/pull/8749) ([ikopylov](https://github.com/ikopylov))
* Add `generateRandom` table function to generate random rows with given schema. Allows to populate arbitrary test table with data. [#8994](https://github.com/ClickHouse/ClickHouse/pull/8994) ([Ilya Yatsishin](https://github.com/qoega))
* `JSONEachRowFormat`: support special case when objects enclosed in top-level array. [#8860](https://github.com/ClickHouse/ClickHouse/pull/8860) ([Kruglov Pavel](https://github.com/Avogar))
* Now it's possible to create a column with `DEFAULT` expression which depends on a column with default `ALIAS` expression. [#9489](https://github.com/ClickHouse/ClickHouse/pull/9489) ([alesapin](https://github.com/alesapin))
* Allow to specify `--limit` more than the source data size in `clickhouse-obfuscator`. The data will repeat itself with different random seed. [#9155](https://github.com/ClickHouse/ClickHouse/pull/9155) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added `groupArraySample` function (similar to `groupArray`) with reservior sampling algorithm. [#8286](https://github.com/ClickHouse/ClickHouse/pull/8286) ([Amos Bird](https://github.com/amosbird))
* Now you can monitor the size of update queue in `cache`/`complex_key_cache` dictionaries via system metrics. [#9413](https://github.com/ClickHouse/ClickHouse/pull/9413) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Allow to use CRLF as a line separator in CSV output format with setting `output_format_csv_crlf_end_of_line` is set to 1 [#8934](https://github.com/ClickHouse/ClickHouse/pull/8934) [#8935](https://github.com/ClickHouse/ClickHouse/pull/8935) [#8963](https://github.com/ClickHouse/ClickHouse/pull/8963) ([Mikhail Korotov](https://github.com/millb))
* Implement more functions of the [H3](https://github.com/uber/h3) API: `h3GetBaseCell`, `h3HexAreaM2`, `h3IndexesAreNeighbors`, `h3ToChildren`, `h3ToString` and `stringToH3` [#8938](https://github.com/ClickHouse/ClickHouse/pull/8938) ([Nico Mandery](https://github.com/nmandery))
* New setting introduced: `max_parser_depth` to control maximum stack size and allow large complex queries. This fixes [#6681](https://github.com/ClickHouse/ClickHouse/issues/6681) and [#7668](https://github.com/ClickHouse/ClickHouse/issues/7668). [#8647](https://github.com/ClickHouse/ClickHouse/pull/8647) ([Maxim Smirnov](https://github.com/qMBQx8GH))
* Add a setting `force_optimize_skip_unused_shards` setting to throw if skipping of unused shards is not possible [#8805](https://github.com/ClickHouse/ClickHouse/pull/8805) ([Azat Khuzhin](https://github.com/azat))
* Allow to configure multiple disks/volumes for storing data for send in `Distributed` engine [#8756](https://github.com/ClickHouse/ClickHouse/pull/8756) ([Azat Khuzhin](https://github.com/azat))
* Support storage policy (`<tmp_policy>`) for storing temporary data. [#8750](https://github.com/ClickHouse/ClickHouse/pull/8750) ([Azat Khuzhin](https://github.com/azat))
* Added `X-ClickHouse-Exception-Code` HTTP header that is set if exception was thrown before sending data. This implements [#4971](https://github.com/ClickHouse/ClickHouse/issues/4971). [#8786](https://github.com/ClickHouse/ClickHouse/pull/8786) ([Mikhail Korotov](https://github.com/millb))
* Added function `ifNotFinite`. It is just a syntactic sugar: `ifNotFinite(x, y) = isFinite(x) ? x : y`. [#8710](https://github.com/ClickHouse/ClickHouse/pull/8710) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added `last_successful_update_time` column in `system.dictionaries` table [#9394](https://github.com/ClickHouse/ClickHouse/pull/9394) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Add `blockSerializedSize` function (size on disk without compression) [#8952](https://github.com/ClickHouse/ClickHouse/pull/8952) ([Azat Khuzhin](https://github.com/azat))
* Add function `moduloOrZero` [#9358](https://github.com/ClickHouse/ClickHouse/pull/9358) ([hcz](https://github.com/hczhcz))
* Added system tables `system.zeros` and `system.zeros_mt` as well as tale functions `zeros()` and `zeros_mt()`. Tables (and table functions) contain single column with name `zero` and type `UInt8`. This column contains zeros. It is needed for test purposes as the fastest method to generate many rows. This fixes [#6604](https://github.com/ClickHouse/ClickHouse/issues/6604) [#9593](https://github.com/ClickHouse/ClickHouse/pull/9593) ([Nikolai Kochetov](https://github.com/KochetovNicolai))

#### Experimental Feature
* Add new compact format of parts in `MergeTree`-family tables in which all columns are stored in one file. It helps to increase performance of small and frequent inserts. The old format (one file per column) is now called wide. Data storing format is controlled by settings `min_bytes_for_wide_part` and `min_rows_for_wide_part`. [#8290](https://github.com/ClickHouse/ClickHouse/pull/8290) ([Anton Popov](https://github.com/CurtizJ))
* Support for S3 storage for `Log`, `TinyLog` and `StripeLog` tables. [#8862](https://github.com/ClickHouse/ClickHouse/pull/8862) ([Pavel Kovalenko](https://github.com/Jokser))

#### Bug Fix
* Fixed inconsistent whitespaces in log messages. [#9322](https://github.com/ClickHouse/ClickHouse/pull/9322) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix bug in which arrays of unnamed tuples were flattened as Nested structures on table creation. [#8866](https://github.com/ClickHouse/ClickHouse/pull/8866) ([achulkov2](https://github.com/achulkov2))
* Fixed the issue when "Too many open files" error may happen if there are too many files matching glob pattern in `File` table or `file` table function. Now files are opened lazily. This fixes [#8857](https://github.com/ClickHouse/ClickHouse/issues/8857) [#8861](https://github.com/ClickHouse/ClickHouse/pull/8861) ([alexey-milovidov](https://github.com/alexey-milovidov))
* DROP TEMPORARY TABLE now drops only temporary table. [#8907](https://github.com/ClickHouse/ClickHouse/pull/8907) ([Vitaly Baranov](https://github.com/vitlibar))
* Remove outdated partition when we shutdown the server or DETACH/ATTACH a table. [#8602](https://github.com/ClickHouse/ClickHouse/pull/8602) ([Guillaume Tassery](https://github.com/YiuRULE))
* For how the default disk calculates the free space from `data` subdirectory. Fixed the issue when the amount of free space is not calculated correctly if the `data` directory is mounted to a separate device (rare case). This fixes [#7441](https://github.com/ClickHouse/ClickHouse/issues/7441) [#9257](https://github.com/ClickHouse/ClickHouse/pull/9257) ([Mikhail Korotov](https://github.com/millb))
* Allow comma (cross) join with IN () inside. [#9251](https://github.com/ClickHouse/ClickHouse/pull/9251) ([Artem Zuikov](https://github.com/4ertus2))
* Allow to rewrite CROSS to INNER JOIN if there's [NOT] LIKE operator in WHERE section. [#9229](https://github.com/ClickHouse/ClickHouse/pull/9229) ([Artem Zuikov](https://github.com/4ertus2))
* Fix possible incorrect result after `GROUP BY` with enabled setting `distributed_aggregation_memory_efficient`. Fixes [#9134](https://github.com/ClickHouse/ClickHouse/issues/9134). [#9289](https://github.com/ClickHouse/ClickHouse/pull/9289) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Found keys were counted as missed in metrics of cache dictionaries. [#9411](https://github.com/ClickHouse/ClickHouse/pull/9411) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Fix replication protocol incompatibility introduced in [#8598](https://github.com/ClickHouse/ClickHouse/issues/8598). [#9412](https://github.com/ClickHouse/ClickHouse/pull/9412) ([alesapin](https://github.com/alesapin))
* Fixed race condition on `queue_task_handle` at the startup of `ReplicatedMergeTree` tables. [#9552](https://github.com/ClickHouse/ClickHouse/pull/9552) ([alexey-milovidov](https://github.com/alexey-milovidov))
* The token `NOT` didn't work in `SHOW TABLES NOT LIKE` query [#8727](https://github.com/ClickHouse/ClickHouse/issues/8727) [#8940](https://github.com/ClickHouse/ClickHouse/pull/8940) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added range check to function `h3EdgeLengthM`. Without this check, buffer overflow is possible. [#8945](https://github.com/ClickHouse/ClickHouse/pull/8945) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed up a bug in batched calculations of ternary logical OPs on multiple arguments (more than 10). [#8718](https://github.com/ClickHouse/ClickHouse/pull/8718) ([Alexander Kazakov](https://github.com/Akazz))
* Fix error of PREWHERE optimization, which could lead to segfaults or `Inconsistent number of columns got from MergeTreeRangeReader` exception. [#9024](https://github.com/ClickHouse/ClickHouse/pull/9024) ([Anton Popov](https://github.com/CurtizJ))
* Fix unexpected `Timeout exceeded while reading from socket` exception, which randomly happens on secure connection before timeout actually exceeded and when query profiler is enabled. Also add `connect_timeout_with_failover_secure_ms` settings (default 100ms), which is similar to `connect_timeout_with_failover_ms`, but is used for secure connections (because SSL handshake is slower, than ordinary TCP connection) [#9026](https://github.com/ClickHouse/ClickHouse/pull/9026) ([tavplubix](https://github.com/tavplubix))
* Fix bug with mutations finalization, when mutation may hang in state with `parts_to_do=0` and `is_done=0`. [#9022](https://github.com/ClickHouse/ClickHouse/pull/9022) ([alesapin](https://github.com/alesapin))
* Use new ANY JOIN logic with `partial_merge_join` setting. It's possible to make `ANY|ALL|SEMI LEFT` and `ALL INNER` joins with `partial_merge_join=1` now. [#8932](https://github.com/ClickHouse/ClickHouse/pull/8932) ([Artem Zuikov](https://github.com/4ertus2))
* Shard now clamps the settings got from the initiator to the shard's constaints instead of throwing an exception. This fix allows to send queries to a shard with another constraints. [#9447](https://github.com/ClickHouse/ClickHouse/pull/9447) ([Vitaly Baranov](https://github.com/vitlibar))
* Fixed memory management problem in `MergeTreeReadPool`. [#8791](https://github.com/ClickHouse/ClickHouse/pull/8791) ([Vladimir Chebotarev](https://github.com/excitoon))
* Fix `toDecimal*OrNull()` functions family when called with string `e`. Fixes [#8312](https://github.com/ClickHouse/ClickHouse/issues/8312) [#8764](https://github.com/ClickHouse/ClickHouse/pull/8764) ([Artem Zuikov](https://github.com/4ertus2))
* Make sure that `FORMAT Null` sends no data to the client. [#8767](https://github.com/ClickHouse/ClickHouse/pull/8767) ([Alexander Kuzmenkov](https://github.com/akuzm))
* Fix bug that timestamp in `LiveViewBlockInputStream` will not updated. `LIVE VIEW` is an experimental feature. [#8644](https://github.com/ClickHouse/ClickHouse/pull/8644) ([vxider](https://github.com/Vxider)) [#8625](https://github.com/ClickHouse/ClickHouse/pull/8625) ([vxider](https://github.com/Vxider))
* Fixed `ALTER MODIFY TTL` wrong behavior which did not allow to delete old TTL expressions. [#8422](https://github.com/ClickHouse/ClickHouse/pull/8422) ([Vladimir Chebotarev](https://github.com/excitoon))
* Fixed UBSan report in MergeTreeIndexSet. This fixes [#9250](https://github.com/ClickHouse/ClickHouse/issues/9250) [#9365](https://github.com/ClickHouse/ClickHouse/pull/9365) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed the behaviour of `match` and `extract` functions when haystack has zero bytes. The behaviour was wrong when haystack was constant. This fixes [#9160](https://github.com/ClickHouse/ClickHouse/issues/9160) [#9163](https://github.com/ClickHouse/ClickHouse/pull/9163) ([alexey-milovidov](https://github.com/alexey-milovidov)) [#9345](https://github.com/ClickHouse/ClickHouse/pull/9345) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Avoid throwing from destructor in Apache Avro 3rd-party library. [#9066](https://github.com/ClickHouse/ClickHouse/pull/9066) ([Andrew Onyshchuk](https://github.com/oandrew))
* Don't commit a batch polled from `Kafka` partially as it can lead to holes in data. [#8876](https://github.com/ClickHouse/ClickHouse/pull/8876) ([filimonov](https://github.com/filimonov))
* Fix `joinGet` with nullable return types. https://github.com/ClickHouse/ClickHouse/issues/8919 [#9014](https://github.com/ClickHouse/ClickHouse/pull/9014) ([Amos Bird](https://github.com/amosbird))
* Fix data incompatibility when compressed with `T64` codec. [#9016](https://github.com/ClickHouse/ClickHouse/pull/9016) ([Artem Zuikov](https://github.com/4ertus2)) Fix data type ids in `T64` compression codec that leads to wrong (de)compression in affected versions. [#9033](https://github.com/ClickHouse/ClickHouse/pull/9033) ([Artem Zuikov](https://github.com/4ertus2))
* Add setting `enable_early_constant_folding` and disable it in some cases that leads to errors. [#9010](https://github.com/ClickHouse/ClickHouse/pull/9010) ([Artem Zuikov](https://github.com/4ertus2))
* Fix pushdown predicate optimizer with VIEW and enable the test [#9011](https://github.com/ClickHouse/ClickHouse/pull/9011) ([Winter Zhang](https://github.com/zhang2014))
* Fix segfault in `Merge` tables, that can happen when reading from `File` storages [#9387](https://github.com/ClickHouse/ClickHouse/pull/9387) ([tavplubix](https://github.com/tavplubix))
* Added a check for storage policy in `ATTACH PARTITION FROM`, `REPLACE PARTITION`, `MOVE TO TABLE`. Otherwise it could make data of part inaccessible after restart and prevent ClickHouse to start. [#9383](https://github.com/ClickHouse/ClickHouse/pull/9383) ([Vladimir Chebotarev](https://github.com/excitoon))
* Fix alters if there is TTL set for table. [#8800](https://github.com/ClickHouse/ClickHouse/pull/8800) ([Anton Popov](https://github.com/CurtizJ))
* Fix race condition that can happen when `SYSTEM RELOAD ALL DICTIONARIES` is executed while some dictionary is being modified/added/removed. [#8801](https://github.com/ClickHouse/ClickHouse/pull/8801) ([Vitaly Baranov](https://github.com/vitlibar))
* In previous versions `Memory` database engine use empty data path, so tables are created in `path` directory (e.g. `/var/lib/clickhouse/`), not in data directory of database (e.g. `/var/lib/clickhouse/db_name`). [#8753](https://github.com/ClickHouse/ClickHouse/pull/8753) ([tavplubix](https://github.com/tavplubix))
* Fixed wrong log messages about missing default disk or policy. [#9530](https://github.com/ClickHouse/ClickHouse/pull/9530) ([Vladimir Chebotarev](https://github.com/excitoon))
* Fix not(has()) for the bloom_filter index of array types. [#9407](https://github.com/ClickHouse/ClickHouse/pull/9407) ([achimbab](https://github.com/achimbab))
* Allow first column(s) in a table with `Log` engine be an alias [#9231](https://github.com/ClickHouse/ClickHouse/pull/9231) ([Ivan](https://github.com/abyss7))
* Fix order of ranges while reading from `MergeTree` table in one thread. It could lead to exceptions from `MergeTreeRangeReader` or wrong query results. [#9050](https://github.com/ClickHouse/ClickHouse/pull/9050) ([Anton Popov](https://github.com/CurtizJ))
* Make `reinterpretAsFixedString` to return `FixedString` instead of `String`. [#9052](https://github.com/ClickHouse/ClickHouse/pull/9052) ([Andrew Onyshchuk](https://github.com/oandrew))
* Avoid extremely rare cases when the user can get wrong error message (`Success` instead of detailed error description). [#9457](https://github.com/ClickHouse/ClickHouse/pull/9457) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Do not crash when using `Template` format with empty row template. [#8785](https://github.com/ClickHouse/ClickHouse/pull/8785) ([Alexander Kuzmenkov](https://github.com/akuzm))
* Metadata files for system tables could be created in wrong place [#8653](https://github.com/ClickHouse/ClickHouse/pull/8653) ([tavplubix](https://github.com/tavplubix)) Fixes [#8581](https://github.com/ClickHouse/ClickHouse/issues/8581).
* Fix data race on exception_ptr in cache dictionary [#8303](https://github.com/ClickHouse/ClickHouse/issues/8303). [#9379](https://github.com/ClickHouse/ClickHouse/pull/9379) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Do not throw an exception for query `ATTACH TABLE IF NOT EXISTS`. Previously it was thrown if table already exists, despite the `IF NOT EXISTS` clause. [#8967](https://github.com/ClickHouse/ClickHouse/pull/8967) ([Anton Popov](https://github.com/CurtizJ))
* Fixed missing closing paren in exception message. [#8811](https://github.com/ClickHouse/ClickHouse/pull/8811) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Avoid message `Possible deadlock avoided` at the startup of clickhouse-client in interactive mode. [#9455](https://github.com/ClickHouse/ClickHouse/pull/9455) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed the issue when padding at the end of base64 encoded value can be malformed. Update base64 library. This fixes [#9491](https://github.com/ClickHouse/ClickHouse/issues/9491), closes [#9492](https://github.com/ClickHouse/ClickHouse/issues/9492) [#9500](https://github.com/ClickHouse/ClickHouse/pull/9500) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Prevent losing data in `Kafka` in rare cases when exception happens after reading suffix but before commit. Fixes [#9378](https://github.com/ClickHouse/ClickHouse/issues/9378) [#9507](https://github.com/ClickHouse/ClickHouse/pull/9507) ([filimonov](https://github.com/filimonov))
* Fixed exception in `DROP TABLE IF EXISTS` [#8663](https://github.com/ClickHouse/ClickHouse/pull/8663) ([Nikita Vasilev](https://github.com/nikvas0))
* Fix crash when a user tries to `ALTER MODIFY SETTING` for old-formated `MergeTree` table engines family. [#9435](https://github.com/ClickHouse/ClickHouse/pull/9435) ([alesapin](https://github.com/alesapin))
* Support for UInt64 numbers that don't fit in Int64 in JSON-related functions. Update SIMDJSON to master. This fixes [#9209](https://github.com/ClickHouse/ClickHouse/issues/9209) [#9344](https://github.com/ClickHouse/ClickHouse/pull/9344) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed execution of inversed predicates when non-strictly monotinic functional index is used. [#9223](https://github.com/ClickHouse/ClickHouse/pull/9223) ([Alexander Kazakov](https://github.com/Akazz))
* Don't try to fold `IN` constant in `GROUP BY` [#8868](https://github.com/ClickHouse/ClickHouse/pull/8868) ([Amos Bird](https://github.com/amosbird))
* Fix bug in `ALTER DELETE` mutations which leads to index corruption. This fixes [#9019](https://github.com/ClickHouse/ClickHouse/issues/9019) and [#8982](https://github.com/ClickHouse/ClickHouse/issues/8982). Additionally fix extremely rare race conditions in `ReplicatedMergeTree` `ALTER` queries. [#9048](https://github.com/ClickHouse/ClickHouse/pull/9048) ([alesapin](https://github.com/alesapin))
* When the setting `compile_expressions` is enabled, you can get `unexpected column` in `LLVMExecutableFunction` when we use `Nullable` type [#8910](https://github.com/ClickHouse/ClickHouse/pull/8910) ([Guillaume Tassery](https://github.com/YiuRULE))
* Multiple fixes for `Kafka` engine: 1) fix duplicates that were appearing during consumer group rebalance. 2) Fix rare 'holes' appeared when data were polled from several partitions with one poll and committed partially (now we always process / commit the whole polled block of messages). 3) Fix flushes by block size (before that only flushing by timeout was working properly). 4) better subscription procedure (with assignment feedback). 5) Make tests work faster (with default intervals and timeouts). Due to the fact that data was not flushed by block size before (as it should according to documentation), that PR may lead to some performance degradation with default settings (due to more often & tinier flushes which are less optimal). If you encounter the performance issue after that change - please increase `kafka_max_block_size` in the table to the bigger value ( for example `CREATE TABLE ...Engine=Kafka ... SETTINGS ... kafka_max_block_size=524288`). Fixes [#7259](https://github.com/ClickHouse/ClickHouse/issues/7259) [#8917](https://github.com/ClickHouse/ClickHouse/pull/8917) ([filimonov](https://github.com/filimonov))
* Fix `Parameter out of bound` exception in some queries after PREWHERE optimizations. [#8914](https://github.com/ClickHouse/ClickHouse/pull/8914) ([Baudouin Giard](https://github.com/bgiard))
* Fixed the case of mixed-constness of arguments of function `arrayZip`. [#8705](https://github.com/ClickHouse/ClickHouse/pull/8705) ([alexey-milovidov](https://github.com/alexey-milovidov))
* When executing `CREATE` query, fold constant expressions in storage engine arguments. Replace empty database name with current database. Fixes [#6508](https://github.com/ClickHouse/ClickHouse/issues/6508), [#3492](https://github.com/ClickHouse/ClickHouse/issues/3492) [#9262](https://github.com/ClickHouse/ClickHouse/pull/9262) ([tavplubix](https://github.com/tavplubix))
* Now it's not possible to create or add columns with simple cyclic aliases like `a DEFAULT b, b DEFAULT a`. [#9603](https://github.com/ClickHouse/ClickHouse/pull/9603) ([alesapin](https://github.com/alesapin))
* Fixed a bug with double move which may corrupt original part. This is relevant if you use `ALTER TABLE MOVE` [#8680](https://github.com/ClickHouse/ClickHouse/pull/8680) ([Vladimir Chebotarev](https://github.com/excitoon))
* Allow `interval` identifier to correctly parse without backticks. Fixed issue when a query cannot be executed even if the `interval` identifier is enclosed in backticks or double quotes. This fixes [#9124](https://github.com/ClickHouse/ClickHouse/issues/9124). [#9142](https://github.com/ClickHouse/ClickHouse/pull/9142) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed fuzz test and incorrect behaviour of `bitTestAll`/`bitTestAny` functions. [#9143](https://github.com/ClickHouse/ClickHouse/pull/9143) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix possible crash/wrong number of rows in `LIMIT n WITH TIES` when there are a lot of rows equal to n'th row. [#9464](https://github.com/ClickHouse/ClickHouse/pull/9464) ([tavplubix](https://github.com/tavplubix))
* Fix mutations with parts written with enabled `insert_quorum`. [#9463](https://github.com/ClickHouse/ClickHouse/pull/9463) ([alesapin](https://github.com/alesapin))
* Fix data race at destruction of `Poco::HTTPServer`. It could happen when server is started and immediately shut down. [#9468](https://github.com/ClickHouse/ClickHouse/pull/9468) ([Anton Popov](https://github.com/CurtizJ))
* Fix bug in which a misleading error message was shown when running `SHOW CREATE TABLE a_table_that_does_not_exist`. [#8899](https://github.com/ClickHouse/ClickHouse/pull/8899) ([achulkov2](https://github.com/achulkov2))
* Fixed `Parameters are out of bound` exception in some rare cases when we have a constant in the `SELECT` clause when we have an `ORDER BY` and a `LIMIT` clause. [#8892](https://github.com/ClickHouse/ClickHouse/pull/8892) ([Guillaume Tassery](https://github.com/YiuRULE))
* Fix mutations finalization, when already done mutation can have status `is_done=0`. [#9217](https://github.com/ClickHouse/ClickHouse/pull/9217) ([alesapin](https://github.com/alesapin))
* Prevent from executing `ALTER ADD INDEX` for MergeTree tables with old syntax, because it doesn't work. [#8822](https://github.com/ClickHouse/ClickHouse/pull/8822) ([Mikhail Korotov](https://github.com/millb))
* During server startup do not access table, which `LIVE VIEW` depends on, so server will be able to start. Also remove `LIVE VIEW` dependencies when detaching `LIVE VIEW`. `LIVE VIEW` is an experimental feature. [#8824](https://github.com/ClickHouse/ClickHouse/pull/8824) ([tavplubix](https://github.com/tavplubix))
* Fix possible segfault in `MergeTreeRangeReader`, while executing `PREWHERE`. [#9106](https://github.com/ClickHouse/ClickHouse/pull/9106) ([Anton Popov](https://github.com/CurtizJ))
* Fix possible mismatched checksums with column TTLs. [#9451](https://github.com/ClickHouse/ClickHouse/pull/9451) ([Anton Popov](https://github.com/CurtizJ))
* Fixed a bug when parts were not being moved in background by TTL rules in case when there is only one volume. [#8672](https://github.com/ClickHouse/ClickHouse/pull/8672) ([Vladimir Chebotarev](https://github.com/excitoon))
* Fixed the issue `Method createColumn() is not implemented for data type Set`. This fixes [#7799](https://github.com/ClickHouse/ClickHouse/issues/7799). [#8674](https://github.com/ClickHouse/ClickHouse/pull/8674) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Now we will try finalize mutations more frequently. [#9427](https://github.com/ClickHouse/ClickHouse/pull/9427) ([alesapin](https://github.com/alesapin))
* Fix `intDiv` by minus one constant [#9351](https://github.com/ClickHouse/ClickHouse/pull/9351) ([hcz](https://github.com/hczhcz))
* Fix possible race condition in `BlockIO`. [#9356](https://github.com/ClickHouse/ClickHouse/pull/9356) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix bug leading to server termination when trying to use / drop `Kafka` table created with wrong parameters. [#9513](https://github.com/ClickHouse/ClickHouse/pull/9513) ([filimonov](https://github.com/filimonov))
* Added workaround if OS returns wrong result for `timer_create` function. [#8837](https://github.com/ClickHouse/ClickHouse/pull/8837) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed error in usage of `min_marks_for_seek` parameter. Fixed the error message when there is no sharding key in Distributed table and we try to skip unused shards. [#8908](https://github.com/ClickHouse/ClickHouse/pull/8908) ([Azat Khuzhin](https://github.com/azat))

#### Improvement
* Implement `ALTER MODIFY/DROP` queries on top of mutations for `ReplicatedMergeTree*` engines family. Now `ALTERS` blocks only at the metadata update stage, and don't block after that. [#8701](https://github.com/ClickHouse/ClickHouse/pull/8701) ([alesapin](https://github.com/alesapin))
* Add ability to rewrite CROSS to INNER JOINs with `WHERE` section containing unqialified names. [#9512](https://github.com/ClickHouse/ClickHouse/pull/9512) ([Artem Zuikov](https://github.com/4ertus2))
* Make `SHOW TABLES` and `SHOW DATABASES` queries support the `WHERE` expressions and `FROM`/`IN` [#9076](https://github.com/ClickHouse/ClickHouse/pull/9076) ([sundyli](https://github.com/sundy-li))
* Added a setting `deduplicate_blocks_in_dependent_materialized_views`. [#9070](https://github.com/ClickHouse/ClickHouse/pull/9070) ([urykhy](https://github.com/urykhy))
* After recent changes MySQL client started to print binary strings in hex thereby making them not readable ([#9032](https://github.com/ClickHouse/ClickHouse/issues/9032)). The workaround in ClickHouse is to mark string columns as UTF-8, which is not always, but usually the case. [#9079](https://github.com/ClickHouse/ClickHouse/pull/9079) ([Yuriy Baranov](https://github.com/yurriy))
* Add support of String and FixedString keys for `sumMap` [#8903](https://github.com/ClickHouse/ClickHouse/pull/8903) ([Baudouin Giard](https://github.com/bgiard))
* Support string keys in SummingMergeTree maps [#8933](https://github.com/ClickHouse/ClickHouse/pull/8933) ([Baudouin Giard](https://github.com/bgiard))
* Signal termination of thread to the thread pool even if the thread has thrown exception [#8736](https://github.com/ClickHouse/ClickHouse/pull/8736) ([Ding Xiang Fei](https://github.com/dingxiangfei2009))
* Allow to set `query_id` in `clickhouse-benchmark` [#9416](https://github.com/ClickHouse/ClickHouse/pull/9416) ([Anton Popov](https://github.com/CurtizJ))
* Don't allow strange expressions in `ALTER TABLE ... PARTITION partition` query. This addresses [#7192](https://github.com/ClickHouse/ClickHouse/issues/7192) [#8835](https://github.com/ClickHouse/ClickHouse/pull/8835) ([alexey-milovidov](https://github.com/alexey-milovidov))
* The table `system.table_engines` now provides information about feature support (like `supports_ttl` or `supports_sort_order`). [#8830](https://github.com/ClickHouse/ClickHouse/pull/8830) ([Max Akhmedov](https://github.com/zlobober))
* Enable `system.metric_log` by default. It will contain rows with values of ProfileEvents, CurrentMetrics collected with "collect_interval_milliseconds" interval (one second by default). The table is very small (usually in order of megabytes) and collecting this data by default is reasonable. [#9225](https://github.com/ClickHouse/ClickHouse/pull/9225) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Initialize query profiler for all threads in a group, e.g. it allows to fully profile insert-queries. Fixes [#6964](https://github.com/ClickHouse/ClickHouse/issues/6964) [#8874](https://github.com/ClickHouse/ClickHouse/pull/8874) ([Ivan](https://github.com/abyss7))
* Now temporary `LIVE VIEW` is created by `CREATE LIVE VIEW name WITH TIMEOUT [42] ...` instead of `CREATE TEMPORARY LIVE VIEW ...`, because the previous syntax was not consistent with `CREATE TEMPORARY TABLE ...` [#9131](https://github.com/ClickHouse/ClickHouse/pull/9131) ([tavplubix](https://github.com/tavplubix))
* Add text_log.level configuration parameter to limit entries that goes to `system.text_log` table [#8809](https://github.com/ClickHouse/ClickHouse/pull/8809) ([Azat Khuzhin](https://github.com/azat))
* Allow to put downloaded part to a disks/volumes according to TTL rules [#8598](https://github.com/ClickHouse/ClickHouse/pull/8598) ([Vladimir Chebotarev](https://github.com/excitoon))
* For external MySQL dictionaries, allow to mutualize MySQL connection pool to "share" them among dictionaries. This option significantly reduces the number of connections to MySQL servers. [#9409](https://github.com/ClickHouse/ClickHouse/pull/9409) ([Clément Rodriguez](https://github.com/clemrodriguez))
* Show nearest query execution time for quantiles in `clickhouse-benchmark` output instead of interpolated values. It's better to show values that correspond to the execution time of some queries. [#8712](https://github.com/ClickHouse/ClickHouse/pull/8712) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Possibility to add key & timestamp for the message when inserting data to Kafka. Fixes [#7198](https://github.com/ClickHouse/ClickHouse/issues/7198) [#8969](https://github.com/ClickHouse/ClickHouse/pull/8969) ([filimonov](https://github.com/filimonov))
* If server is run from terminal, highlight thread number, query id and log priority by colors. This is for improved readability of correlated log messages for developers. [#8961](https://github.com/ClickHouse/ClickHouse/pull/8961) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Better exception message while loading tables for `Ordinary` database. [#9527](https://github.com/ClickHouse/ClickHouse/pull/9527) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Implement `arraySlice` for arrays with aggregate function states. This fixes [#9388](https://github.com/ClickHouse/ClickHouse/issues/9388) [#9391](https://github.com/ClickHouse/ClickHouse/pull/9391) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Allow constant functions and constant arrays to be used on the right side of IN operator. [#8813](https://github.com/ClickHouse/ClickHouse/pull/8813) ([Anton Popov](https://github.com/CurtizJ))
* If zookeeper exception has happened while fetching data for system.replicas, display it in a separate column. This implements [#9137](https://github.com/ClickHouse/ClickHouse/issues/9137) [#9138](https://github.com/ClickHouse/ClickHouse/pull/9138) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Atomically remove MergeTree data parts on destroy. [#8402](https://github.com/ClickHouse/ClickHouse/pull/8402) ([Vladimir Chebotarev](https://github.com/excitoon))
* Support row-level security for Distributed tables. [#8926](https://github.com/ClickHouse/ClickHouse/pull/8926) ([Ivan](https://github.com/abyss7))
* Now we recognize suffix (like KB, KiB...) in settings values. [#8072](https://github.com/ClickHouse/ClickHouse/pull/8072) ([Mikhail Korotov](https://github.com/millb))
* Prevent out of memory while constructing result of a large JOIN. [#8637](https://github.com/ClickHouse/ClickHouse/pull/8637) ([Artem Zuikov](https://github.com/4ertus2))
* Added names of clusters to suggestions in interactive mode in `clickhouse-client`. [#8709](https://github.com/ClickHouse/ClickHouse/pull/8709) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Initialize query profiler for all threads in a group, e.g. it allows to fully profile insert-queries [#8820](https://github.com/ClickHouse/ClickHouse/pull/8820) ([Ivan](https://github.com/abyss7))
* Added column `exception_code` in `system.query_log` table. [#8770](https://github.com/ClickHouse/ClickHouse/pull/8770) ([Mikhail Korotov](https://github.com/millb))
* Enabled MySQL compatibility server on port `9004` in the default server configuration file. Fixed password generation command in the example in configuration. [#8771](https://github.com/ClickHouse/ClickHouse/pull/8771) ([Yuriy Baranov](https://github.com/yurriy))
* Prevent abort on shutdown if the filesystem is readonly. This fixes [#9094](https://github.com/ClickHouse/ClickHouse/issues/9094) [#9100](https://github.com/ClickHouse/ClickHouse/pull/9100) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Better exception message when length is required in HTTP POST query. [#9453](https://github.com/ClickHouse/ClickHouse/pull/9453) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add `_path` and `_file` virtual columns to `HDFS` and `File` engines and `hdfs` and `file` table functions [#8489](https://github.com/ClickHouse/ClickHouse/pull/8489) ([Olga Khvostikova](https://github.com/stavrolia))
* Fix error `Cannot find column` while inserting into `MATERIALIZED VIEW` in case if new column was added to view's internal table. [#8766](https://github.com/ClickHouse/ClickHouse/pull/8766) [#8788](https://github.com/ClickHouse/ClickHouse/pull/8788) ([vzakaznikov](https://github.com/vzakaznikov)) [#8788](https://github.com/ClickHouse/ClickHouse/issues/8788) [#8806](https://github.com/ClickHouse/ClickHouse/pull/8806) ([Nikolai Kochetov](https://github.com/KochetovNicolai)) [#8803](https://github.com/ClickHouse/ClickHouse/pull/8803) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix progress over native client-server protocol, by send progress after final update (like logs). This may be relevant only to some third-party tools that are using native protocol. [#9495](https://github.com/ClickHouse/ClickHouse/pull/9495) ([Azat Khuzhin](https://github.com/azat))
* Add a system metric tracking the number of client connections using MySQL protocol ([#9013](https://github.com/ClickHouse/ClickHouse/issues/9013)). [#9015](https://github.com/ClickHouse/ClickHouse/pull/9015) ([Eugene Klimov](https://github.com/Slach))
* From now on, HTTP responses will have `X-ClickHouse-Timezone` header set to the same timezone value that `SELECT timezone()` would report. [#9493](https://github.com/ClickHouse/ClickHouse/pull/9493) ([Denis Glazachev](https://github.com/traceon))

#### Performance Improvement
* Improve performance of analysing index with IN [#9261](https://github.com/ClickHouse/ClickHouse/pull/9261) ([Anton Popov](https://github.com/CurtizJ))
* Simpler and more efficient code in Logical Functions + code cleanups. A followup to [#8718](https://github.com/ClickHouse/ClickHouse/issues/8718) [#8728](https://github.com/ClickHouse/ClickHouse/pull/8728) ([Alexander Kazakov](https://github.com/Akazz))
* Overall performance improvement (in range of 5%..200% for affected queries) by ensuring even more strict aliasing with C++20 features. [#9304](https://github.com/ClickHouse/ClickHouse/pull/9304) ([Amos Bird](https://github.com/amosbird))
* More strict aliasing for inner loops of comparison functions. [#9327](https://github.com/ClickHouse/ClickHouse/pull/9327) ([alexey-milovidov](https://github.com/alexey-milovidov))
* More strict aliasing for inner loops of arithmetic functions. [#9325](https://github.com/ClickHouse/ClickHouse/pull/9325) ([alexey-milovidov](https://github.com/alexey-milovidov))
* A ~3 times faster implementation for ColumnVector::replicate(), via which ColumnConst::convertToFullColumn() is implemented. Also will be useful in tests when materializing constants. [#9293](https://github.com/ClickHouse/ClickHouse/pull/9293) ([Alexander Kazakov](https://github.com/Akazz))
* Another minor performance improvement to `ColumnVector::replicate()` (this speeds up the `materialize` function and higher order functions) an even further improvement to [#9293](https://github.com/ClickHouse/ClickHouse/issues/9293) [#9442](https://github.com/ClickHouse/ClickHouse/pull/9442) ([Alexander Kazakov](https://github.com/Akazz))
* Improved performance of `stochasticLinearRegression` aggregate function. This patch is contributed by Intel. [#8652](https://github.com/ClickHouse/ClickHouse/pull/8652) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Improve performance of `reinterpretAsFixedString` function. [#9342](https://github.com/ClickHouse/ClickHouse/pull/9342) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Do not send blocks to client for `Null` format in processors pipeline. [#8797](https://github.com/ClickHouse/ClickHouse/pull/8797) ([Nikolai Kochetov](https://github.com/KochetovNicolai)) [#8767](https://github.com/ClickHouse/ClickHouse/pull/8767) ([Alexander Kuzmenkov](https://github.com/akuzm))

#### Build/Testing/Packaging Improvement
* Exception handling now works correctly on Windows Subsystem for Linux. See https://github.com/ClickHouse-Extras/libunwind/pull/3 This fixes [#6480](https://github.com/ClickHouse/ClickHouse/issues/6480) [#9564](https://github.com/ClickHouse/ClickHouse/pull/9564) ([sobolevsv](https://github.com/sobolevsv))
* Replace `readline` with `replxx` for interactive line editing in `clickhouse-client` [#8416](https://github.com/ClickHouse/ClickHouse/pull/8416) ([Ivan](https://github.com/abyss7))
* Better build time and less template instantiations in FunctionsComparison. [#9324](https://github.com/ClickHouse/ClickHouse/pull/9324) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added integration with `clang-tidy` in CI. See also [#6044](https://github.com/ClickHouse/ClickHouse/issues/6044) [#9566](https://github.com/ClickHouse/ClickHouse/pull/9566) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Now we link ClickHouse in CI using `lld` even for `gcc`. [#9049](https://github.com/ClickHouse/ClickHouse/pull/9049) ([alesapin](https://github.com/alesapin))
* Allow to randomize thread scheduling and insert glitches when `THREAD_FUZZER_*` environment variables are set. This helps testing. [#9459](https://github.com/ClickHouse/ClickHouse/pull/9459) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Enable secure sockets in stateless tests [#9288](https://github.com/ClickHouse/ClickHouse/pull/9288) ([tavplubix](https://github.com/tavplubix))
* Make SPLIT_SHARED_LIBRARIES=OFF more robust [#9156](https://github.com/ClickHouse/ClickHouse/pull/9156) ([Azat Khuzhin](https://github.com/azat))
* Make "performance_introspection_and_logging" test reliable to random server stuck. This may happen in CI environment. See also [#9515](https://github.com/ClickHouse/ClickHouse/issues/9515) [#9528](https://github.com/ClickHouse/ClickHouse/pull/9528) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Validate XML in style check. [#9550](https://github.com/ClickHouse/ClickHouse/pull/9550) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed race condition in test `00738_lock_for_inner_table`. This test relied on sleep. [#9555](https://github.com/ClickHouse/ClickHouse/pull/9555) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Remove performance tests of type `once`. This is needed to run all performance tests in statistical comparison mode (more reliable). [#9557](https://github.com/ClickHouse/ClickHouse/pull/9557) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added performance test for arithmetic functions. [#9326](https://github.com/ClickHouse/ClickHouse/pull/9326) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added performance test for `sumMap` and `sumMapWithOverflow` aggregate functions. Follow-up for [#8933](https://github.com/ClickHouse/ClickHouse/issues/8933) [#8947](https://github.com/ClickHouse/ClickHouse/pull/8947) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Ensure style of ErrorCodes by style check. [#9370](https://github.com/ClickHouse/ClickHouse/pull/9370) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add script for tests history. [#8796](https://github.com/ClickHouse/ClickHouse/pull/8796) ([alesapin](https://github.com/alesapin))
* Add GCC warning `-Wsuggest-override` to locate and fix all places where `override` keyword must be used. [#8760](https://github.com/ClickHouse/ClickHouse/pull/8760) ([kreuzerkrieg](https://github.com/kreuzerkrieg))
* Ignore weak symbol under Mac OS X because it must be defined [#9538](https://github.com/ClickHouse/ClickHouse/pull/9538) ([Deleted user](https://github.com/ghost))
* Normalize running time of some queries in performance tests. This is done in preparation to run all the performance tests in comparison mode. [#9565](https://github.com/ClickHouse/ClickHouse/pull/9565) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix some tests to support pytest with query tests [#9062](https://github.com/ClickHouse/ClickHouse/pull/9062) ([Ivan](https://github.com/abyss7))
* Enable SSL in build with MSan, so server will not fail at startup when running stateless tests [#9531](https://github.com/ClickHouse/ClickHouse/pull/9531) ([tavplubix](https://github.com/tavplubix))
* Fix database substitution in test results [#9384](https://github.com/ClickHouse/ClickHouse/pull/9384) ([Ilya Yatsishin](https://github.com/qoega))
* Build fixes for miscellaneous platforms [#9381](https://github.com/ClickHouse/ClickHouse/pull/9381) ([proller](https://github.com/proller)) [#8755](https://github.com/ClickHouse/ClickHouse/pull/8755) ([proller](https://github.com/proller)) [#8631](https://github.com/ClickHouse/ClickHouse/pull/8631) ([proller](https://github.com/proller))
* Added disks section to stateless-with-coverage test docker image [#9213](https://github.com/ClickHouse/ClickHouse/pull/9213) ([Pavel Kovalenko](https://github.com/Jokser))
* Get rid of in-source-tree files when building with GRPC [#9588](https://github.com/ClickHouse/ClickHouse/pull/9588) ([Amos Bird](https://github.com/amosbird))
* Slightly faster build time by removing SessionCleaner from Context. Make the code of SessionCleaner more simple. [#9232](https://github.com/ClickHouse/ClickHouse/pull/9232) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Updated checking for hung queries in clickhouse-test script [#8858](https://github.com/ClickHouse/ClickHouse/pull/8858) ([Alexander Kazakov](https://github.com/Akazz))
* Removed some useless files from repository. [#8843](https://github.com/ClickHouse/ClickHouse/pull/8843) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Changed type of math perftests from `once` to `loop`. [#8783](https://github.com/ClickHouse/ClickHouse/pull/8783) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Add docker image which allows to build interactive code browser HTML report for our codebase. [#8781](https://github.com/ClickHouse/ClickHouse/pull/8781) ([alesapin](https://github.com/alesapin)) See [Woboq Code Browser](https://clickhouse-test-reports.s3.yandex.net/codebrowser/html_report///ClickHouse/dbms/index.html)
* Suppress some test failures under MSan. [#8780](https://github.com/ClickHouse/ClickHouse/pull/8780) ([Alexander Kuzmenkov](https://github.com/akuzm))
* Speedup "exception while insert" test. This test often time out in debug-with-coverage build. [#8711](https://github.com/ClickHouse/ClickHouse/pull/8711) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Updated `libcxx` and `libcxxabi` to master. In preparation to [#9304](https://github.com/ClickHouse/ClickHouse/issues/9304) [#9308](https://github.com/ClickHouse/ClickHouse/pull/9308) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix flacky test `00910_zookeeper_test_alter_compression_codecs`. [#9525](https://github.com/ClickHouse/ClickHouse/pull/9525) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Clean up duplicated linker flags. Make sure the linker won't look up an unexpected symbol. [#9433](https://github.com/ClickHouse/ClickHouse/pull/9433) ([Amos Bird](https://github.com/amosbird))
* Add `clickhouse-odbc` driver into test images. This allows to test interaction of ClickHouse with ClickHouse via its own ODBC driver. [#9348](https://github.com/ClickHouse/ClickHouse/pull/9348) ([filimonov](https://github.com/filimonov))
* Fix several bugs in unit tests. [#9047](https://github.com/ClickHouse/ClickHouse/pull/9047) ([alesapin](https://github.com/alesapin))
* Enable `-Wmissing-include-dirs` GCC warning to eliminate all non-existing includes - mostly as a result of CMake scripting errors [#8704](https://github.com/ClickHouse/ClickHouse/pull/8704) ([kreuzerkrieg](https://github.com/kreuzerkrieg))
* Describe reasons if query profiler cannot work. This is intended for [#9049](https://github.com/ClickHouse/ClickHouse/issues/9049) [#9144](https://github.com/ClickHouse/ClickHouse/pull/9144) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Update OpenSSL to upstream master. Fixed the issue when TLS connections may fail with the message `OpenSSL SSL_read: error:14094438:SSL routines:ssl3_read_bytes:tlsv1 alert internal error` and `SSL Exception: error:2400006E:random number generator::error retrieving entropy`. The issue was present in version 20.1. [#8956](https://github.com/ClickHouse/ClickHouse/pull/8956) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Update Dockerfile for server [#8893](https://github.com/ClickHouse/ClickHouse/pull/8893) ([Ilya Mazaev](https://github.com/ne-ray))
* Minor fixes in build-gcc-from-sources script [#8774](https://github.com/ClickHouse/ClickHouse/pull/8774) ([Michael Nacharov](https://github.com/mnach))
* Replace `numbers` to `zeros` in perftests where `number` column is not used. This will lead to more clean test results. [#9600](https://github.com/ClickHouse/ClickHouse/pull/9600) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix stack overflow issue when using initializer_list in Column constructors. [#9367](https://github.com/ClickHouse/ClickHouse/pull/9367) ([Deleted user](https://github.com/ghost))
* Upgrade librdkafka to v1.3.0. Enable bundled `rdkafka` and `gsasl` libraries on Mac OS X. [#9000](https://github.com/ClickHouse/ClickHouse/pull/9000) ([Andrew Onyshchuk](https://github.com/oandrew))
* build fix on GCC 9.2.0 [#9306](https://github.com/ClickHouse/ClickHouse/pull/9306) ([vxider](https://github.com/Vxider))


## ClickHouse release v20.1

### ClickHouse release v20.1.12.86, 2020-05-26

#### Bug Fix

* Fixed incompatibility of two-level aggregation between versions 20.1 and earlier. This incompatibility happens when different versions of ClickHouse are used on initiator node and remote nodes and the size of GROUP BY result is large and aggregation is performed by a single String field. It leads to several unmerged rows for a single key in result. [#10952](https://github.com/ClickHouse/ClickHouse/pull/10952) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fixed data corruption for `LowCardinality(FixedString)` key column in `SummingMergeTree` which could have happened after merge. Fixes [#10489](https://github.com/ClickHouse/ClickHouse/issues/10489). [#10721](https://github.com/ClickHouse/ClickHouse/pull/10721) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed bug, which causes http requests stuck on client close when `readonly=2` and `cancel_http_readonly_queries_on_client_close=1`. Fixes [#7939](https://github.com/ClickHouse/ClickHouse/issues/7939), [#7019](https://github.com/ClickHouse/ClickHouse/issues/7019), [#7736](https://github.com/ClickHouse/ClickHouse/issues/7736), [#7091](https://github.com/ClickHouse/ClickHouse/issues/7091). [#10684](https://github.com/ClickHouse/ClickHouse/pull/10684) ([tavplubix](https://github.com/tavplubix)).
* Fixed a bug when on `SYSTEM DROP DNS CACHE` query also drop caches, which are used to check if user is allowed to connect from some IP addresses. [#10608](https://github.com/ClickHouse/ClickHouse/pull/10608) ([tavplubix](https://github.com/tavplubix)).
* Fixed incorrect scalar results inside inner query of `MATERIALIZED VIEW` in case if this query contained dependent table. [#10603](https://github.com/ClickHouse/ClickHouse/pull/10603) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed the situation when mutation finished all parts, but hung up in `is_done=0`. [#10526](https://github.com/ClickHouse/ClickHouse/pull/10526) ([alesapin](https://github.com/alesapin)).
* Fixed overflow at beginning of unix epoch for timezones with fractional offset from UTC. This fixes [#9335](https://github.com/ClickHouse/ClickHouse/issues/9335). [#10513](https://github.com/ClickHouse/ClickHouse/pull/10513) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fixed improper shutdown of Distributed storage. [#10491](https://github.com/ClickHouse/ClickHouse/pull/10491) ([Azat Khuzhin](https://github.com/azat)).
* Fixed numeric overflow in `simpleLinearRegression` over large integers. [#10474](https://github.com/ClickHouse/ClickHouse/pull/10474) ([hcz](https://github.com/hczhcz)).
* Fixed removing metadata directory when attach database fails. [#10442](https://github.com/ClickHouse/ClickHouse/pull/10442) ([Winter Zhang](https://github.com/zhang2014)).
* Added a check of number and type of arguments when creating `BloomFilter` index [#9623](https://github.com/ClickHouse/ClickHouse/issues/9623). [#10431](https://github.com/ClickHouse/ClickHouse/pull/10431) ([Winter Zhang](https://github.com/zhang2014)).
* Fixed the issue when a query with `ARRAY JOIN`, `ORDER BY` and `LIMIT` may return incomplete result. This fixes [#10226](https://github.com/ClickHouse/ClickHouse/issues/10226). [#10427](https://github.com/ClickHouse/ClickHouse/pull/10427) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Prefer `fallback_to_stale_replicas` over `skip_unavailable_shards`. [#10422](https://github.com/ClickHouse/ClickHouse/pull/10422) ([Azat Khuzhin](https://github.com/azat)).
* Fixed wrong flattening of `Array(Tuple(...))` data types. This fixes [#10259](https://github.com/ClickHouse/ClickHouse/issues/10259). [#10390](https://github.com/ClickHouse/ClickHouse/pull/10390) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fixed wrong behavior in `HashTable` that caused compilation error when trying to read HashMap from buffer. [#10386](https://github.com/ClickHouse/ClickHouse/pull/10386) ([palasonic1](https://github.com/palasonic1)).
* Fixed possible `Pipeline stuck` error in `ConcatProcessor` which could have happened in remote query. [#10381](https://github.com/ClickHouse/ClickHouse/pull/10381) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed error `Pipeline stuck` with `max_rows_to_group_by` and `group_by_overflow_mode = 'break'`. [#10279](https://github.com/ClickHouse/ClickHouse/pull/10279) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed several bugs when some data was inserted with quorum, then deleted somehow (DROP PARTITION, TTL) and this leaded to the stuck of INSERTs or false-positive exceptions in SELECTs. This fixes [#9946](https://github.com/ClickHouse/ClickHouse/issues/9946). [#10188](https://github.com/ClickHouse/ClickHouse/pull/10188) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Fixed incompatibility when versions prior to 18.12.17 are used on remote servers and newer is used on initiating server, and GROUP BY both fixed and non-fixed keys, and when two-level group by method is activated. [#3254](https://github.com/ClickHouse/ClickHouse/pull/3254) ([alexey-milovidov](https://github.com/alexey-milovidov)).

#### Build/Testing/Packaging Improvement

* Added CA certificates to clickhouse-server docker image. [#10476](https://github.com/ClickHouse/ClickHouse/pull/10476) ([filimonov](https://github.com/filimonov)).


### ClickHouse release v20.1.10.70, 2020-04-17

#### Bug Fix

* Fix rare possible exception `Cannot drain connections: cancel first`. [#10239](https://github.com/ClickHouse/ClickHouse/pull/10239) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed bug where ClickHouse would throw `'Unknown function lambda.'` error message when user tries to run `ALTER UPDATE/DELETE` on tables with `ENGINE = Replicated*`. Check for nondeterministic functions now handles lambda expressions correctly. [#10237](https://github.com/ClickHouse/ClickHouse/pull/10237) ([Alexander Kazakov](https://github.com/Akazz)).
* Fix `parseDateTimeBestEffort` for strings in RFC-2822 when day of week is Tuesday or Thursday. This fixes [#10082](https://github.com/ClickHouse/ClickHouse/issues/10082). [#10214](https://github.com/ClickHouse/ClickHouse/pull/10214) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fix column names of constants inside `JOIN` that may clash with names of constants outside of `JOIN`. [#10207](https://github.com/ClickHouse/ClickHouse/pull/10207) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Fix possible inifinite query execution when the query actually should stop on LIMIT, while reading from infinite source like `system.numbers` or `system.zeros`. [#10206](https://github.com/ClickHouse/ClickHouse/pull/10206) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix move-to-prewhere optimization in presense of `arrayJoin` functions (in certain cases). This fixes [#10092](https://github.com/ClickHouse/ClickHouse/issues/10092). [#10195](https://github.com/ClickHouse/ClickHouse/pull/10195) ([alexey-milovidov](https://github.com/alexey-milovidov)).
* Add the ability to relax the restriction on non-deterministic functions usage in mutations with `allow_nondeterministic_mutations` setting. [#10186](https://github.com/ClickHouse/ClickHouse/pull/10186) ([filimonov](https://github.com/filimonov)).
* Convert blocks if structure does not match on `INSERT` into table with `Distributed` engine. [#10135](https://github.com/ClickHouse/ClickHouse/pull/10135) ([Azat Khuzhin](https://github.com/azat)).
* Fix `SIGSEGV` on `INSERT` into `Distributed` table when its structure differs from the underlying tables. [#10105](https://github.com/ClickHouse/ClickHouse/pull/10105) ([Azat Khuzhin](https://github.com/azat)).
* Fix possible rows loss for queries with `JOIN` and `UNION ALL`. Fixes [#9826](https://github.com/ClickHouse/ClickHouse/issues/9826), [#10113](https://github.com/ClickHouse/ClickHouse/issues/10113). [#10099](https://github.com/ClickHouse/ClickHouse/pull/10099) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Add arguments check and support identifier arguments for MySQL Database Engine. [#10077](https://github.com/ClickHouse/ClickHouse/pull/10077) ([Winter Zhang](https://github.com/zhang2014)).
* Fix bug in clickhouse dictionary source from localhost clickhouse server. The bug may lead to memory corruption if types in dictionary and source are not compatible. [#10071](https://github.com/ClickHouse/ClickHouse/pull/10071) ([alesapin](https://github.com/alesapin)).
* Fix error `Cannot clone block with columns because block has 0 columns ... While executing GroupingAggregatedTransform`. It happened when setting `distributed_aggregation_memory_efficient` was enabled, and distributed query read aggregating data with different level from different shards (mixed single and two level aggregation). [#10063](https://github.com/ClickHouse/ClickHouse/pull/10063) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix a segmentation fault that could occur in `GROUP BY` over string keys containing trailing zero bytes ([#8636](https://github.com/ClickHouse/ClickHouse/issues/8636), [#8925](https://github.com/ClickHouse/ClickHouse/issues/8925)). [#10025](https://github.com/ClickHouse/ClickHouse/pull/10025) ([Alexander Kuzmenkov](https://github.com/akuzm)).
* Fix bug in which the necessary tables weren't retrieved at one of the processing stages of queries to some databases. Fixes [#9699](https://github.com/ClickHouse/ClickHouse/issues/9699). [#9949](https://github.com/ClickHouse/ClickHouse/pull/9949) ([achulkov2](https://github.com/achulkov2)).
* Fix `'Not found column in block'` error when `JOIN` appears with `TOTALS`. Fixes [#9839](https://github.com/ClickHouse/ClickHouse/issues/9839). [#9939](https://github.com/ClickHouse/ClickHouse/pull/9939) ([Artem Zuikov](https://github.com/4ertus2)).
* Fix a bug with `ON CLUSTER` DDL queries freezing on server startup. [#9927](https://github.com/ClickHouse/ClickHouse/pull/9927) ([Gagan Arneja](https://github.com/garneja)).
* Fix `TRUNCATE` for Join table engine ([#9917](https://github.com/ClickHouse/ClickHouse/issues/9917)). [#9920](https://github.com/ClickHouse/ClickHouse/pull/9920) ([Amos Bird](https://github.com/amosbird)).
* Fix `'scalar doesn't exist'` error in ALTER queries ([#9878](https://github.com/ClickHouse/ClickHouse/issues/9878)). [#9904](https://github.com/ClickHouse/ClickHouse/pull/9904) ([Amos Bird](https://github.com/amosbird)).
* Fix race condition between drop and optimize in `ReplicatedMergeTree`. [#9901](https://github.com/ClickHouse/ClickHouse/pull/9901) ([alesapin](https://github.com/alesapin)).
* Fixed `DeleteOnDestroy` logic in `ATTACH PART` which could lead to automatic removal of attached part and added few tests. [#9410](https://github.com/ClickHouse/ClickHouse/pull/9410) ([Vladimir Chebotarev](https://github.com/excitoon)).

#### Build/Testing/Packaging Improvement

* Fix unit test `collapsing_sorted_stream`. [#9367](https://github.com/ClickHouse/ClickHouse/pull/9367) ([Deleted user](https://github.com/ghost)).

### ClickHouse release v20.1.9.54, 2020-03-28

#### Bug Fix

* Fix `'Different expressions with the same alias'` error when query has `PREWHERE` and `WHERE` on distributed table and `SET distributed_product_mode = 'local'`. [#9871](https://github.com/ClickHouse/ClickHouse/pull/9871) ([Artem Zuikov](https://github.com/4ertus2)).
* Fix mutations excessive memory consumption for tables with a composite primary key. This fixes [#9850](https://github.com/ClickHouse/ClickHouse/issues/9850). [#9860](https://github.com/ClickHouse/ClickHouse/pull/9860) ([alesapin](https://github.com/alesapin)).
* For INSERT queries shard now clamps the settings got from the initiator to the shard's constaints instead of throwing an exception. This fix allows to send `INSERT` queries to a shard with another constraints. This change improves fix [#9447](https://github.com/ClickHouse/ClickHouse/issues/9447). [#9852](https://github.com/ClickHouse/ClickHouse/pull/9852) ([Vitaly Baranov](https://github.com/vitlibar)).
* Fix possible exception `Got 0 in totals chunk, expected 1` on client. It happened for queries with `JOIN` in case if right joined table had zero rows. Example: `select * from system.one t1 join system.one t2 on t1.dummy = t2.dummy limit 0 FORMAT TabSeparated;`. Fixes [#9777](https://github.com/ClickHouse/ClickHouse/issues/9777). [#9823](https://github.com/ClickHouse/ClickHouse/pull/9823) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix `SIGSEGV` with `optimize_skip_unused_shards` when type cannot be converted. [#9804](https://github.com/ClickHouse/ClickHouse/pull/9804) ([Azat Khuzhin](https://github.com/azat)).
* Fixed a few cases when timezone of the function argument wasn't used properly. [#9574](https://github.com/ClickHouse/ClickHouse/pull/9574) ([Vasily Nemkov](https://github.com/Enmk)).

#### Improvement

* Remove `ORDER BY` stage from mutations because we read from a single ordered part in a single thread. Also add check that the order of rows in mutation is ordered in sorting key order and this order is not violated. [#9886](https://github.com/ClickHouse/ClickHouse/pull/9886) ([alesapin](https://github.com/alesapin)).

#### Build/Testing/Packaging Improvement

* Clean up duplicated linker flags. Make sure the linker won't look up an unexpected symbol. [#9433](https://github.com/ClickHouse/ClickHouse/pull/9433) ([Amos Bird](https://github.com/amosbird)).

### ClickHouse release v20.1.8.41, 2020-03-20

#### Bug Fix
* Fix possible permanent `Cannot schedule a task` error (due to unhandled exception in `ParallelAggregatingBlockInputStream::Handler::onFinish/onFinishThread`). This fixes [#6833](https://github.com/ClickHouse/ClickHouse/issues/6833). [#9154](https://github.com/ClickHouse/ClickHouse/pull/9154) ([Azat Khuzhin](https://github.com/azat))
* Fix excessive memory consumption in `ALTER` queries (mutations). This fixes [#9533](https://github.com/ClickHouse/ClickHouse/issues/9533) and [#9670](https://github.com/ClickHouse/ClickHouse/issues/9670). [#9754](https://github.com/ClickHouse/ClickHouse/pull/9754) ([alesapin](https://github.com/alesapin))
* Fix bug in backquoting in external dictionaries DDL. This fixes [#9619](https://github.com/ClickHouse/ClickHouse/issues/9619). [#9734](https://github.com/ClickHouse/ClickHouse/pull/9734) ([alesapin](https://github.com/alesapin))

### ClickHouse release v20.1.7.38, 2020-03-18

#### Bug Fix
* Fixed incorrect internal function names for `sumKahan` and `sumWithOverflow`. I lead to exception while using this functions in remote queries. [#9636](https://github.com/ClickHouse/ClickHouse/pull/9636) ([Azat Khuzhin](https://github.com/azat)). This issue was in all ClickHouse releases.
* Allow `ALTER ON CLUSTER` of `Distributed` tables with internal replication. This fixes [#3268](https://github.com/ClickHouse/ClickHouse/issues/3268). [#9617](https://github.com/ClickHouse/ClickHouse/pull/9617) ([shinoi2](https://github.com/shinoi2)). This issue was in all ClickHouse releases.
* Fix possible exceptions `Size of filter doesn't match size of column` and `Invalid number of rows in Chunk` in `MergeTreeRangeReader`. They could appear while executing `PREWHERE` in some cases. Fixes [#9132](https://github.com/ClickHouse/ClickHouse/issues/9132). [#9612](https://github.com/ClickHouse/ClickHouse/pull/9612) ([Anton Popov](https://github.com/CurtizJ))
* Fixed the issue: timezone was not preserved if you write a simple arithmetic expression like `time + 1` (in contrast to an expression like `time + INTERVAL 1 SECOND`). This fixes [#5743](https://github.com/ClickHouse/ClickHouse/issues/5743). [#9323](https://github.com/ClickHouse/ClickHouse/pull/9323) ([alexey-milovidov](https://github.com/alexey-milovidov)). This issue was in all ClickHouse releases.
* Now it's not possible to create or add columns with simple cyclic aliases like `a DEFAULT b, b DEFAULT a`. [#9603](https://github.com/ClickHouse/ClickHouse/pull/9603) ([alesapin](https://github.com/alesapin))
* Fixed the issue when padding at the end of base64 encoded value can be malformed. Update base64 library. This fixes [#9491](https://github.com/ClickHouse/ClickHouse/issues/9491), closes [#9492](https://github.com/ClickHouse/ClickHouse/issues/9492) [#9500](https://github.com/ClickHouse/ClickHouse/pull/9500) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix data race at destruction of `Poco::HTTPServer`. It could happen when server is started and immediately shut down. [#9468](https://github.com/ClickHouse/ClickHouse/pull/9468) ([Anton Popov](https://github.com/CurtizJ))
* Fix possible crash/wrong number of rows in `LIMIT n WITH TIES` when there are a lot of rows equal to n'th row. [#9464](https://github.com/ClickHouse/ClickHouse/pull/9464) ([tavplubix](https://github.com/tavplubix))
* Fix possible mismatched checksums with column TTLs. [#9451](https://github.com/ClickHouse/ClickHouse/pull/9451) ([Anton Popov](https://github.com/CurtizJ))
* Fix crash when a user tries to `ALTER MODIFY SETTING` for old-formated `MergeTree` table engines family. [#9435](https://github.com/ClickHouse/ClickHouse/pull/9435) ([alesapin](https://github.com/alesapin))
* Now we will try finalize mutations more frequently. [#9427](https://github.com/ClickHouse/ClickHouse/pull/9427) ([alesapin](https://github.com/alesapin))
* Fix replication protocol incompatibility introduced in [#8598](https://github.com/ClickHouse/ClickHouse/issues/8598). [#9412](https://github.com/ClickHouse/ClickHouse/pull/9412) ([alesapin](https://github.com/alesapin))
* Fix not(has()) for the bloom_filter index of array types. [#9407](https://github.com/ClickHouse/ClickHouse/pull/9407) ([achimbab](https://github.com/achimbab))
* Fixed the behaviour of `match` and `extract` functions when haystack has zero bytes. The behaviour was wrong when haystack was constant. This fixes [#9160](https://github.com/ClickHouse/ClickHouse/issues/9160) [#9163](https://github.com/ClickHouse/ClickHouse/pull/9163) ([alexey-milovidov](https://github.com/alexey-milovidov)) [#9345](https://github.com/ClickHouse/ClickHouse/pull/9345) ([alexey-milovidov](https://github.com/alexey-milovidov))

#### Build/Testing/Packaging Improvement

* Exception handling now works correctly on Windows Subsystem for Linux. See https://github.com/ClickHouse-Extras/libunwind/pull/3 This fixes [#6480](https://github.com/ClickHouse/ClickHouse/issues/6480) [#9564](https://github.com/ClickHouse/ClickHouse/pull/9564) ([sobolevsv](https://github.com/sobolevsv))


### ClickHouse release v20.1.6.30, 2020-03-05

#### Bug Fix

* Fix data incompatibility when compressed with `T64` codec.
[#9039](https://github.com/ClickHouse/ClickHouse/pull/9039) [(abyss7)](https://github.com/abyss7)
* Fix order of ranges while reading from MergeTree table in one thread. Fixes [#8964](https://github.com/ClickHouse/ClickHouse/issues/8964).
[#9050](https://github.com/ClickHouse/ClickHouse/pull/9050) [(CurtizJ)](https://github.com/CurtizJ)
* Fix possible segfault in `MergeTreeRangeReader`, while executing `PREWHERE`. Fixes [#9064](https://github.com/ClickHouse/ClickHouse/issues/9064).
[#9106](https://github.com/ClickHouse/ClickHouse/pull/9106) [(CurtizJ)](https://github.com/CurtizJ)
* Fix `reinterpretAsFixedString` to return `FixedString` instead of `String`.
[#9052](https://github.com/ClickHouse/ClickHouse/pull/9052) [(oandrew)](https://github.com/oandrew)
* Fix `joinGet` with nullable return types. Fixes [#8919](https://github.com/ClickHouse/ClickHouse/issues/8919)
[#9014](https://github.com/ClickHouse/ClickHouse/pull/9014) [(amosbird)](https://github.com/amosbird)
* Fix fuzz test and incorrect behaviour of bitTestAll/bitTestAny functions.
[#9143](https://github.com/ClickHouse/ClickHouse/pull/9143) [(alexey-milovidov)](https://github.com/alexey-milovidov)
* Fix the behaviour of match and extract functions when haystack has zero bytes. The behaviour was wrong when haystack was constant. Fixes [#9160](https://github.com/ClickHouse/ClickHouse/issues/9160)
[#9163](https://github.com/ClickHouse/ClickHouse/pull/9163) [(alexey-milovidov)](https://github.com/alexey-milovidov)
* Fixed execution of inversed predicates when non-strictly monotinic functional index is used. Fixes [#9034](https://github.com/ClickHouse/ClickHouse/issues/9034)
[#9223](https://github.com/ClickHouse/ClickHouse/pull/9223) [(Akazz)](https://github.com/Akazz)
* Allow to rewrite `CROSS` to `INNER JOIN` if there's `[NOT] LIKE` operator in `WHERE` section. Fixes [#9191](https://github.com/ClickHouse/ClickHouse/issues/9191)
[#9229](https://github.com/ClickHouse/ClickHouse/pull/9229) [(4ertus2)](https://github.com/4ertus2)
* Allow first column(s) in a table with Log engine be an alias.
[#9231](https://github.com/ClickHouse/ClickHouse/pull/9231) [(abyss7)](https://github.com/abyss7)
* Allow comma join with `IN()` inside. Fixes [#7314](https://github.com/ClickHouse/ClickHouse/issues/7314).
[#9251](https://github.com/ClickHouse/ClickHouse/pull/9251) [(4ertus2)](https://github.com/4ertus2)
* Improve `ALTER MODIFY/ADD` queries logic. Now you cannot `ADD` column without type, `MODIFY` default expression doesn't change type of column and `MODIFY` type doesn't loose default expression value. Fixes [#8669](https://github.com/ClickHouse/ClickHouse/issues/8669).
[#9227](https://github.com/ClickHouse/ClickHouse/pull/9227)  [(alesapin)](https://github.com/alesapin)
* Fix mutations finalization, when already done mutation can have status is_done=0.
[#9217](https://github.com/ClickHouse/ClickHouse/pull/9217) [(alesapin)](https://github.com/alesapin)
* Support "Processors" pipeline for system.numbers and system.numbers_mt. This also fixes the bug when `max_execution_time` is not respected.
[#7796](https://github.com/ClickHouse/ClickHouse/pull/7796)  [(KochetovNicolai)](https://github.com/KochetovNicolai)
* Fix wrong counting of `DictCacheKeysRequestedFound` metric.
[#9411](https://github.com/ClickHouse/ClickHouse/pull/9411) [(nikitamikhaylov)](https://github.com/nikitamikhaylov)
* Added a check for storage policy in `ATTACH PARTITION FROM`, `REPLACE PARTITION`, `MOVE TO TABLE` which otherwise could make data of part inaccessible after restart and prevent ClickHouse to start.
[#9383](https://github.com/ClickHouse/ClickHouse/pull/9383) [(excitoon)](https://github.com/excitoon)
* Fixed UBSan report in `MergeTreeIndexSet`. This fixes [#9250](https://github.com/ClickHouse/ClickHouse/issues/9250)
[#9365](https://github.com/ClickHouse/ClickHouse/pull/9365) [(alexey-milovidov)](https://github.com/alexey-milovidov)
* Fix possible datarace in BlockIO.
[#9356](https://github.com/ClickHouse/ClickHouse/pull/9356) [(KochetovNicolai)](https://github.com/KochetovNicolai)
* Support for `UInt64` numbers that don't fit in Int64 in JSON-related functions. Update `SIMDJSON` to master. This fixes [#9209](https://github.com/ClickHouse/ClickHouse/issues/9209)
[#9344](https://github.com/ClickHouse/ClickHouse/pull/9344) [(alexey-milovidov)](https://github.com/alexey-milovidov)
* Fix the issue when the amount of free space is not calculated correctly if the data directory is mounted to a separate device. For default disk calculate the free space from data subdirectory. This fixes [#7441](https://github.com/ClickHouse/ClickHouse/issues/7441)
[#9257](https://github.com/ClickHouse/ClickHouse/pull/9257) [(millb)](https://github.com/millb)
* Fix the issue when TLS connections may fail with the message `OpenSSL SSL_read: error:14094438:SSL routines:ssl3_read_bytes:tlsv1 alert internal error and SSL Exception: error:2400006E:random number generator::error retrieving entropy.` Update OpenSSL to upstream master.
[#8956](https://github.com/ClickHouse/ClickHouse/pull/8956) [(alexey-milovidov)](https://github.com/alexey-milovidov)
* When executing `CREATE` query, fold constant expressions in storage engine arguments. Replace empty database name with current database. Fixes [#6508](https://github.com/ClickHouse/ClickHouse/issues/6508), [#3492](https://github.com/ClickHouse/ClickHouse/issues/3492). Also fix check for local address in ClickHouseDictionarySource.
[#9262](https://github.com/ClickHouse/ClickHouse/pull/9262) [(tabplubix)](https://github.com/tavplubix)
* Fix segfault in `StorageMerge`, which can happen when reading from StorageFile.
[#9387](https://github.com/ClickHouse/ClickHouse/pull/9387) [(tabplubix)](https://github.com/tavplubix)
* Prevent losing data in `Kafka` in rare cases when exception happens after reading suffix but before commit. Fixes [#9378](https://github.com/ClickHouse/ClickHouse/issues/9378). Related: [#7175](https://github.com/ClickHouse/ClickHouse/issues/7175)
[#9507](https://github.com/ClickHouse/ClickHouse/pull/9507) [(filimonov)](https://github.com/filimonov)
* Fix bug leading to server termination when trying to use / drop `Kafka` table created with wrong parameters. Fixes [#9494](https://github.com/ClickHouse/ClickHouse/issues/9494). Incorporates [#9507](https://github.com/ClickHouse/ClickHouse/issues/9507).
[#9513](https://github.com/ClickHouse/ClickHouse/pull/9513) [(filimonov)](https://github.com/filimonov)

#### New Feature
* Add `deduplicate_blocks_in_dependent_materialized_views` option to control the behaviour of idempotent inserts into tables with materialized views. This new feature was added to the bugfix release by a special request from Altinity.
[#9070](https://github.com/ClickHouse/ClickHouse/pull/9070) [(urykhy)](https://github.com/urykhy)

### ClickHouse release v20.1.2.4, 2020-01-22

#### Backward Incompatible Change
* Make the setting `merge_tree_uniform_read_distribution` obsolete. The server still recognizes this setting but it has no effect. [#8308](https://github.com/ClickHouse/ClickHouse/pull/8308) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Changed return type of the function `greatCircleDistance` to `Float32` because now the result of calculation is `Float32`. [#7993](https://github.com/ClickHouse/ClickHouse/pull/7993) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Now it's expected that query parameters are represented in "escaped" format. For example, to pass string `a<tab>b` you have to write `a\tb` or `a\<tab>b` and respectively, `a%5Ctb` or `a%5C%09b` in URL. This is needed to add the possibility to pass NULL as `\N`. This fixes [#7488](https://github.com/ClickHouse/ClickHouse/issues/7488). [#8517](https://github.com/ClickHouse/ClickHouse/pull/8517) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Enable `use_minimalistic_part_header_in_zookeeper` setting for `ReplicatedMergeTree` by default. This will significantly reduce amount of data stored in ZooKeeper. This setting is supported since version 19.1 and we already use it in production in multiple services without any issues for more than half a year. Disable this setting if you have a chance to downgrade to versions older than 19.1. [#6850](https://github.com/ClickHouse/ClickHouse/pull/6850) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Data skipping indices are production ready and enabled by default. The settings `allow_experimental_data_skipping_indices`, `allow_experimental_cross_to_join_conversion` and `allow_experimental_multiple_joins_emulation` are now obsolete and do nothing. [#7974](https://github.com/ClickHouse/ClickHouse/pull/7974) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add new `ANY JOIN` logic for `StorageJoin` consistent with `JOIN` operation. To upgrade without changes in behaviour you need add `SETTINGS any_join_distinct_right_table_keys = 1` to Engine Join tables metadata or recreate these tables after upgrade. [#8400](https://github.com/ClickHouse/ClickHouse/pull/8400) ([Artem Zuikov](https://github.com/4ertus2))
* Require server to be restarted to apply the changes in logging configuration. This is a temporary workaround to avoid the bug where the server logs to a deleted log file (see [#8696](https://github.com/ClickHouse/ClickHouse/issues/8696)). [#8707](https://github.com/ClickHouse/ClickHouse/pull/8707) ([Alexander Kuzmenkov](https://github.com/akuzm))

#### New Feature
* Added information about part paths to `system.merges`. [#8043](https://github.com/ClickHouse/ClickHouse/pull/8043) ([Vladimir Chebotarev](https://github.com/excitoon))
* Add ability to execute `SYSTEM RELOAD DICTIONARY` query in `ON CLUSTER` mode. [#8288](https://github.com/ClickHouse/ClickHouse/pull/8288) ([Guillaume Tassery](https://github.com/YiuRULE))
* Add ability to execute `CREATE DICTIONARY` queries in `ON CLUSTER` mode. [#8163](https://github.com/ClickHouse/ClickHouse/pull/8163) ([alesapin](https://github.com/alesapin))
* Now user's profile in `users.xml` can inherit multiple profiles. [#8343](https://github.com/ClickHouse/ClickHouse/pull/8343) ([Mikhail f. Shiryaev](https://github.com/Felixoid))
* Added `system.stack_trace` table that allows to look at stack traces of all server threads. This is useful for developers to introspect server state. This fixes [#7576](https://github.com/ClickHouse/ClickHouse/issues/7576). [#8344](https://github.com/ClickHouse/ClickHouse/pull/8344) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add `DateTime64` datatype with configurable sub-second precision. [#7170](https://github.com/ClickHouse/ClickHouse/pull/7170) ([Vasily Nemkov](https://github.com/Enmk))
* Add table function `clusterAllReplicas` which allows to query all the nodes in the cluster. [#8493](https://github.com/ClickHouse/ClickHouse/pull/8493) ([kiran sunkari](https://github.com/kiransunkari))
* Add aggregate function `categoricalInformationValue` which calculates the information value of a discrete feature. [#8117](https://github.com/ClickHouse/ClickHouse/pull/8117) ([hcz](https://github.com/hczhcz))
* Speed up parsing of data files in `CSV`, `TSV` and `JSONEachRow` format by doing it in parallel. [#7780](https://github.com/ClickHouse/ClickHouse/pull/7780) ([Alexander Kuzmenkov](https://github.com/akuzm))
* Add function `bankerRound` which performs banker's rounding. [#8112](https://github.com/ClickHouse/ClickHouse/pull/8112) ([hcz](https://github.com/hczhcz))
* Support more languages in embedded dictionary for region names: 'ru', 'en', 'ua', 'uk', 'by', 'kz', 'tr', 'de', 'uz', 'lv', 'lt', 'et', 'pt', 'he', 'vi'. [#8189](https://github.com/ClickHouse/ClickHouse/pull/8189) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Improvements in consistency of `ANY JOIN` logic. Now `t1 ANY LEFT JOIN t2` equals `t2 ANY RIGHT JOIN t1`. [#7665](https://github.com/ClickHouse/ClickHouse/pull/7665) ([Artem Zuikov](https://github.com/4ertus2))
* Add setting `any_join_distinct_right_table_keys` which enables old behaviour for `ANY INNER JOIN`. [#7665](https://github.com/ClickHouse/ClickHouse/pull/7665) ([Artem Zuikov](https://github.com/4ertus2))
* Add new `SEMI` and `ANTI JOIN`. Old `ANY INNER JOIN` behaviour now available as `SEMI LEFT JOIN`. [#7665](https://github.com/ClickHouse/ClickHouse/pull/7665) ([Artem Zuikov](https://github.com/4ertus2))
* Added `Distributed` format for `File` engine and `file` table function which allows to read from `.bin` files generated by asynchronous  inserts into `Distributed` table. [#8535](https://github.com/ClickHouse/ClickHouse/pull/8535) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Add optional reset column argument for `runningAccumulate` which allows to reset aggregation results for each new key value. [#8326](https://github.com/ClickHouse/ClickHouse/pull/8326) ([Sergey Kononenko](https://github.com/kononencheg))
* Add ability to use ClickHouse as Prometheus endpoint. [#7900](https://github.com/ClickHouse/ClickHouse/pull/7900) ([vdimir](https://github.com/Vdimir))
* Add section `<remote_url_allow_hosts>` in `config.xml` which restricts allowed hosts for remote table engines and table functions `URL`, `S3`, `HDFS`. [#7154](https://github.com/ClickHouse/ClickHouse/pull/7154) ([Mikhail Korotov](https://github.com/millb))
* Added function `greatCircleAngle` which calculates the distance on a sphere in degrees. [#8105](https://github.com/ClickHouse/ClickHouse/pull/8105) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Changed Earth radius to be consistent with H3 library. [#8105](https://github.com/ClickHouse/ClickHouse/pull/8105) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added `JSONCompactEachRow` and `JSONCompactEachRowWithNamesAndTypes` formats for input and output. [#7841](https://github.com/ClickHouse/ClickHouse/pull/7841) ([Mikhail Korotov](https://github.com/millb))
* Added feature for file-related table engines and table functions (`File`, `S3`, `URL`, `HDFS`) which allows to read and write `gzip` files based on additional engine parameter or file extension. [#7840](https://github.com/ClickHouse/ClickHouse/pull/7840) ([Andrey Bodrov](https://github.com/apbodrov))
* Added the `randomASCII(length)` function, generating a string with a random set of [ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters) printable characters. [#8401](https://github.com/ClickHouse/ClickHouse/pull/8401) ([BayoNet](https://github.com/BayoNet))
* Added function `JSONExtractArrayRaw` which returns an array on unparsed json array elements from `JSON` string. [#8081](https://github.com/ClickHouse/ClickHouse/pull/8081) ([Oleg Matrokhin](https://github.com/errx))
* Add `arrayZip` function which allows to combine multiple arrays of equal lengths into one array of tuples. [#8149](https://github.com/ClickHouse/ClickHouse/pull/8149) ([Winter Zhang](https://github.com/zhang2014))
* Add ability to move data between disks according to configured `TTL`-expressions for `*MergeTree` table engines family. [#8140](https://github.com/ClickHouse/ClickHouse/pull/8140) ([Vladimir Chebotarev](https://github.com/excitoon))
* Added new aggregate function `avgWeighted` which allows to calculate weighted average. [#7898](https://github.com/ClickHouse/ClickHouse/pull/7898) ([Andrey Bodrov](https://github.com/apbodrov))
* Now parallel parsing is enabled by default for `TSV`, `TSKV`, `CSV` and `JSONEachRow` formats. [#7894](https://github.com/ClickHouse/ClickHouse/pull/7894) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Add several geo functions from `H3` library: `h3GetResolution`, `h3EdgeAngle`, `h3EdgeLength`, `h3IsValid` and `h3kRing`. [#8034](https://github.com/ClickHouse/ClickHouse/pull/8034) ([Konstantin Malanchev](https://github.com/hombit))
* Added support for brotli (`br`) compression in file-related storages and table functions. This fixes [#8156](https://github.com/ClickHouse/ClickHouse/issues/8156). [#8526](https://github.com/ClickHouse/ClickHouse/pull/8526) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add `groupBit*` functions for the `SimpleAggregationFunction` type. [#8485](https://github.com/ClickHouse/ClickHouse/pull/8485) ([Guillaume Tassery](https://github.com/YiuRULE))

#### Bug Fix
* Fix rename of tables with `Distributed` engine. Fixes issue [#7868](https://github.com/ClickHouse/ClickHouse/issues/7868). [#8306](https://github.com/ClickHouse/ClickHouse/pull/8306) ([tavplubix](https://github.com/tavplubix))
* Now dictionaries support `EXPRESSION` for attributes in arbitrary string in non-ClickHouse SQL dialect. [#8098](https://github.com/ClickHouse/ClickHouse/pull/8098) ([alesapin](https://github.com/alesapin))
* Fix broken `INSERT SELECT FROM mysql(...)` query. This fixes [#8070](https://github.com/ClickHouse/ClickHouse/issues/8070) and [#7960](https://github.com/ClickHouse/ClickHouse/issues/7960). [#8234](https://github.com/ClickHouse/ClickHouse/pull/8234) ([tavplubix](https://github.com/tavplubix))
* Fix error "Mismatch column sizes" when inserting default `Tuple` from `JSONEachRow`. This fixes [#5653](https://github.com/ClickHouse/ClickHouse/issues/5653). [#8606](https://github.com/ClickHouse/ClickHouse/pull/8606) ([tavplubix](https://github.com/tavplubix))
* Now an exception will be thrown in case of using `WITH TIES` alongside `LIMIT BY`. Also add ability to use `TOP` with `LIMIT BY`. This fixes [#7472](https://github.com/ClickHouse/ClickHouse/issues/7472). [#7637](https://github.com/ClickHouse/ClickHouse/pull/7637) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Fix unintendent dependency from fresh glibc version in `clickhouse-odbc-bridge` binary. [#8046](https://github.com/ClickHouse/ClickHouse/pull/8046) ([Amos Bird](https://github.com/amosbird))
* Fix bug in check function of `*MergeTree` engines family. Now it doesn't fail in case when we have equal amount of rows in last granule and last mark (non-final). [#8047](https://github.com/ClickHouse/ClickHouse/pull/8047) ([alesapin](https://github.com/alesapin))
* Fix insert into `Enum*` columns after `ALTER` query, when underlying numeric type is equal to table specified type. This fixes [#7836](https://github.com/ClickHouse/ClickHouse/issues/7836). [#7908](https://github.com/ClickHouse/ClickHouse/pull/7908) ([Anton Popov](https://github.com/CurtizJ))
* Allowed non-constant negative "size" argument for function `substring`. It was not allowed by mistake. This fixes [#4832](https://github.com/ClickHouse/ClickHouse/issues/4832). [#7703](https://github.com/ClickHouse/ClickHouse/pull/7703) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix parsing bug when wrong number of arguments passed to `(O|J)DBC` table engine. [#7709](https://github.com/ClickHouse/ClickHouse/pull/7709) ([alesapin](https://github.com/alesapin))
* Using command name of the running clickhouse process when sending logs to syslog. In previous versions, empty string was used instead of command name. [#8460](https://github.com/ClickHouse/ClickHouse/pull/8460) ([Michael Nacharov](https://github.com/mnach))
* Fix check of allowed hosts for `localhost`. This PR fixes the solution provided in [#8241](https://github.com/ClickHouse/ClickHouse/pull/8241). [#8342](https://github.com/ClickHouse/ClickHouse/pull/8342) ([Vitaly Baranov](https://github.com/vitlibar))
* Fix rare crash in `argMin` and `argMax` functions for long string arguments, when result is used in `runningAccumulate` function. This fixes [#8325](https://github.com/ClickHouse/ClickHouse/issues/8325) [#8341](https://github.com/ClickHouse/ClickHouse/pull/8341) ([dinosaur](https://github.com/769344359))
* Fix memory overcommit for tables with `Buffer` engine. [#8345](https://github.com/ClickHouse/ClickHouse/pull/8345) ([Azat Khuzhin](https://github.com/azat))
* Fixed potential bug in functions that can take `NULL` as one of the arguments and return non-NULL. [#8196](https://github.com/ClickHouse/ClickHouse/pull/8196) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Better metrics calculations in thread pool for background processes for `MergeTree` table engines. [#8194](https://github.com/ClickHouse/ClickHouse/pull/8194) ([Vladimir Chebotarev](https://github.com/excitoon))
* Fix function `IN` inside `WHERE` statement when row-level table filter is present. Fixes [#6687](https://github.com/ClickHouse/ClickHouse/issues/6687) [#8357](https://github.com/ClickHouse/ClickHouse/pull/8357) ([Ivan](https://github.com/abyss7))
* Now an exception is thrown if the integral value is not parsed completely for settings values. [#7678](https://github.com/ClickHouse/ClickHouse/pull/7678) ([Mikhail Korotov](https://github.com/millb))
* Fix exception when aggregate function is used in query to distributed table with more than two local shards. [#8164](https://github.com/ClickHouse/ClickHouse/pull/8164) ([小路](https://github.com/nicelulu))
* Now bloom filter can handle zero length arrays and doesn't perform redundant calculations. [#8242](https://github.com/ClickHouse/ClickHouse/pull/8242) ([achimbab](https://github.com/achimbab))
* Fixed checking if a client host is allowed by matching the client host to `host_regexp` specified in `users.xml`. [#8241](https://github.com/ClickHouse/ClickHouse/pull/8241) ([Vitaly Baranov](https://github.com/vitlibar))
* Relax ambiguous column check that leads to false positives in multiple `JOIN ON` section. [#8385](https://github.com/ClickHouse/ClickHouse/pull/8385) ([Artem Zuikov](https://github.com/4ertus2))
* Fixed possible server crash (`std::terminate`) when the server cannot send or write data in `JSON` or `XML` format with values of `String` data type (that require `UTF-8` validation) or when compressing result data with Brotli algorithm or in some other rare cases. This fixes [#7603](https://github.com/ClickHouse/ClickHouse/issues/7603) [#8384](https://github.com/ClickHouse/ClickHouse/pull/8384) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix race condition in `StorageDistributedDirectoryMonitor` found by CI. This fixes [#8364](https://github.com/ClickHouse/ClickHouse/issues/8364). [#8383](https://github.com/ClickHouse/ClickHouse/pull/8383) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Now background merges in `*MergeTree` table engines family preserve storage policy volume order more accurately. [#8549](https://github.com/ClickHouse/ClickHouse/pull/8549) ([Vladimir Chebotarev](https://github.com/excitoon))
* Now table engine `Kafka` works properly with `Native` format. This fixes [#6731](https://github.com/ClickHouse/ClickHouse/issues/6731) [#7337](https://github.com/ClickHouse/ClickHouse/issues/7337) [#8003](https://github.com/ClickHouse/ClickHouse/issues/8003). [#8016](https://github.com/ClickHouse/ClickHouse/pull/8016) ([filimonov](https://github.com/filimonov))
* Fixed formats with headers (like `CSVWithNames`) which were throwing exception about EOF for table engine `Kafka`. [#8016](https://github.com/ClickHouse/ClickHouse/pull/8016) ([filimonov](https://github.com/filimonov))
* Fixed a bug with making set from subquery in right part of `IN` section. This fixes [#5767](https://github.com/ClickHouse/ClickHouse/issues/5767) and [#2542](https://github.com/ClickHouse/ClickHouse/issues/2542). [#7755](https://github.com/ClickHouse/ClickHouse/pull/7755) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Fix possible crash while reading from storage `File`. [#7756](https://github.com/ClickHouse/ClickHouse/pull/7756) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fixed reading of the files in `Parquet` format containing columns of type `list`. [#8334](https://github.com/ClickHouse/ClickHouse/pull/8334) ([maxulan](https://github.com/maxulan))
* Fix error `Not found column` for distributed queries with `PREWHERE` condition dependent on sampling key if `max_parallel_replicas > 1`. [#7913](https://github.com/ClickHouse/ClickHouse/pull/7913) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix error `Not found column` if query used `PREWHERE` dependent on table's alias and the result set was empty because of primary key condition. [#7911](https://github.com/ClickHouse/ClickHouse/pull/7911) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fixed return type for functions `rand` and `randConstant` in case of `Nullable` argument. Now functions always return `UInt32` and never `Nullable(UInt32)`. [#8204](https://github.com/ClickHouse/ClickHouse/pull/8204) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Disabled predicate push-down for `WITH FILL` expression. This fixes [#7784](https://github.com/ClickHouse/ClickHouse/issues/7784). [#7789](https://github.com/ClickHouse/ClickHouse/pull/7789) ([Winter Zhang](https://github.com/zhang2014))
* Fixed incorrect `count()` result for `SummingMergeTree` when `FINAL` section is used. [#3280](https://github.com/ClickHouse/ClickHouse/issues/3280) [#7786](https://github.com/ClickHouse/ClickHouse/pull/7786) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Fix possible incorrect result for constant functions from remote servers. It happened for queries with functions like `version()`, `uptime()`, etc. which returns different constant values for different servers. This fixes [#7666](https://github.com/ClickHouse/ClickHouse/issues/7666). [#7689](https://github.com/ClickHouse/ClickHouse/pull/7689) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix complicated bug in push-down predicate optimization which leads to wrong results. This fixes a lot of issues on push-down predicate optimization. [#8503](https://github.com/ClickHouse/ClickHouse/pull/8503) ([Winter Zhang](https://github.com/zhang2014))
* Fix crash in `CREATE TABLE .. AS dictionary` query. [#8508](https://github.com/ClickHouse/ClickHouse/pull/8508) ([Azat Khuzhin](https://github.com/azat))
* Several improvements ClickHouse grammar in `.g4` file. [#8294](https://github.com/ClickHouse/ClickHouse/pull/8294) ([taiyang-li](https://github.com/taiyang-li))
* Fix bug that leads to crashes in `JOIN`s with tables with engine `Join`. This fixes [#7556](https://github.com/ClickHouse/ClickHouse/issues/7556) [#8254](https://github.com/ClickHouse/ClickHouse/issues/8254) [#7915](https://github.com/ClickHouse/ClickHouse/issues/7915) [#8100](https://github.com/ClickHouse/ClickHouse/issues/8100). [#8298](https://github.com/ClickHouse/ClickHouse/pull/8298) ([Artem Zuikov](https://github.com/4ertus2))
* Fix redundant dictionaries reload on `CREATE DATABASE`. [#7916](https://github.com/ClickHouse/ClickHouse/pull/7916) ([Azat Khuzhin](https://github.com/azat))
* Limit maximum number of streams for read from `StorageFile` and `StorageHDFS`. Fixes https://github.com/ClickHouse/ClickHouse/issues/7650. [#7981](https://github.com/ClickHouse/ClickHouse/pull/7981) ([alesapin](https://github.com/alesapin))
* Fix bug in `ALTER ... MODIFY ... CODEC` query, when user specify both default expression and codec. Fixes [8593](https://github.com/ClickHouse/ClickHouse/issues/8593). [#8614](https://github.com/ClickHouse/ClickHouse/pull/8614) ([alesapin](https://github.com/alesapin))
* Fix error in background merge of columns with `SimpleAggregateFunction(LowCardinality)` type. [#8613](https://github.com/ClickHouse/ClickHouse/pull/8613) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fixed type check in function `toDateTime64`. [#8375](https://github.com/ClickHouse/ClickHouse/pull/8375) ([Vasily Nemkov](https://github.com/Enmk))
* Now server do not crash on `LEFT` or `FULL JOIN` with and Join engine and unsupported `join_use_nulls` settings. [#8479](https://github.com/ClickHouse/ClickHouse/pull/8479) ([Artem Zuikov](https://github.com/4ertus2))
* Now `DROP DICTIONARY IF EXISTS db.dict` query doesn't throw exception if `db` doesn't exist. [#8185](https://github.com/ClickHouse/ClickHouse/pull/8185) ([Vitaly Baranov](https://github.com/vitlibar))
* Fix possible crashes in table functions (`file`, `mysql`, `remote`) caused by usage of reference to removed `IStorage` object. Fix incorrect parsing of columns specified at insertion into table function. [#7762](https://github.com/ClickHouse/ClickHouse/pull/7762) ([tavplubix](https://github.com/tavplubix))
* Ensure network be up before starting `clickhouse-server`. This fixes [#7507](https://github.com/ClickHouse/ClickHouse/issues/7507). [#8570](https://github.com/ClickHouse/ClickHouse/pull/8570) ([Zhichang Yu](https://github.com/yuzhichang))
* Fix timeouts handling for secure connections, so queries doesn't hang indefenitely. This fixes [#8126](https://github.com/ClickHouse/ClickHouse/issues/8126). [#8128](https://github.com/ClickHouse/ClickHouse/pull/8128) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix `clickhouse-copier`'s redundant contention between concurrent workers. [#7816](https://github.com/ClickHouse/ClickHouse/pull/7816) ([Ding Xiang Fei](https://github.com/dingxiangfei2009))
* Now mutations doesn't skip attached parts, even if their mutation version were larger than current mutation version. [#7812](https://github.com/ClickHouse/ClickHouse/pull/7812) ([Zhichang Yu](https://github.com/yuzhichang)) [#8250](https://github.com/ClickHouse/ClickHouse/pull/8250) ([alesapin](https://github.com/alesapin))
* Ignore redundant copies of `*MergeTree` data parts after move to another disk and server restart. [#7810](https://github.com/ClickHouse/ClickHouse/pull/7810) ([Vladimir Chebotarev](https://github.com/excitoon))
* Fix crash in `FULL JOIN` with `LowCardinality` in `JOIN` key. [#8252](https://github.com/ClickHouse/ClickHouse/pull/8252) ([Artem Zuikov](https://github.com/4ertus2))
* Forbidden to use column name more than once in insert query like `INSERT INTO tbl (x, y, x)`. This fixes [#5465](https://github.com/ClickHouse/ClickHouse/issues/5465), [#7681](https://github.com/ClickHouse/ClickHouse/issues/7681). [#7685](https://github.com/ClickHouse/ClickHouse/pull/7685) ([alesapin](https://github.com/alesapin))
* Added fallback for detection the number of physical CPU cores for unknown CPUs (using the number of logical CPU cores). This fixes [#5239](https://github.com/ClickHouse/ClickHouse/issues/5239). [#7726](https://github.com/ClickHouse/ClickHouse/pull/7726) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix `There's no column` error for materialized and alias columns. [#8210](https://github.com/ClickHouse/ClickHouse/pull/8210) ([Artem Zuikov](https://github.com/4ertus2))
* Fixed sever crash when `EXISTS` query was used without `TABLE` or `DICTIONARY` qualifier. Just like `EXISTS t`. This fixes [#8172](https://github.com/ClickHouse/ClickHouse/issues/8172). This bug was introduced in version 19.17. [#8213](https://github.com/ClickHouse/ClickHouse/pull/8213) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix rare bug with error `"Sizes of columns doesn't match"` that might appear when using `SimpleAggregateFunction` column. [#7790](https://github.com/ClickHouse/ClickHouse/pull/7790) ([Boris Granveaud](https://github.com/bgranvea))
* Fix bug where user with empty `allow_databases` got access to all databases (and same for `allow_dictionaries`). [#7793](https://github.com/ClickHouse/ClickHouse/pull/7793) ([DeifyTheGod](https://github.com/DeifyTheGod))
* Fix client crash when server already disconnected from client. [#8071](https://github.com/ClickHouse/ClickHouse/pull/8071) ([Azat Khuzhin](https://github.com/azat))
* Fix `ORDER BY` behaviour in case of sorting by primary key prefix and non primary key suffix. [#7759](https://github.com/ClickHouse/ClickHouse/pull/7759) ([Anton Popov](https://github.com/CurtizJ))
* Check if qualified column present in the table. This fixes [#6836](https://github.com/ClickHouse/ClickHouse/issues/6836). [#7758](https://github.com/ClickHouse/ClickHouse/pull/7758) ([Artem Zuikov](https://github.com/4ertus2))
* Fixed behavior with `ALTER MOVE` ran immediately after merge finish moves superpart of specified. Fixes [#8103](https://github.com/ClickHouse/ClickHouse/issues/8103). [#8104](https://github.com/ClickHouse/ClickHouse/pull/8104) ([Vladimir Chebotarev](https://github.com/excitoon))
* Fix possible server crash while using `UNION` with different number of columns. Fixes [#7279](https://github.com/ClickHouse/ClickHouse/issues/7279). [#7929](https://github.com/ClickHouse/ClickHouse/pull/7929) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix size of result substring for function `substr` with negative size. [#8589](https://github.com/ClickHouse/ClickHouse/pull/8589) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Now server does not execute part mutation in `MergeTree` if there are not enough free threads in background pool.  [#8588](https://github.com/ClickHouse/ClickHouse/pull/8588) ([tavplubix](https://github.com/tavplubix))
* Fix a minor typo on formatting `UNION ALL` AST. [#7999](https://github.com/ClickHouse/ClickHouse/pull/7999) ([litao91](https://github.com/litao91))
* Fixed incorrect bloom filter results for negative numbers. This fixes [#8317](https://github.com/ClickHouse/ClickHouse/issues/8317). [#8566](https://github.com/ClickHouse/ClickHouse/pull/8566) ([Winter Zhang](https://github.com/zhang2014))
* Fixed potential buffer overflow in decompress. Malicious user can pass fabricated compressed data that will cause read after buffer. This issue was found by Eldar Zaitov from Yandex information security team. [#8404](https://github.com/ClickHouse/ClickHouse/pull/8404) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix incorrect result because of integers overflow in `arrayIntersect`. [#7777](https://github.com/ClickHouse/ClickHouse/pull/7777) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Now `OPTIMIZE TABLE` query will not wait for offline replicas to perform the operation. [#8314](https://github.com/ClickHouse/ClickHouse/pull/8314) ([javi santana](https://github.com/javisantana))
* Fixed `ALTER TTL` parser for `Replicated*MergeTree` tables. [#8318](https://github.com/ClickHouse/ClickHouse/pull/8318) ([Vladimir Chebotarev](https://github.com/excitoon))
* Fix communication between server and client, so server read temporary tables info after query failure. [#8084](https://github.com/ClickHouse/ClickHouse/pull/8084) ([Azat Khuzhin](https://github.com/azat))
* Fix `bitmapAnd` function error when intersecting an aggregated bitmap and a scalar bitmap. [#8082](https://github.com/ClickHouse/ClickHouse/pull/8082) ([Yue Huang](https://github.com/moon03432))
* Refine the definition of `ZXid` according to the ZooKeeper Programmer's Guide which fixes bug in `clickhouse-cluster-copier`. [#8088](https://github.com/ClickHouse/ClickHouse/pull/8088) ([Ding Xiang Fei](https://github.com/dingxiangfei2009))
* `odbc` table function now respects `external_table_functions_use_nulls` setting. [#7506](https://github.com/ClickHouse/ClickHouse/pull/7506) ([Vasily Nemkov](https://github.com/Enmk))
* Fixed bug that lead to a rare data race. [#8143](https://github.com/ClickHouse/ClickHouse/pull/8143) ([Alexander Kazakov](https://github.com/Akazz))
* Now `SYSTEM RELOAD DICTIONARY` reloads a dictionary completely, ignoring `update_field`. This fixes [#7440](https://github.com/ClickHouse/ClickHouse/issues/7440). [#8037](https://github.com/ClickHouse/ClickHouse/pull/8037) ([Vitaly Baranov](https://github.com/vitlibar))
* Add ability to check if dictionary exists in create query. [#8032](https://github.com/ClickHouse/ClickHouse/pull/8032) ([alesapin](https://github.com/alesapin))
* Fix `Float*` parsing in `Values` format. This fixes [#7817](https://github.com/ClickHouse/ClickHouse/issues/7817). [#7870](https://github.com/ClickHouse/ClickHouse/pull/7870) ([tavplubix](https://github.com/tavplubix))
* Fix crash when we cannot reserve space in some background operations of `*MergeTree` table engines family. [#7873](https://github.com/ClickHouse/ClickHouse/pull/7873) ([Vladimir Chebotarev](https://github.com/excitoon))
* Fix crash of merge operation when table contains `SimpleAggregateFunction(LowCardinality)` column. This fixes [#8515](https://github.com/ClickHouse/ClickHouse/issues/8515). [#8522](https://github.com/ClickHouse/ClickHouse/pull/8522) ([Azat Khuzhin](https://github.com/azat))
* Restore support of all ICU locales and add the ability to apply collations for constant expressions. Also add language name to `system.collations` table. [#8051](https://github.com/ClickHouse/ClickHouse/pull/8051) ([alesapin](https://github.com/alesapin))
* Fix bug when external dictionaries with zero minimal lifetime (`LIFETIME(MIN 0 MAX N)`, `LIFETIME(N)`) don't update in background. [#7983](https://github.com/ClickHouse/ClickHouse/pull/7983) ([alesapin](https://github.com/alesapin))
* Fix crash when external dictionary with ClickHouse source has subquery in query. [#8351](https://github.com/ClickHouse/ClickHouse/pull/8351) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Fix incorrect parsing of file extension in table with engine `URL`. This fixes [#8157](https://github.com/ClickHouse/ClickHouse/issues/8157). [#8419](https://github.com/ClickHouse/ClickHouse/pull/8419) ([Andrey Bodrov](https://github.com/apbodrov))
* Fix `CHECK TABLE` query for `*MergeTree` tables without key. Fixes [#7543](https://github.com/ClickHouse/ClickHouse/issues/7543). [#7979](https://github.com/ClickHouse/ClickHouse/pull/7979) ([alesapin](https://github.com/alesapin))
* Fixed conversion of `Float64` to MySQL type. [#8079](https://github.com/ClickHouse/ClickHouse/pull/8079) ([Yuriy Baranov](https://github.com/yurriy))
* Now if table was not completely dropped because of server crash, server will try to restore and load it. [#8176](https://github.com/ClickHouse/ClickHouse/pull/8176) ([tavplubix](https://github.com/tavplubix))
* Fixed crash in table function `file` while inserting into file that doesn't exist. Now in this case file would be created and then insert would be processed. [#8177](https://github.com/ClickHouse/ClickHouse/pull/8177) ([Olga Khvostikova](https://github.com/stavrolia))
* Fix rare deadlock which can happen when `trace_log` is in enabled. [#7838](https://github.com/ClickHouse/ClickHouse/pull/7838) ([filimonov](https://github.com/filimonov))
* Add ability to work with different types besides `Date` in `RangeHashed` external dictionary created from DDL query. Fixes [7899](https://github.com/ClickHouse/ClickHouse/issues/7899). [#8275](https://github.com/ClickHouse/ClickHouse/pull/8275) ([alesapin](https://github.com/alesapin))
* Fixes crash when `now64()` is called with result of another function. [#8270](https://github.com/ClickHouse/ClickHouse/pull/8270) ([Vasily Nemkov](https://github.com/Enmk))
* Fixed bug with detecting client IP for connections through mysql wire protocol. [#7743](https://github.com/ClickHouse/ClickHouse/pull/7743) ([Dmitry Muzyka](https://github.com/dmitriy-myz))
* Fix empty array handling in `arraySplit` function. This fixes [#7708](https://github.com/ClickHouse/ClickHouse/issues/7708). [#7747](https://github.com/ClickHouse/ClickHouse/pull/7747) ([hcz](https://github.com/hczhcz))
* Fixed the issue when `pid-file` of another running `clickhouse-server` may be deleted. [#8487](https://github.com/ClickHouse/ClickHouse/pull/8487) ([Weiqing Xu](https://github.com/weiqxu))
* Fix dictionary reload if it has `invalidate_query`, which stopped updates and some exception on previous update tries. [#8029](https://github.com/ClickHouse/ClickHouse/pull/8029) ([alesapin](https://github.com/alesapin))
* Fixed error in function `arrayReduce` that may lead to "double free" and error in aggregate function combinator `Resample` that may lead to memory leak. Added aggregate function `aggThrow`. This function can be used for testing purposes. [#8446](https://github.com/ClickHouse/ClickHouse/pull/8446) ([alexey-milovidov](https://github.com/alexey-milovidov))

#### Improvement
* Improved logging when working with `S3` table engine. [#8251](https://github.com/ClickHouse/ClickHouse/pull/8251) ([Grigory Pervakov](https://github.com/GrigoryPervakov))
* Printed help message when no arguments are passed when calling `clickhouse-local`. This fixes [#5335](https://github.com/ClickHouse/ClickHouse/issues/5335). [#8230](https://github.com/ClickHouse/ClickHouse/pull/8230) ([Andrey Nagorny](https://github.com/Melancholic))
* Add setting `mutations_sync` which allows to wait `ALTER UPDATE/DELETE` queries synchronously. [#8237](https://github.com/ClickHouse/ClickHouse/pull/8237) ([alesapin](https://github.com/alesapin))
* Allow to set up relative `user_files_path` in `config.xml` (in the way similar to `format_schema_path`). [#7632](https://github.com/ClickHouse/ClickHouse/pull/7632) ([hcz](https://github.com/hczhcz))
* Add exception for illegal types for conversion functions with `-OrZero` postfix. [#7880](https://github.com/ClickHouse/ClickHouse/pull/7880) ([Andrey Konyaev](https://github.com/akonyaev90))
* Simplify format of the header of data sending to a shard in a distributed query. [#8044](https://github.com/ClickHouse/ClickHouse/pull/8044) ([Vitaly Baranov](https://github.com/vitlibar))
* `Live View` table engine refactoring. [#8519](https://github.com/ClickHouse/ClickHouse/pull/8519) ([vzakaznikov](https://github.com/vzakaznikov))
* Add additional checks for external dictionaries created from DDL-queries. [#8127](https://github.com/ClickHouse/ClickHouse/pull/8127) ([alesapin](https://github.com/alesapin))
* Fix error `Column ... already exists` while using `FINAL` and `SAMPLE` together, e.g. `select count() from table final sample 1/2`. Fixes [#5186](https://github.com/ClickHouse/ClickHouse/issues/5186). [#7907](https://github.com/ClickHouse/ClickHouse/pull/7907) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Now table the first argument of `joinGet` function can be table indentifier. [#7707](https://github.com/ClickHouse/ClickHouse/pull/7707) ([Amos Bird](https://github.com/amosbird))
* Allow using `MaterializedView` with subqueries above `Kafka` tables. [#8197](https://github.com/ClickHouse/ClickHouse/pull/8197) ([filimonov](https://github.com/filimonov))
* Now background moves between disks run it the seprate thread pool. [#7670](https://github.com/ClickHouse/ClickHouse/pull/7670) ([Vladimir Chebotarev](https://github.com/excitoon))
* `SYSTEM RELOAD DICTIONARY` now executes synchronously. [#8240](https://github.com/ClickHouse/ClickHouse/pull/8240) ([Vitaly Baranov](https://github.com/vitlibar))
* Stack traces now display physical addresses (offsets in object file) instead of virtual memory addresses (where the object file was loaded). That allows the use of `addr2line` when binary is position independent and ASLR is active. This fixes [#8360](https://github.com/ClickHouse/ClickHouse/issues/8360). [#8387](https://github.com/ClickHouse/ClickHouse/pull/8387) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Support new syntax for row-level security filters: `<table name='table_name'>…</table>`. Fixes [#5779](https://github.com/ClickHouse/ClickHouse/issues/5779). [#8381](https://github.com/ClickHouse/ClickHouse/pull/8381) ([Ivan](https://github.com/abyss7))
* Now `cityHash` function can work with `Decimal` and `UUID` types. Fixes [#5184](https://github.com/ClickHouse/ClickHouse/issues/5184). [#7693](https://github.com/ClickHouse/ClickHouse/pull/7693) ([Mikhail Korotov](https://github.com/millb))
* Removed fixed index granularity (it was 1024) from system logs because it's obsolete after implementation of adaptive granularity. [#7698](https://github.com/ClickHouse/ClickHouse/pull/7698) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Enabled MySQL compatibility server when ClickHouse is compiled without SSL. [#7852](https://github.com/ClickHouse/ClickHouse/pull/7852) ([Yuriy Baranov](https://github.com/yurriy))
* Now server checksums distributed batches, which gives more verbose errors in case of corrupted data in batch. [#7914](https://github.com/ClickHouse/ClickHouse/pull/7914) ([Azat Khuzhin](https://github.com/azat))
* Support `DROP DATABASE`, `DETACH TABLE`, `DROP TABLE` and `ATTACH TABLE` for `MySQL` database engine. [#8202](https://github.com/ClickHouse/ClickHouse/pull/8202) ([Winter Zhang](https://github.com/zhang2014))
* Add authentication in S3 table function and table engine. [#7623](https://github.com/ClickHouse/ClickHouse/pull/7623) ([Vladimir Chebotarev](https://github.com/excitoon))
* Added check for extra parts of `MergeTree` at different disks, in order to not allow to miss data parts at undefined disks. [#8118](https://github.com/ClickHouse/ClickHouse/pull/8118) ([Vladimir Chebotarev](https://github.com/excitoon))
* Enable SSL support for Mac client and server. [#8297](https://github.com/ClickHouse/ClickHouse/pull/8297) ([Ivan](https://github.com/abyss7))
* Now ClickHouse can work as MySQL federated server (see https://dev.mysql.com/doc/refman/5.7/en/federated-create-server.html). [#7717](https://github.com/ClickHouse/ClickHouse/pull/7717) ([Maxim Fedotov](https://github.com/MaxFedotov))
* `clickhouse-client` now only enable `bracketed-paste` when multiquery is on and multiline is off. This fixes (#7757)[https://github.com/ClickHouse/ClickHouse/issues/7757]. [#7761](https://github.com/ClickHouse/ClickHouse/pull/7761) ([Amos Bird](https://github.com/amosbird))
* Support `Array(Decimal)` in `if` function. [#7721](https://github.com/ClickHouse/ClickHouse/pull/7721) ([Artem Zuikov](https://github.com/4ertus2))
* Support Decimals in `arrayDifference`, `arrayCumSum` and `arrayCumSumNegative` functions. [#7724](https://github.com/ClickHouse/ClickHouse/pull/7724) ([Artem Zuikov](https://github.com/4ertus2))
* Added `lifetime` column to `system.dictionaries` table. [#6820](https://github.com/ClickHouse/ClickHouse/issues/6820) [#7727](https://github.com/ClickHouse/ClickHouse/pull/7727) ([kekekekule](https://github.com/kekekekule))
* Improved check for existing parts on different disks for `*MergeTree` table engines. Addresses [#7660](https://github.com/ClickHouse/ClickHouse/issues/7660). [#8440](https://github.com/ClickHouse/ClickHouse/pull/8440) ([Vladimir Chebotarev](https://github.com/excitoon))
* Integration with `AWS SDK` for `S3` interactions which allows to use all S3 features out of the box. [#8011](https://github.com/ClickHouse/ClickHouse/pull/8011) ([Pavel Kovalenko](https://github.com/Jokser))
* Added support for subqueries in `Live View` tables. [#7792](https://github.com/ClickHouse/ClickHouse/pull/7792) ([vzakaznikov](https://github.com/vzakaznikov))
* Check for using `Date` or `DateTime` column from `TTL` expressions was removed. [#7920](https://github.com/ClickHouse/ClickHouse/pull/7920) ([Vladimir Chebotarev](https://github.com/excitoon))
* Information about disk was added to `system.detached_parts` table. [#7833](https://github.com/ClickHouse/ClickHouse/pull/7833) ([Vladimir Chebotarev](https://github.com/excitoon))
* Now settings `max_(table|partition)_size_to_drop` can be changed without a restart. [#7779](https://github.com/ClickHouse/ClickHouse/pull/7779) ([Grigory Pervakov](https://github.com/GrigoryPervakov))
* Slightly better usability of error messages. Ask user not to remove the lines below `Stack trace:`. [#7897](https://github.com/ClickHouse/ClickHouse/pull/7897) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Better reading messages from `Kafka` engine in various formats after [#7935](https://github.com/ClickHouse/ClickHouse/issues/7935). [#8035](https://github.com/ClickHouse/ClickHouse/pull/8035) ([Ivan](https://github.com/abyss7))
* Better compatibility with MySQL clients which don't support `sha2_password` auth plugin. [#8036](https://github.com/ClickHouse/ClickHouse/pull/8036) ([Yuriy Baranov](https://github.com/yurriy))
* Support more column types in MySQL compatibility server. [#7975](https://github.com/ClickHouse/ClickHouse/pull/7975) ([Yuriy Baranov](https://github.com/yurriy))
* Implement `ORDER BY` optimization for `Merge`, `Buffer` and `Materilized View` storages with underlying `MergeTree` tables. [#8130](https://github.com/ClickHouse/ClickHouse/pull/8130) ([Anton Popov](https://github.com/CurtizJ))
* Now we always use POSIX implementation of `getrandom` to have better compatibility with old kernels (< 3.17). [#7940](https://github.com/ClickHouse/ClickHouse/pull/7940) ([Amos Bird](https://github.com/amosbird))
* Better check for valid destination in a move TTL rule. [#8410](https://github.com/ClickHouse/ClickHouse/pull/8410) ([Vladimir Chebotarev](https://github.com/excitoon))
* Better checks for broken insert batches for `Distributed` table engine. [#7933](https://github.com/ClickHouse/ClickHouse/pull/7933) ([Azat Khuzhin](https://github.com/azat))
* Add column with array of parts name which mutations must process in future to `system.mutations` table. [#8179](https://github.com/ClickHouse/ClickHouse/pull/8179) ([alesapin](https://github.com/alesapin))
* Parallel merge sort optimization for processors. [#8552](https://github.com/ClickHouse/ClickHouse/pull/8552) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* The settings `mark_cache_min_lifetime` is now obsolete and does nothing. In previous versions, mark cache can grow in memory larger than `mark_cache_size` to accomodate data within `mark_cache_min_lifetime` seconds. That was leading to confusion and higher memory usage than expected, that is especially bad on memory constrained systems. If you will see performance degradation after installing this release, you should increase the `mark_cache_size`. [#8484](https://github.com/ClickHouse/ClickHouse/pull/8484) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Preparation to use `tid` everywhere. This is needed for [#7477](https://github.com/ClickHouse/ClickHouse/issues/7477). [#8276](https://github.com/ClickHouse/ClickHouse/pull/8276) ([alexey-milovidov](https://github.com/alexey-milovidov))

#### Performance Improvement
* Performance optimizations in processors pipeline. [#7988](https://github.com/ClickHouse/ClickHouse/pull/7988) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Non-blocking updates of expired keys in cache dictionaries (with permission to read old ones). [#8303](https://github.com/ClickHouse/ClickHouse/pull/8303) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Compile ClickHouse without `-fno-omit-frame-pointer` globally to spare one more register. [#8097](https://github.com/ClickHouse/ClickHouse/pull/8097) ([Amos Bird](https://github.com/amosbird))
* Speedup `greatCircleDistance` function and add performance tests for it. [#7307](https://github.com/ClickHouse/ClickHouse/pull/7307) ([Olga Khvostikova](https://github.com/stavrolia))
* Improved performance of function `roundDown`. [#8465](https://github.com/ClickHouse/ClickHouse/pull/8465) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Improved performance of `max`, `min`, `argMin`, `argMax` for `DateTime64` data type. [#8199](https://github.com/ClickHouse/ClickHouse/pull/8199) ([Vasily Nemkov](https://github.com/Enmk))
* Improved performance of sorting without a limit or with big limit and external sorting. [#8545](https://github.com/ClickHouse/ClickHouse/pull/8545) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Improved performance of formatting floating point numbers up to 6 times. [#8542](https://github.com/ClickHouse/ClickHouse/pull/8542) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Improved performance of `modulo` function. [#7750](https://github.com/ClickHouse/ClickHouse/pull/7750) ([Amos Bird](https://github.com/amosbird))
* Optimized `ORDER BY` and merging with single column key. [#8335](https://github.com/ClickHouse/ClickHouse/pull/8335) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Better implementation for `arrayReduce`, `-Array` and `-State` combinators.  [#7710](https://github.com/ClickHouse/ClickHouse/pull/7710) ([Amos Bird](https://github.com/amosbird))
* Now `PREWHERE` should be optimized to be at least as efficient as `WHERE`. [#7769](https://github.com/ClickHouse/ClickHouse/pull/7769) ([Amos Bird](https://github.com/amosbird))
* Improve the way `round` and `roundBankers` handling negative numbers. [#8229](https://github.com/ClickHouse/ClickHouse/pull/8229) ([hcz](https://github.com/hczhcz))
* Improved decoding performance of `DoubleDelta` and `Gorilla` codecs by roughly 30-40%. This fixes [#7082](https://github.com/ClickHouse/ClickHouse/issues/7082). [#8019](https://github.com/ClickHouse/ClickHouse/pull/8019) ([Vasily Nemkov](https://github.com/Enmk))
* Improved performance of `base64` related functions. [#8444](https://github.com/ClickHouse/ClickHouse/pull/8444) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Added a function `geoDistance`. It is similar to `greatCircleDistance` but uses approximation to WGS-84 ellipsoid model. The performance of both functions are near the same. [#8086](https://github.com/ClickHouse/ClickHouse/pull/8086) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Faster `min` and `max` aggregation functions for `Decimal` data type. [#8144](https://github.com/ClickHouse/ClickHouse/pull/8144) ([Artem Zuikov](https://github.com/4ertus2))
* Vectorize processing `arrayReduce`. [#7608](https://github.com/ClickHouse/ClickHouse/pull/7608) ([Amos Bird](https://github.com/amosbird))
* `if` chains are now optimized as `multiIf`. [#8355](https://github.com/ClickHouse/ClickHouse/pull/8355) ([kamalov-ruslan](https://github.com/kamalov-ruslan))
* Fix performance regression of `Kafka` table engine introduced in 19.15. This fixes [#7261](https://github.com/ClickHouse/ClickHouse/issues/7261). [#7935](https://github.com/ClickHouse/ClickHouse/pull/7935) ([filimonov](https://github.com/filimonov))
* Removed "pie" code generation that `gcc` from Debian packages occasionally brings by default. [#8483](https://github.com/ClickHouse/ClickHouse/pull/8483) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Parallel parsing data formats [#6553](https://github.com/ClickHouse/ClickHouse/pull/6553) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov))
* Enable optimized parser of `Values` with expressions by default (`input_format_values_deduce_templates_of_expressions=1`). [#8231](https://github.com/ClickHouse/ClickHouse/pull/8231) ([tavplubix](https://github.com/tavplubix))

#### Build/Testing/Packaging Improvement
* Build fixes for `ARM` and in minimal mode. [#8304](https://github.com/ClickHouse/ClickHouse/pull/8304) ([proller](https://github.com/proller))
* Add coverage file flush for `clickhouse-server` when std::atexit is not called. Also slightly improved logging in stateless tests with coverage. [#8267](https://github.com/ClickHouse/ClickHouse/pull/8267) ([alesapin](https://github.com/alesapin))
* Update LLVM library in contrib. Avoid using LLVM from OS packages. [#8258](https://github.com/ClickHouse/ClickHouse/pull/8258) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Make bundled `curl` build fully quiet. [#8232](https://github.com/ClickHouse/ClickHouse/pull/8232) [#8203](https://github.com/ClickHouse/ClickHouse/pull/8203) ([Pavel Kovalenko](https://github.com/Jokser))
* Fix some `MemorySanitizer` warnings. [#8235](https://github.com/ClickHouse/ClickHouse/pull/8235) ([Alexander Kuzmenkov](https://github.com/akuzm))
* Use `add_warning` and `no_warning` macros in `CMakeLists.txt`. [#8604](https://github.com/ClickHouse/ClickHouse/pull/8604) ([Ivan](https://github.com/abyss7))
* Add support of Minio S3 Compatible object (https://min.io/) for better integration tests. [#7863](https://github.com/ClickHouse/ClickHouse/pull/7863) [#7875](https://github.com/ClickHouse/ClickHouse/pull/7875) ([Pavel Kovalenko](https://github.com/Jokser))
* Imported `libc` headers to contrib. It allows to make builds more consistent across various systems (only for `x86_64-linux-gnu`). [#5773](https://github.com/ClickHouse/ClickHouse/pull/5773) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Remove `-fPIC` from some libraries. [#8464](https://github.com/ClickHouse/ClickHouse/pull/8464) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Clean `CMakeLists.txt` for curl. See https://github.com/ClickHouse/ClickHouse/pull/8011#issuecomment-569478910 [#8459](https://github.com/ClickHouse/ClickHouse/pull/8459) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Silent warnings in `CapNProto` library. [#8220](https://github.com/ClickHouse/ClickHouse/pull/8220) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Add performance tests for short string optimized hash tables. [#7679](https://github.com/ClickHouse/ClickHouse/pull/7679) ([Amos Bird](https://github.com/amosbird))
* Now ClickHouse will build on `AArch64` even if `MADV_FREE` is not available. This fixes [#8027](https://github.com/ClickHouse/ClickHouse/issues/8027). [#8243](https://github.com/ClickHouse/ClickHouse/pull/8243) ([Amos Bird](https://github.com/amosbird))
* Update `zlib-ng` to fix memory sanitizer problems. [#7182](https://github.com/ClickHouse/ClickHouse/pull/7182) [#8206](https://github.com/ClickHouse/ClickHouse/pull/8206) ([Alexander Kuzmenkov](https://github.com/akuzm))
* Enable internal MySQL library on non-Linux system, because usage of OS packages is very fragile and usually doesn't work at all. This fixes [#5765](https://github.com/ClickHouse/ClickHouse/issues/5765). [#8426](https://github.com/ClickHouse/ClickHouse/pull/8426) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed build on some systems after enabling `libc++`. This supersedes [#8374](https://github.com/ClickHouse/ClickHouse/issues/8374). [#8380](https://github.com/ClickHouse/ClickHouse/pull/8380) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Make `Field` methods more type-safe to find more errors. [#7386](https://github.com/ClickHouse/ClickHouse/pull/7386) [#8209](https://github.com/ClickHouse/ClickHouse/pull/8209) ([Alexander Kuzmenkov](https://github.com/akuzm))
* Added missing files to the `libc-headers` submodule. [#8507](https://github.com/ClickHouse/ClickHouse/pull/8507) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix wrong `JSON` quoting in performance test output. [#8497](https://github.com/ClickHouse/ClickHouse/pull/8497) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Now stack trace is displayed for `std::exception` and `Poco::Exception`. In previous versions it was available only for `DB::Exception`. This improves diagnostics. [#8501](https://github.com/ClickHouse/ClickHouse/pull/8501) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Porting `clock_gettime` and `clock_nanosleep` for fresh glibc versions. [#8054](https://github.com/ClickHouse/ClickHouse/pull/8054) ([Amos Bird](https://github.com/amosbird))
* Enable `part_log` in example config for developers. [#8609](https://github.com/ClickHouse/ClickHouse/pull/8609) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix async nature of reload in `01036_no_superfluous_dict_reload_on_create_database*`. [#8111](https://github.com/ClickHouse/ClickHouse/pull/8111) ([Azat Khuzhin](https://github.com/azat))
* Fixed codec performance tests. [#8615](https://github.com/ClickHouse/ClickHouse/pull/8615) ([Vasily Nemkov](https://github.com/Enmk))
* Add install scripts for `.tgz` build and documentation for them. [#8612](https://github.com/ClickHouse/ClickHouse/pull/8612) [#8591](https://github.com/ClickHouse/ClickHouse/pull/8591) ([alesapin](https://github.com/alesapin))
* Removed old `ZSTD` test (it was created in year 2016 to reproduce the bug that pre 1.0 version of ZSTD has had). This fixes [#8618](https://github.com/ClickHouse/ClickHouse/issues/8618). [#8619](https://github.com/ClickHouse/ClickHouse/pull/8619) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed build on Mac OS Catalina. [#8600](https://github.com/ClickHouse/ClickHouse/pull/8600) ([meo](https://github.com/meob))
* Increased number of rows in codec performance tests to make results noticeable. [#8574](https://github.com/ClickHouse/ClickHouse/pull/8574) ([Vasily Nemkov](https://github.com/Enmk))
* In debug builds, treat `LOGICAL_ERROR` exceptions as assertion failures, so that they are easier to notice. [#8475](https://github.com/ClickHouse/ClickHouse/pull/8475) ([Alexander Kuzmenkov](https://github.com/akuzm))
* Make formats-related performance test more deterministic. [#8477](https://github.com/ClickHouse/ClickHouse/pull/8477) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Update `lz4` to fix a MemorySanitizer failure. [#8181](https://github.com/ClickHouse/ClickHouse/pull/8181) ([Alexander Kuzmenkov](https://github.com/akuzm))
* Suppress a known MemorySanitizer false positive in exception handling. [#8182](https://github.com/ClickHouse/ClickHouse/pull/8182) ([Alexander Kuzmenkov](https://github.com/akuzm))
* Update `gcc` and `g++` to version 9 in `build/docker/build.sh` [#7766](https://github.com/ClickHouse/ClickHouse/pull/7766) ([TLightSky](https://github.com/tlightsky))
* Add performance test case to test that `PREWHERE` is worse than `WHERE`.  [#7768](https://github.com/ClickHouse/ClickHouse/pull/7768) ([Amos Bird](https://github.com/amosbird))
* Progress towards fixing one flacky test. [#8621](https://github.com/ClickHouse/ClickHouse/pull/8621) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Avoid MemorySanitizer report for data from `libunwind`. [#8539](https://github.com/ClickHouse/ClickHouse/pull/8539) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Updated `libc++` to the latest version. [#8324](https://github.com/ClickHouse/ClickHouse/pull/8324) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Build ICU library from sources. This fixes [#6460](https://github.com/ClickHouse/ClickHouse/issues/6460). [#8219](https://github.com/ClickHouse/ClickHouse/pull/8219) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Switched from `libressl` to `openssl`. ClickHouse should support TLS 1.3 and SNI after this change. This fixes [#8171](https://github.com/ClickHouse/ClickHouse/issues/8171). [#8218](https://github.com/ClickHouse/ClickHouse/pull/8218) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fixed UBSan report when using `chacha20_poly1305` from SSL (happens on connect to https://yandex.ru/). [#8214](https://github.com/ClickHouse/ClickHouse/pull/8214) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix mode of default password file for `.deb` linux distros. [#8075](https://github.com/ClickHouse/ClickHouse/pull/8075) ([proller](https://github.com/proller))
* Improved expression for getting `clickhouse-server` PID in `clickhouse-test`. [#8063](https://github.com/ClickHouse/ClickHouse/pull/8063) ([Alexander Kazakov](https://github.com/Akazz))
* Updated contrib/googletest to v1.10.0. [#8587](https://github.com/ClickHouse/ClickHouse/pull/8587) ([Alexander Burmak](https://github.com/Alex-Burmak))
* Fixed ThreadSaninitizer report in `base64` library. Also updated this library to the latest version, but it doesn't matter. This fixes [#8397](https://github.com/ClickHouse/ClickHouse/issues/8397). [#8403](https://github.com/ClickHouse/ClickHouse/pull/8403) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Fix `00600_replace_running_query` for processors. [#8272](https://github.com/ClickHouse/ClickHouse/pull/8272) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Remove support for `tcmalloc` to make `CMakeLists.txt` simpler. [#8310](https://github.com/ClickHouse/ClickHouse/pull/8310) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Release gcc builds now use `libc++` instead of `libstdc++`. Recently `libc++` was used only with clang. This will improve consistency of build configurations and portability. [#8311](https://github.com/ClickHouse/ClickHouse/pull/8311) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Enable ICU library for build with MemorySanitizer. [#8222](https://github.com/ClickHouse/ClickHouse/pull/8222) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Suppress warnings from `CapNProto` library. [#8224](https://github.com/ClickHouse/ClickHouse/pull/8224) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Removed special cases of code for `tcmalloc`, because it's no longer supported. [#8225](https://github.com/ClickHouse/ClickHouse/pull/8225) ([alexey-milovidov](https://github.com/alexey-milovidov))
* In CI coverage task, kill the server gracefully to allow it to save the coverage report. This fixes incomplete coverage reports we've been seeing lately. [#8142](https://github.com/ClickHouse/ClickHouse/pull/8142) ([alesapin](https://github.com/alesapin))
* Performance tests for all codecs against `Float64` and `UInt64` values. [#8349](https://github.com/ClickHouse/ClickHouse/pull/8349) ([Vasily Nemkov](https://github.com/Enmk))
* `termcap` is very much deprecated and lead to various problems (f.g. missing "up" cap and echoing `^J` instead of multi line) . Favor `terminfo` or bundled `ncurses`. [#7737](https://github.com/ClickHouse/ClickHouse/pull/7737) ([Amos Bird](https://github.com/amosbird))
* Fix `test_storage_s3` integration test. [#7734](https://github.com/ClickHouse/ClickHouse/pull/7734) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Support `StorageFile(<format>, null) ` to insert block into given format file without actually write to disk. This is required for performance tests. [#8455](https://github.com/ClickHouse/ClickHouse/pull/8455) ([Amos Bird](https://github.com/amosbird))
* Added argument `--print-time` to functional tests which prints execution time per test. [#8001](https://github.com/ClickHouse/ClickHouse/pull/8001) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Added asserts to `KeyCondition` while evaluating RPN. This will fix warning from gcc-9. [#8279](https://github.com/ClickHouse/ClickHouse/pull/8279) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Dump cmake options in CI builds. [#8273](https://github.com/ClickHouse/ClickHouse/pull/8273) ([Alexander Kuzmenkov](https://github.com/akuzm))
* Don't generate debug info for some fat libraries. [#8271](https://github.com/ClickHouse/ClickHouse/pull/8271) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Make `log_to_console.xml` always log to stderr, regardless of is it interactive or not. [#8395](https://github.com/ClickHouse/ClickHouse/pull/8395) ([Alexander Kuzmenkov](https://github.com/akuzm))
* Removed some unused features from `clickhouse-performance-test` tool. [#8555](https://github.com/ClickHouse/ClickHouse/pull/8555) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Now we will also search for `lld-X` with corresponding `clang-X` version. [#8092](https://github.com/ClickHouse/ClickHouse/pull/8092) ([alesapin](https://github.com/alesapin))
* Parquet build improvement. [#8421](https://github.com/ClickHouse/ClickHouse/pull/8421) ([maxulan](https://github.com/maxulan))
* More GCC warnings [#8221](https://github.com/ClickHouse/ClickHouse/pull/8221) ([kreuzerkrieg](https://github.com/kreuzerkrieg))
* Package for Arch Linux now allows to run ClickHouse server, and not only client. [#8534](https://github.com/ClickHouse/ClickHouse/pull/8534) ([Vladimir Chebotarev](https://github.com/excitoon))
* Fix test with processors. Tiny performance fixes. [#7672](https://github.com/ClickHouse/ClickHouse/pull/7672) ([Nikolai Kochetov](https://github.com/KochetovNicolai))
* Update contrib/protobuf. [#8256](https://github.com/ClickHouse/ClickHouse/pull/8256) ([Matwey V. Kornilov](https://github.com/matwey))
* In preparation of switching to c++20 as a new year celebration. "May the C++ force be with ClickHouse." [#8447](https://github.com/ClickHouse/ClickHouse/pull/8447) ([Amos Bird](https://github.com/amosbird))

#### Experimental Feature
* Added experimental setting `min_bytes_to_use_mmap_io`. It allows to read big files without copying data from kernel to userspace. The setting is disabled by default. Recommended threshold is about 64 MB, because mmap/munmap is slow. [#8520](https://github.com/ClickHouse/ClickHouse/pull/8520) ([alexey-milovidov](https://github.com/alexey-milovidov))
* Reworked quotas as a part of access control system. Added new table `system.quotas`, new functions `currentQuota`, `currentQuotaKey`, new SQL syntax `CREATE QUOTA`, `ALTER QUOTA`, `DROP QUOTA`, `SHOW QUOTA`. [#7257](https://github.com/ClickHouse/ClickHouse/pull/7257) ([Vitaly Baranov](https://github.com/vitlibar))
* Allow skipping unknown settings with warnings instead of throwing exceptions. [#7653](https://github.com/ClickHouse/ClickHouse/pull/7653) ([Vitaly Baranov](https://github.com/vitlibar))
* Reworked row policies as a part of access control system. Added new table `system.row_policies`, new function `currentRowPolicies()`, new SQL syntax `CREATE POLICY`, `ALTER POLICY`, `DROP POLICY`, `SHOW CREATE POLICY`, `SHOW POLICIES`. [#7808](https://github.com/ClickHouse/ClickHouse/pull/7808) ([Vitaly Baranov](https://github.com/vitlibar))

#### Security Fix
* Fixed the possibility of reading directories structure in tables with `File` table engine. This fixes [#8536](https://github.com/ClickHouse/ClickHouse/issues/8536). [#8537](https://github.com/ClickHouse/ClickHouse/pull/8537) ([alexey-milovidov](https://github.com/alexey-milovidov))

## [Changelog for 2019](https://github.com/ClickHouse/ClickHouse/blob/master/docs/en/whats-new/changelog/2019.md)
