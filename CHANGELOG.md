### Table of Contents
**[ClickHouse release v23.1, 2023-01-25](#231)**<br/>
**[Changelog for 2022](https://clickhouse.com/docs/en/whats-new/changelog/2022/)**<br/>

# 2023 Changelog

### <a id="231"></a> ClickHouse release 23.1, 2023-01-25

# 2023 Changelog

### ClickHouse release master (2f1092e6d24) FIXME as compared to v22.12.1.1752-stable (688e488e930)

#### Upgrade Notes
* The `SYSTEM RESTART DISK` query becomes a no-op. [#44647](https://github.com/ClickHouse/ClickHouse/pull/44647) ([alesapin](https://github.com/alesapin)).
* The `PREALLOCATE` option for `HASHED`/`SPARSE_HASHED` dictionaries becomes a no-op. [#45388](https://github.com/ClickHouse/ClickHouse/pull/45388) ([Azat Khuzhin](https://github.com/azat)). It does not give significant advantages anymore.
* Disallow `Gorilla` codec on columns of non-Float32 or non-Float64 type. [#45252](https://github.com/ClickHouse/ClickHouse/pull/45252) ([Robert Schulze](https://github.com/rschu1ze)). It was pointless and led to inconsistencies. 
* Parallel quorum inserts might work incorrectly with `*MergeTree` tables created with the deprecated syntax. Therefore, parallel quorum inserts support is completely disabled for such tables. It does not affect tables created with a new syntax. [#45430](https://github.com/ClickHouse/ClickHouse/pull/45430) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Use `GetObjectAttributes` request instead of `HeadObject` request to get the size of an object in AWS S3. This change fixes handling endpoints without explicit region after updating the AWS SDK, for example. [#45288](https://github.com/ClickHouse/ClickHouse/pull/45288) ([Vitaly Baranov](https://github.com/vitlibar)). AWS S3 and Minio are tested, but keep in mind that various S3-compatible services (GCS, R2, B2) may have subtle incompatibilities.

#### New Feature
* Dictionary source for extracting keys by traversing regular expressions tree. It can be used for User-Agent parsing. [#40878](https://github.com/ClickHouse/ClickHouse/pull/40878) ([Vage Ogannisian](https://github.com/nooblose)). [#43858](https://github.com/ClickHouse/ClickHouse/pull/43858) ([Han Fei](https://github.com/hanfei1991)).
* Added parametrized view functionality, now it's possible to specify query parameters for View table engine. resolves [#40907](https://github.com/ClickHouse/ClickHouse/issues/40907). [#41687](https://github.com/ClickHouse/ClickHouse/pull/41687) ([SmitaRKulkarni](https://github.com/SmitaRKulkarni)).
* Add `quantileInterpolatedWeighted`/`quantilesInterpolatedWeighted` functions. [#38252](https://github.com/ClickHouse/ClickHouse/pull/38252) ([Bharat Nallan](https://github.com/bharatnc)).
* Add column `ptr` to `system.trace_log` for `trace_type = 'MemorySample'`. This column contains an address of allocation. Added function `flameGraph` which can build flamegraph containing allocated and not released memory. Reworking of [#38391](https://github.com/ClickHouse/ClickHouse/issues/38391). [#38953](https://github.com/ClickHouse/ClickHouse/pull/38953) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Array join support Map type, like function explode in Spark. [#43239](https://github.com/ClickHouse/ClickHouse/pull/43239) ([李扬](https://github.com/taiyang-li)).
* Support SQL standard binary and hex string literals. [#43785](https://github.com/ClickHouse/ClickHouse/pull/43785) ([Mo Xuan](https://github.com/mo-avatar)).
* Allow to format DateTime in Joda-Time style. Refer to [the Joda-Time docs](https://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html). [#43818](https://github.com/ClickHouse/ClickHouse/pull/43818) ([李扬](https://github.com/taiyang-li)).
* Implemented a fractional second formatter (`%f`) for `formatDateTime`. [#44060](https://github.com/ClickHouse/ClickHouse/pull/44060) ([ltrk2](https://github.com/ltrk2)). [#44497](https://github.com/ClickHouse/ClickHouse/pull/44497) ([Alexander Gololobov](https://github.com/davenger)).
* Added age function to calculate difference between two dates or dates with time values expressed as number of full units. Closes [#41115](https://github.com/ClickHouse/ClickHouse/issues/41115). [#44421](https://github.com/ClickHouse/ClickHouse/pull/44421) ([Robert Schulze](https://github.com/rschu1ze)).
* Add `Null` source for dictionaries. Closes [#44240](https://github.com/ClickHouse/ClickHouse/issues/44240). [#44502](https://github.com/ClickHouse/ClickHouse/pull/44502) ([mayamika](https://github.com/mayamika)).
* We can use `s3_storage_class` to set different tiers. Such as `<s3_storage_class>STANDARD/INTELLIGENT_TIERING</s3_storage_class>` Closes [#44443](https://github.com/ClickHouse/ClickHouse/issues/44443). [#44707](https://github.com/ClickHouse/ClickHouse/pull/44707) ([chen](https://github.com/xiedeyantu)).
* Insert default values in case of missing elements in JSON object while parsing named tuple. Add setting `input_format_json_defaults_for_missing_elements_in_named_tuple` that controls this behaviour. Closes [#45142](https://github.com/ClickHouse/ClickHouse/issues/45142)#issuecomment-1380153217. [#45231](https://github.com/ClickHouse/ClickHouse/pull/45231) ([Kruglov Pavel](https://github.com/Avogar)).
* Record server startup time in ProfileEvents resolves [#43188](https://github.com/ClickHouse/ClickHouse/issues/43188) Implementation: * Added ProfileEvents::ServerStartupMilliseconds. * Recorded time from start of main till listening to sockets. Testing: * Added a test 02532_profileevents_server_startup_time.sql. [#45250](https://github.com/ClickHouse/ClickHouse/pull/45250) ([SmitaRKulkarni](https://github.com/SmitaRKulkarni)).
* Refactor and Improve streaming engines Kafka/RabbitMQ/NATS and add support for all formats, also refactor formats a bit: - Fix producing messages in row-based formats with suffixes/prefixes. Now every message is formatted complitely with all delimiters and can be parsed back using input format. - Support block-based formats like Native, Parquet, ORC, etc. Every block is formatted as a separated message. The number of rows in one message depends on block size, so you can control it via setting `max_block_size`. - Add new engine settings `kafka_max_rows_per_message/rabbitmq_max_rows_per_message/nats_max_rows_per_message`. They control the number of rows formatted in one message in row-based formats. Default value: 1. - Fix high memory consumption in NATS table engine. - Support arbitrary binary data in NATS producer (previously it worked only with strings contained \0 at the end) - Add missing Kafka/RabbitMQ/NATS engine settings in documentation. - Refactor producing and consuming in Kafka/RabbitMQ/NATS, separate it from WriteBuffers/ReadBuffers semantic. - Refactor output formats: remove callbacks on each row used in Kafka/RabbitMQ/NATS (now we don't use callbacks there), allow to use IRowOutputFormat directly, clarify row end and row between delimiters, make it possible to reset output format to start formatting again - Add proper implementation in formatRow function (bonus after formats refactoring). [#42777](https://github.com/ClickHouse/ClickHouse/pull/42777) ([Kruglov Pavel](https://github.com/Avogar)).
* Support reading/writing `Nested` tables as `List` of `Struct` in CapnProto format. Read/write `Decimal32/64` as `Int32/64`. Closes [#43319](https://github.com/ClickHouse/ClickHouse/issues/43319). [#43379](https://github.com/ClickHouse/ClickHouse/pull/43379) ([Kruglov Pavel](https://github.com/Avogar)).
* Added a `message_format_string` column to `system.text_log`. The column contains a pattern that was used to format the message. [#44543](https://github.com/ClickHouse/ClickHouse/pull/44543) ([Alexander Tokmakov](https://github.com/tavplubix)). This allows various analytics over ClickHouse own logs.

#### Experimental Feature
* Add an experimental inverted index as a new secondary index type for efficient text search. [#38667](https://github.com/ClickHouse/ClickHouse/pull/38667) ([larryluogit](https://github.com/larryluogit)).
* Add experimental query result cache. [#43797](https://github.com/ClickHouse/ClickHouse/pull/43797) ([Robert Schulze](https://github.com/rschu1ze)).
* Added extendable and configurable scheduling subsystem for IO requests (not yet integrated with IO code itself). [#41840](https://github.com/ClickHouse/ClickHouse/pull/41840) ([Sergei Trifonov](https://github.com/serxa)).
* Added `SYSTEM DROP DATABASE REPLICA` that removes metadata of dead replica of `Replicated` database. Resolves [#41794](https://github.com/ClickHouse/ClickHouse/issues/41794). [#42807](https://github.com/ClickHouse/ClickHouse/pull/42807) ([Alexander Tokmakov](https://github.com/tavplubix)).

#### Performance Improvement
* Do not load inactive parts at startup of `MergeTree` tables. [#42181](https://github.com/ClickHouse/ClickHouse/pull/42181) ([Anton Popov](https://github.com/CurtizJ)).
* Improved latency of reading from storage `S3` and table function `s3` with large number of small files. Now settings `remote_filesystem_read_method` and `remote_filesystem_read_prefetch` take effect while reading from storage `S3`. [#43726](https://github.com/ClickHouse/ClickHouse/pull/43726) ([Anton Popov](https://github.com/CurtizJ)).
* Optimization for reading struct fields in Parquet/ORC files. Only the required fields are loaded. [#44484](https://github.com/ClickHouse/ClickHouse/pull/44484) ([lgbo](https://github.com/lgbo-ustc)).
* Two-level aggregation algorithm was mistakenly disabled for queries over HTTP interface. It was enabled back, and it leads to a major performance improvement. [#45450](https://github.com/ClickHouse/ClickHouse/pull/45450) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Added mmap support for StorageFile, which should improve the performance of clickhouse-local. [#43927](https://github.com/ClickHouse/ClickHouse/pull/43927) ([pufit](https://github.com/pufit)).
* Added sharding support in HashedDictionary to allow parallel load (almost linear scaling based on number of shards). [#40003](https://github.com/ClickHouse/ClickHouse/pull/40003) ([Azat Khuzhin](https://github.com/azat)).
* Speed up query parsing. [#42284](https://github.com/ClickHouse/ClickHouse/pull/42284) ([Raúl Marín](https://github.com/Algunenano)).
* Always replace OR chain `expr = x1 OR ... OR expr = xN` to `expr IN (x1, ..., xN)` in case if `expr` is a `LowCardinality` column. Setting `optimize_min_equality_disjunction_chain_length` is ignored in this case. [#42889](https://github.com/ClickHouse/ClickHouse/pull/42889) ([Guo Wangyang](https://github.com/guowangy)).
* Original changelog In the original implementation, the memory of ThreadGroupStatus:: finished_threads_counters_memory is released by moving it to a temporary std::vector, which soon expired and gets destructed. This method is viable, however not straightforward enough. To enhance the code readability, this commit releases the memory in the vector by firstly resizing it to 0 and then shrinking the capacity accordingly. [#43586](https://github.com/ClickHouse/ClickHouse/pull/43586) ([Zhiguo Zhou](https://github.com/ZhiguoZh)).
* As a follow-up of [#42214](https://github.com/ClickHouse/ClickHouse/issues/42214), this PR tries to optimize the column-wise ternary logic evaluation by achieving auto-vectorization. In the performance test of this [microbenchmark](https://github.com/ZhiguoZh/ClickHouse/blob/20221123-ternary-logic-opt-example/src/Functions/examples/associative_applier_perf.cpp), we've observed a peak **performance gain** of **21x** on the ICX device (Intel Xeon Platinum 8380 CPU). [#43669](https://github.com/ClickHouse/ClickHouse/pull/43669) ([Zhiguo Zhou](https://github.com/ZhiguoZh)).
* Avoid acquiring read locks in the `system.tables` table if possible. [#43840](https://github.com/ClickHouse/ClickHouse/pull/43840) ([Raúl Marín](https://github.com/Algunenano)).
* The performance experiments of SSB (Star Schema Benchmark) on the ICX device (Intel Xeon Platinum 8380 CPU, 80 cores, 160 threads) shows that this change could effectively decrease the lock contention for ThreadPoolImpl::mutex by **75%**, increasing the CPU utilization and improving the overall performance by **2.4%**. [#44308](https://github.com/ClickHouse/ClickHouse/pull/44308) ([Zhiguo Zhou](https://github.com/ZhiguoZh)).
* Now optimisation for predicting the hash table size is applied only if the cached hash table size is sufficiently large (thresholds were determined empirically and hardcoded). [#44455](https://github.com/ClickHouse/ClickHouse/pull/44455) ([Nikita Taranov](https://github.com/nickitat)).
* Small performance improvement for asynchronous reading from remote filesystems. [#44868](https://github.com/ClickHouse/ClickHouse/pull/44868) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Add fast path for: - `col like '%%'`; - `col like '%'`; - `col not like '%'`; - `col not like '%'`; - `match(col, '.*')`. [#45244](https://github.com/ClickHouse/ClickHouse/pull/45244) ([李扬](https://github.com/taiyang-li)).
* Slightly improve happy path optimisation in filtering (WHERE clause). [#45289](https://github.com/ClickHouse/ClickHouse/pull/45289) ([Nikita Taranov](https://github.com/nickitat)).
* Provide monotonicity info for `toUnixTimestamp64*` to enable more algebraic optimizations for index analysis. [#44116](https://github.com/ClickHouse/ClickHouse/pull/44116) ([Nikita Taranov](https://github.com/nickitat)).
* Allow to configure temporary data for query processing (spilling to disk) to cooperate with filesystem cache (taking up the space from the cache disk) [#43972](https://github.com/ClickHouse/ClickHouse/pull/43972) ([Vladimir C](https://github.com/vdimir)). This mainly improves [ClickHouse Cloud](https://clickhouse.cloud/), but can be used for self-managed setups as well, if you know what to do.
* Make `system.replicas` table do parallel fetches of replicas statuses. Closes [#43918](https://github.com/ClickHouse/ClickHouse/issues/43918). [#43998](https://github.com/ClickHouse/ClickHouse/pull/43998) ([Nikolay Degterinsky](https://github.com/evillique)).
* Optimize memory consumption during backup to S3: files to S3 now will be copied directly without using `WriteBufferFromS3` (which could use a lot of memory). [#45188](https://github.com/ClickHouse/ClickHouse/pull/45188) ([Vitaly Baranov](https://github.com/vitlibar)).

#### Improvement

* Improve the Asterisk and ColumnMatcher parsers. Part of [#42648](https://github.com/ClickHouse/ClickHouse/issues/42648). [#42884](https://github.com/ClickHouse/ClickHouse/pull/42884) ([Nikolay Degterinsky](https://github.com/evillique)).
* Improve reading CSV field in CustomSeparated/Template format. Closes [#42352](https://github.com/ClickHouse/ClickHouse/issues/42352) Closes [#39620](https://github.com/ClickHouse/ClickHouse/issues/39620). [#43332](https://github.com/ClickHouse/ClickHouse/pull/43332) ([Kruglov Pavel](https://github.com/Avogar)).
* Unify query elapsed time measurements. [#43455](https://github.com/ClickHouse/ClickHouse/pull/43455) ([Raúl Marín](https://github.com/Algunenano)).
* Improve automatic usage of structure from insertion table in table functions file/hdfs/s3 when virtual columns present in select query, it fixes possible error `Block structure mismatch` or `number of columns mismatch`. [#43695](https://github.com/ClickHouse/ClickHouse/pull/43695) ([Kruglov Pavel](https://github.com/Avogar)).
* Add support for signed arguments in range(). Fixes [#43333](https://github.com/ClickHouse/ClickHouse/issues/43333). [#43733](https://github.com/ClickHouse/ClickHouse/pull/43733) ([sanyu](https://github.com/wineternity)).
* Remove redundant sorting, for example, sorting related ORDER BY clauses in subqueries. Implemented on top of query plan. It does similar optimization as `optimize_duplicate_order_by_and_distinct` regarding `ORDER BY` clauses, but more generic, since it's applied to any redundant sorting steps (not only caused by ORDER BY clause) and applied to subqueries of any depth. Related to [#42648](https://github.com/ClickHouse/ClickHouse/issues/42648). [#43905](https://github.com/ClickHouse/ClickHouse/pull/43905) ([Igor Nikonov](https://github.com/devcrafter)).
* Add ability to disable deduplication for BACKUP (for backups wiithout deduplication ATTACH can be used instead of full RESTORE), example `BACKUP foo TO S3(...) SETTINGS deduplicate_files=0` (default `deduplicate_files=1`). [#43947](https://github.com/ClickHouse/ClickHouse/pull/43947) ([Azat Khuzhin](https://github.com/azat)).
* Refactor and improve schema inference for text formats. Add new setting `schema_inference_make_columns_nullable` that controls making result types `Nullable` (enabled by default);. [#44019](https://github.com/ClickHouse/ClickHouse/pull/44019) ([Kruglov Pavel](https://github.com/Avogar)).
* Better support for PROXYv1. [#44135](https://github.com/ClickHouse/ClickHouse/pull/44135) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Add information about the latest part check by cleanup thread into `system.parts` table. [#44244](https://github.com/ClickHouse/ClickHouse/pull/44244) ([Dmitry Novik](https://github.com/novikd)).
* Disable table functions in readonly for inserts. [#44290](https://github.com/ClickHouse/ClickHouse/pull/44290) ([SmitaRKulkarni](https://github.com/SmitaRKulkarni)).
* Add a setting `simultaneous_parts_removal_limit` to allow to limit the number of parts being processed by one iteration of CleanupThread. [#44461](https://github.com/ClickHouse/ClickHouse/pull/44461) ([Dmitry Novik](https://github.com/novikd)).
* If user only need virtual columns, we don't need to initialize ReadBufferFromS3. May be helpful to [#44246](https://github.com/ClickHouse/ClickHouse/issues/44246). [#44493](https://github.com/ClickHouse/ClickHouse/pull/44493) ([chen](https://github.com/xiedeyantu)).
* Prevent duplicate column names hints. Closes [#44130](https://github.com/ClickHouse/ClickHouse/issues/44130). [#44519](https://github.com/ClickHouse/ClickHouse/pull/44519) ([Joanna Hulboj](https://github.com/jh0x)).
* Allow macro substitution in endpoint of disks resolve [#40951](https://github.com/ClickHouse/ClickHouse/issues/40951). [#44533](https://github.com/ClickHouse/ClickHouse/pull/44533) ([SmitaRKulkarni](https://github.com/SmitaRKulkarni)).
* Improve schema inference when `input_format_json_read_object_as_string` is enabled. [#44546](https://github.com/ClickHouse/ClickHouse/pull/44546) ([Kruglov Pavel](https://github.com/Avogar)).
* Add user-level setting `database_replicated_allow_replicated_engine_arguments` which allow to ban creation of `ReplicatedMergeTree` tables with arguments in `DatabaseReplicated`. [#44566](https://github.com/ClickHouse/ClickHouse/pull/44566) ([alesapin](https://github.com/alesapin)).
* Prevent users from mistakenly specifying zero (invalid) value for `index_granularity`. This closes [#44536](https://github.com/ClickHouse/ClickHouse/issues/44536). [#44578](https://github.com/ClickHouse/ClickHouse/pull/44578) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Added possibility to set path to service keytab file in `keytab` parameter in `kerberos` section of config.xml. [#44594](https://github.com/ClickHouse/ClickHouse/pull/44594) ([Roman Vasin](https://github.com/rvasin)).
* Use already written part of the query for fuzzy search (pass to skim). [#44600](https://github.com/ClickHouse/ClickHouse/pull/44600) ([Azat Khuzhin](https://github.com/azat)).
* Enable `input_format_json_read_objects_as_strings` by default to be able to read nested JSON objects while JSON Object type is experimental. [#44657](https://github.com/ClickHouse/ClickHouse/pull/44657) ([Kruglov Pavel](https://github.com/Avogar)).
* When users do duplicate async inserts, we should dedup inside the memory before we query keeper. [#44682](https://github.com/ClickHouse/ClickHouse/pull/44682) ([Han Fei](https://github.com/hanfei1991)).
* Input/ouptut `Avro` bool type as ClickHouse bool type. [#44684](https://github.com/ClickHouse/ClickHouse/pull/44684) ([Kruglov Pavel](https://github.com/Avogar)).
* Don't greedy parse beyond the quotes when reading UUIDs - it may lead to mistakenly successful parsing of incorrect data. [#44686](https://github.com/ClickHouse/ClickHouse/pull/44686) ([Raúl Marín](https://github.com/Algunenano)).
* Infer UInt64 in case of Int64 overflow and fix some transforms in schema inference. [#44696](https://github.com/ClickHouse/ClickHouse/pull/44696) ([Kruglov Pavel](https://github.com/Avogar)).
* Previously dependency resolving inside DatabaseReplicated was done in a hacky way, and now it's done right using an explicit graph. [#44697](https://github.com/ClickHouse/ClickHouse/pull/44697) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Support Bool type in Arrow/Parquet/ORC. Closes [#43970](https://github.com/ClickHouse/ClickHouse/issues/43970). [#44698](https://github.com/ClickHouse/ClickHouse/pull/44698) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix `output_format_pretty_row_numbers` does not preserve the counter across the blocks. Closes [#44815](https://github.com/ClickHouse/ClickHouse/issues/44815). [#44832](https://github.com/ClickHouse/ClickHouse/pull/44832) ([flynn](https://github.com/ucasfl)).
* Extend function "toDayOfWeek" with a mode argument describing if a) the week starts on Monday or Sunday and b) if counting starts at 0 or 1. [#44860](https://github.com/ClickHouse/ClickHouse/pull/44860) ([李扬](https://github.com/taiyang-li)).
* Don't report errors in `system.errors` due to parts being merged concurrently with the background cleanup process. [#44874](https://github.com/ClickHouse/ClickHouse/pull/44874) ([Raúl Marín](https://github.com/Algunenano)).
* Optimize and fix metrics for Distributed async INSERT. [#44922](https://github.com/ClickHouse/ClickHouse/pull/44922) ([Azat Khuzhin](https://github.com/azat)).
* Added settings to disallow concurrent backups and restores resolves [#43891](https://github.com/ClickHouse/ClickHouse/issues/43891) Implementation: * Added server level settings to disallow concurrent backups and restores, which are read and set when BackupWorker is created in Context. * Settings are set to true by default. * Before starting backup or restores, added a check to see if any other backups/restores are running. For internal request it checks if its from the self node using backup_uuid. [#45072](https://github.com/ClickHouse/ClickHouse/pull/45072) ([SmitaRKulkarni](https://github.com/SmitaRKulkarni)).
* Add a cache for async block ids. This will reduce the requests of zookeeper when we enable async inserts deduplication. [#45106](https://github.com/ClickHouse/ClickHouse/pull/45106) ([Han Fei](https://github.com/hanfei1991)).
* Use structure from insertion table in generateRandom without arguments. [#45239](https://github.com/ClickHouse/ClickHouse/pull/45239) ([Kruglov Pavel](https://github.com/Avogar)).
* Add `<storage_policy>` config parameter for system logs. [#45320](https://github.com/ClickHouse/ClickHouse/pull/45320) ([Stig Bakken](https://github.com/stigsb)).
* Allow to implicitly convert floats stored in string fields of JSON to integers in `JSONExtract` functions. E.g. `JSONExtract('{"a": "1000.111"}', 'a', 'UInt64')` -> `1000`, previously it returned 0. [#45432](https://github.com/ClickHouse/ClickHouse/pull/45432) ([Anton Popov](https://github.com/CurtizJ)).
* Added fields `supports_parallel_parsing` and `supports_parallel_formatting` to table `system.formats` for better introspection. [#45499](https://github.com/ClickHouse/ClickHouse/pull/45499) ([Anton Popov](https://github.com/CurtizJ)).

#### Build/Testing/Packaging Improvement
* Builtin skim for fuzzy search in clickhouse client/local history. [#44239](https://github.com/ClickHouse/ClickHouse/pull/44239) ([Azat Khuzhin](https://github.com/azat)).
* We removed support for shared linking because of Rust. Actually, Rust is only an excuse for this removal, and we wanted to remove it nevertheless. [#44828](https://github.com/ClickHouse/ClickHouse/pull/44828) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Remove the dependency on the `adduser` tool from the packages, because we don't use it. This fixes [#44934](https://github.com/ClickHouse/ClickHouse/issues/44934). [#45011](https://github.com/ClickHouse/ClickHouse/pull/45011) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* The `SQLite` library is updated to the latest. It is used for the SQLite database and table integration engines. Also, fixed a false-positive TSan report. This closes [#45027](https://github.com/ClickHouse/ClickHouse/issues/45027). [#45031](https://github.com/ClickHouse/ClickHouse/pull/45031) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* CRC32 changes to address the WeakHash collision issue in PowerPC. [#45144](https://github.com/ClickHouse/ClickHouse/pull/45144) ([MeenaRenganathan22](https://github.com/MeenaRenganathan22)).
* Update aws-c* submodules [#43020](https://github.com/ClickHouse/ClickHouse/pull/43020) ([Vitaly Baranov](https://github.com/vitlibar)).
* Replace domain IP types (IPv4, IPv6) with native [#43221](https://github.com/ClickHouse/ClickHouse/pull/43221) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Automatically merge green backport PRs and green approved PRs [#41110](https://github.com/ClickHouse/ClickHouse/pull/41110) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Introduce a [website](https://aretestsgreenyet.com/) for the status of ClickHouse CI. [Source](https://github.com/ClickHouse/aretestsgreenyet).

#### Bug Fix

* Fix backup if mutations get killed during the backup process. [#45351](https://github.com/ClickHouse/ClickHouse/pull/45351) ([Vitaly Baranov](https://github.com/vitlibar)).
* #40651 [#41404](https://github.com/ClickHouse/ClickHouse/issues/41404). [#42126](https://github.com/ClickHouse/ClickHouse/pull/42126) ([Alexander Gololobov](https://github.com/davenger)).
* Fix possible use-of-unitialized value after executing expressions after sorting. Closes [#43386](https://github.com/ClickHouse/ClickHouse/issues/43386) [#43635](https://github.com/ClickHouse/ClickHouse/pull/43635) ([Kruglov Pavel](https://github.com/Avogar)).
* Better handling of NULL in aggregate combinators, fix possible segfault/logical error while using optimization `optimize_rewrite_sum_if_to_count_if`. Closes [#43758](https://github.com/ClickHouse/ClickHouse/issues/43758). [#43813](https://github.com/ClickHouse/ClickHouse/pull/43813) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix CREATE USER/ROLE query settings constraints. [#43993](https://github.com/ClickHouse/ClickHouse/pull/43993) ([Nikolay Degterinsky](https://github.com/evillique)).
* Fix wrong behavior of `JOIN ON t1.x = t2.x AND 1 = 1`, forbid such queries. [#44016](https://github.com/ClickHouse/ClickHouse/pull/44016) ([Vladimir C](https://github.com/vdimir)).
* Fixed bug with non-parsable default value for `EPHEMERAL` column in table metadata. [#44026](https://github.com/ClickHouse/ClickHouse/pull/44026) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Fix parsing of bad version from compatibility setting. [#44224](https://github.com/ClickHouse/ClickHouse/pull/44224) ([Kruglov Pavel](https://github.com/Avogar)).
* Bring interval subtraction from datetime in line with addition. [#44241](https://github.com/ClickHouse/ClickHouse/pull/44241) ([ltrk2](https://github.com/ltrk2)).
* Remove limits on maximum size of the result for view. [#44261](https://github.com/ClickHouse/ClickHouse/pull/44261) ([lizhuoyu5](https://github.com/lzydmxy)).
* Fix possible logical error in cache if `do_not_evict_index_and_mrk_files=1`. Closes [#42142](https://github.com/ClickHouse/ClickHouse/issues/42142). [#44268](https://github.com/ClickHouse/ClickHouse/pull/44268) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fix possible too early cache write interruption in write-through cache (caching could be stopped due to false assumption when it shouldn't have). [#44289](https://github.com/ClickHouse/ClickHouse/pull/44289) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fix possible crash in case function `IN` with constant arguments was used as a constant argument together with `LowCardinality`. Fixes [#44221](https://github.com/ClickHouse/ClickHouse/issues/44221). [#44346](https://github.com/ClickHouse/ClickHouse/pull/44346) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix support for complex parameters (like arrays) of parametric aggregate functions. This closes [#30975](https://github.com/ClickHouse/ClickHouse/issues/30975). The aggregate function `sumMapFiltered` was unusable in distributed queries before this change. [#44358](https://github.com/ClickHouse/ClickHouse/pull/44358) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix reading ObjectId in BSON schema inference. [#44382](https://github.com/ClickHouse/ClickHouse/pull/44382) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix race which can lead to premature temp parts removal before merge finished in ReplicatedMergeTree. This issue could lead to errors like `No such file or directory: xxx`. Fixes [#43983](https://github.com/ClickHouse/ClickHouse/issues/43983). [#44383](https://github.com/ClickHouse/ClickHouse/pull/44383) ([alesapin](https://github.com/alesapin)).
* Some invalid `SYSTEM ... ON CLUSTER` queries worked in an unexpected way if a cluster name was not specified. It's fixed, now invalid queries throw `SYNTAX_ERROR` as they should. Fixes [#44264](https://github.com/ClickHouse/ClickHouse/issues/44264). [#44387](https://github.com/ClickHouse/ClickHouse/pull/44387) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Fix reading Map type in ORC format. [#44400](https://github.com/ClickHouse/ClickHouse/pull/44400) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix reading columns that are not presented in input data in Parquet/ORC formats. Previously it could lead to error `INCORRECT_NUMBER_OF_COLUMNS`. Closes [#44333](https://github.com/ClickHouse/ClickHouse/issues/44333). [#44405](https://github.com/ClickHouse/ClickHouse/pull/44405) ([Kruglov Pavel](https://github.com/Avogar)).
* Previously bar() function used the same '▋' (U+258B "Left five eighths block") character to display both 5/8 and 6/8 bars. This change corrects this behavior by using '▊' (U+258A "Left three quarters block") for displaying 6/8 bar. [#44410](https://github.com/ClickHouse/ClickHouse/pull/44410) ([Alexander Gololobov](https://github.com/davenger)).
* Placing profile settings after profile settings constraints in the configuration file made constraints ineffective. [#44411](https://github.com/ClickHouse/ClickHouse/pull/44411) ([Konstantin Bogdanov](https://github.com/thevar1able)).
* Fix `SYNTAX_ERROR` while running `EXPLAIN AST INSERT` queries with data. Closes [#44207](https://github.com/ClickHouse/ClickHouse/issues/44207). [#44413](https://github.com/ClickHouse/ClickHouse/pull/44413) ([save-my-heart](https://github.com/save-my-heart)).
* Fix reading bool value with CRLF in CSV format. Closes [#44401](https://github.com/ClickHouse/ClickHouse/issues/44401). [#44442](https://github.com/ClickHouse/ClickHouse/pull/44442) ([Kruglov Pavel](https://github.com/Avogar)).
* Don't execute and/or/if/multiIf on LowCardinality dictionary, so the result type cannot be LowCardinality. It could lead to error `Illegal column ColumnLowCardinality` in some cases. Fixes [#43603](https://github.com/ClickHouse/ClickHouse/issues/43603). [#44469](https://github.com/ClickHouse/ClickHouse/pull/44469) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix mutations with setting `max_streams_for_merge_tree_reading`. [#44472](https://github.com/ClickHouse/ClickHouse/pull/44472) ([Anton Popov](https://github.com/CurtizJ)).
* Fix potential null pointer dereference with GROUPING SETS in ASTSelectQuery::formatImpl ([#43049](https://github.com/ClickHouse/ClickHouse/issues/43049)). [#44479](https://github.com/ClickHouse/ClickHouse/pull/44479) ([Robert Schulze](https://github.com/rschu1ze)).
* Validate types in table function arguments, CAST function arguments, JSONAsObject schema inference according to settings. [#44501](https://github.com/ClickHouse/ClickHouse/pull/44501) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix IN function with LowCardinality and const column, close [#44503](https://github.com/ClickHouse/ClickHouse/issues/44503). [#44506](https://github.com/ClickHouse/ClickHouse/pull/44506) ([Duc Canh Le](https://github.com/canhld94)).
* Fixed a bug in normalization of a `DEFAULT` expression in `CREATE TABLE` statement. The second argument of function `in` (or the right argument of operator `IN`) might be replaced with the result of its evaluation during CREATE query execution. Fixes [#44496](https://github.com/ClickHouse/ClickHouse/issues/44496). [#44547](https://github.com/ClickHouse/ClickHouse/pull/44547) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Projections do not work in presence of WITH ROLLUP, WITH CUBE and WITH TOTALS. In previous versions, a query produced an exception instead of skipping the usage of projections. This closes [#44614](https://github.com/ClickHouse/ClickHouse/issues/44614). This closes [#42772](https://github.com/ClickHouse/ClickHouse/issues/42772). [#44615](https://github.com/ClickHouse/ClickHouse/pull/44615) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Async blocks are not cleaned because the function `get all blocks sorted by time` didn't get async blocks. [#44651](https://github.com/ClickHouse/ClickHouse/pull/44651) ([Han Fei](https://github.com/hanfei1991)).
* Fix `LOGICAL_ERROR` `The top step of the right pipeline should be ExpressionStep` for JOIN with subquery, UNION, and TOTALS. Fixes [#43687](https://github.com/ClickHouse/ClickHouse/issues/43687). [#44673](https://github.com/ClickHouse/ClickHouse/pull/44673) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Avoid `std::out_of_range` exception in StorageExecutable. [#44681](https://github.com/ClickHouse/ClickHouse/pull/44681) ([Kruglov Pavel](https://github.com/Avogar)).
* Do not apply `optimize_syntax_fuse_functions` to quantiles on AST, close [#44712](https://github.com/ClickHouse/ClickHouse/issues/44712). [#44713](https://github.com/ClickHouse/ClickHouse/pull/44713) ([Vladimir C](https://github.com/vdimir)).
* Fix bug with wrong type in Merge table and PREWHERE, close [#43324](https://github.com/ClickHouse/ClickHouse/issues/43324). [#44716](https://github.com/ClickHouse/ClickHouse/pull/44716) ([Vladimir C](https://github.com/vdimir)).
* Fix possible crash during shutdown (while destroying TraceCollector). Fixes [#44757](https://github.com/ClickHouse/ClickHouse/issues/44757). [#44758](https://github.com/ClickHouse/ClickHouse/pull/44758) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix a possible crash in distributed query processing. The crash could happen if a query with totals or extremes returned an empty result and there are mismatched types in the Distrubuted and the local tables. Fixes [#44738](https://github.com/ClickHouse/ClickHouse/issues/44738). [#44760](https://github.com/ClickHouse/ClickHouse/pull/44760) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix fsync for fetches (`min_compressed_bytes_to_fsync_after_fetch`)/small files (ttl.txt, columns.txt) in mutations (`min_rows_to_fsync_after_merge`/`min_compressed_bytes_to_fsync_after_merge`). [#44781](https://github.com/ClickHouse/ClickHouse/pull/44781) ([Azat Khuzhin](https://github.com/azat)).
* A rare race condition was possible when querying the `system.parts` or `system.parts_columns` tables in the presence of parts being moved between disks. Introduced in [#41145](https://github.com/ClickHouse/ClickHouse/issues/41145). [#44809](https://github.com/ClickHouse/ClickHouse/pull/44809) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix the error `Context has expired` which could appear with enabled projections optimization. Can be reproduced for queries with specific functions, like `dictHas/dictGet` which use context in runtime. Fixes [#44844](https://github.com/ClickHouse/ClickHouse/issues/44844). [#44850](https://github.com/ClickHouse/ClickHouse/pull/44850) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Another fix for `Cannot read all data` error which could happen while reading `LowCardinality` dictionary from remote fs. Fixes [#44709](https://github.com/ClickHouse/ClickHouse/issues/44709). [#44875](https://github.com/ClickHouse/ClickHouse/pull/44875) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* - Ignore hwmon sensors on label read issues. [#44895](https://github.com/ClickHouse/ClickHouse/pull/44895) ([Raúl Marín](https://github.com/Algunenano)).
* Use `max_delay_to_insert` value in case calculated time to delay INSERT exceeds the setting value. Related to [#44902](https://github.com/ClickHouse/ClickHouse/issues/44902). [#44916](https://github.com/ClickHouse/ClickHouse/pull/44916) ([Igor Nikonov](https://github.com/devcrafter)).
* Fix error `Different order of columns in UNION subquery` for queries with `UNION`. Fixes [#44866](https://github.com/ClickHouse/ClickHouse/issues/44866). [#44920](https://github.com/ClickHouse/ClickHouse/pull/44920) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Delay for INSERT can be calculated incorrectly, which can lead to always using `max_delay_to_insert` setting as delay instead of a correct value. Using simple formula `max_delay_to_insert * (parts_over_threshold/max_allowed_parts_over_threshold)` i.e. delay grows proportionally to parts over threshold. Closes [#44902](https://github.com/ClickHouse/ClickHouse/issues/44902). [#44954](https://github.com/ClickHouse/ClickHouse/pull/44954) ([Igor Nikonov](https://github.com/devcrafter)).
* fix alter table ttl error when wide part has light weight delete mask. [#44959](https://github.com/ClickHouse/ClickHouse/pull/44959) ([Mingliang Pan](https://github.com/liangliangpan)).
* Follow-up fix for Replace domain IP types (IPv4, IPv6) with native [#43221](https://github.com/ClickHouse/ClickHouse/issues/43221). [#45024](https://github.com/ClickHouse/ClickHouse/pull/45024) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Follow-up fix for Replace domain IP types (IPv4, IPv6) with native https://github.com/ClickHouse/ClickHouse/pull/43221. [#45043](https://github.com/ClickHouse/ClickHouse/pull/45043) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* A buffer overflow was possible in the parser. Found by fuzzer. [#45047](https://github.com/ClickHouse/ClickHouse/pull/45047) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix possible cannot-read-all-data error in storage FileLog. Closes [#45051](https://github.com/ClickHouse/ClickHouse/issues/45051), [#38257](https://github.com/ClickHouse/ClickHouse/issues/38257). [#45057](https://github.com/ClickHouse/ClickHouse/pull/45057) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Memory efficient aggregation (setting `distributed_aggregation_memory_efficient`) is disabled when grouping sets are present in the query. [#45058](https://github.com/ClickHouse/ClickHouse/pull/45058) ([Nikita Taranov](https://github.com/nickitat)).
* Fix `RANGE_HASHED` dictionary to count range columns as part of primary key during updates when `update_field` is specified. Closes [#44588](https://github.com/ClickHouse/ClickHouse/issues/44588). [#45061](https://github.com/ClickHouse/ClickHouse/pull/45061) ([Maksim Kita](https://github.com/kitaisreal)).
* Fix error `Cannot capture column` for `LowCardinality` captured argument of nested labmda. Fixes [#45028](https://github.com/ClickHouse/ClickHouse/issues/45028). [#45065](https://github.com/ClickHouse/ClickHouse/pull/45065) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix the wrong query result of `additional_table_filters` (additional filter was not applied) in case if minmax/count projection is used. [#45133](https://github.com/ClickHouse/ClickHouse/pull/45133) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed bug in `histogram` function accepting negative values. [#45147](https://github.com/ClickHouse/ClickHouse/pull/45147) ([simpleton](https://github.com/rgzntrade)).
* Fix wrong column nullability in StoreageJoin, close [#44940](https://github.com/ClickHouse/ClickHouse/issues/44940). [#45184](https://github.com/ClickHouse/ClickHouse/pull/45184) ([Vladimir C](https://github.com/vdimir)).
* Fix `background_fetches_pool_size` settings reload (increase at runtime). [#45189](https://github.com/ClickHouse/ClickHouse/pull/45189) ([Raúl Marín](https://github.com/Algunenano)).
* Correctly process `SELECT` queries on KV engines (e.g. KeeperMap, EmbeddedRocksDB) using `IN` on the key with subquery producing different type. [#45215](https://github.com/ClickHouse/ClickHouse/pull/45215) ([Antonio Andelic](https://github.com/antonio2368)).
* Fix logical error in SEMI JOIN & join_use_nulls in some cases, close [#45163](https://github.com/ClickHouse/ClickHouse/issues/45163), close [#45209](https://github.com/ClickHouse/ClickHouse/issues/45209). [#45230](https://github.com/ClickHouse/ClickHouse/pull/45230) ([Vladimir C](https://github.com/vdimir)).
* Fix heap-use-after-free in reading from s3. [#45253](https://github.com/ClickHouse/ClickHouse/pull/45253) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix bug when the Avro Union type is ['null', Nested type], closes [#45275](https://github.com/ClickHouse/ClickHouse/issues/45275). Fix bug that incorrectly infer `bytes` type to `Float`. [#45276](https://github.com/ClickHouse/ClickHouse/pull/45276) ([flynn](https://github.com/ucasfl)).
* Throw a correct exception when explicit PREWHERE cannot be used with table using storage engine `Merge`. [#45319](https://github.com/ClickHouse/ClickHouse/pull/45319) ([Antonio Andelic](https://github.com/antonio2368)).
* Under WSL1 Ubuntu self-extracting clickhouse fails to decompress due to inconsistency - /proc/self/maps reporting 32bit file's inode, while stat reporting 64bit inode. [#45339](https://github.com/ClickHouse/ClickHouse/pull/45339) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Fix race in Distributed table startup (that could lead to processing file of async INSERT multiple times). [#45360](https://github.com/ClickHouse/ClickHouse/pull/45360) ([Azat Khuzhin](https://github.com/azat)).
* Fix possible crash while reading from storage `S3` and table function `s3` in case when `ListObject` request has failed. [#45371](https://github.com/ClickHouse/ClickHouse/pull/45371) ([Anton Popov](https://github.com/CurtizJ)).
* Fix `SELECT ... FROM system.dictionaries` exception when there is a dictionary with a bad structure (e.g. incorrect type in xml config). [#45399](https://github.com/ClickHouse/ClickHouse/pull/45399) ([Aleksei Filatov](https://github.com/aalexfvk)).
* Fix s3Cluster schema inference when structure from insertion table is used in `INSERT INTO ... SELECT * FROM s3Cluster` queries. [#45422](https://github.com/ClickHouse/ClickHouse/pull/45422) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix bug in JSON/BSONEachRow parsing with HTTP that could lead to using default values for some columns instead of values from data. [#45424](https://github.com/ClickHouse/ClickHouse/pull/45424) ([Kruglov Pavel](https://github.com/Avogar)).
* Fixed bug (Code: 632. DB::Exception: Unexpected data ... after parsed IPv6 value ...) with typed parsing of IP types from text source. [#45425](https://github.com/ClickHouse/ClickHouse/pull/45425) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* close [#45297](https://github.com/ClickHouse/ClickHouse/issues/45297) Add check for empty regular expressions. [#45428](https://github.com/ClickHouse/ClickHouse/pull/45428) ([Han Fei](https://github.com/hanfei1991)).
* Fix possible (likely distributed) query hung. [#45448](https://github.com/ClickHouse/ClickHouse/pull/45448) ([Azat Khuzhin](https://github.com/azat)).
* Fix possible deadlock with `allow_asynchronous_read_from_io_pool_for_merge_tree` enabled in case of exception from `ThreadPool::schedule`. [#45481](https://github.com/ClickHouse/ClickHouse/pull/45481) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix possible in-use table after DETACH. [#45493](https://github.com/ClickHouse/ClickHouse/pull/45493) ([Azat Khuzhin](https://github.com/azat)).
* Fix rare abort in case when query is canceled and parallel parsing was used during its execution. [#45498](https://github.com/ClickHouse/ClickHouse/pull/45498) ([Anton Popov](https://github.com/CurtizJ)).
* Fix a race between Distributed table creation and INSERT into it (could lead to CANNOT_LINK during INSERT into the table). [#45502](https://github.com/ClickHouse/ClickHouse/pull/45502) ([Azat Khuzhin](https://github.com/azat)).
* Add proper default (SLRU) to cache policy getter. Closes [#45514](https://github.com/ClickHouse/ClickHouse/issues/45514). [#45524](https://github.com/ClickHouse/ClickHouse/pull/45524) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Disallow array join in mutations closes [#42637](https://github.com/ClickHouse/ClickHouse/issues/42637) [#44447](https://github.com/ClickHouse/ClickHouse/pull/44447) ([SmitaRKulkarni](https://github.com/SmitaRKulkarni)).
* Fix for qualified asterisks with alias table name and column transformer resolves [#44736](https://github.com/ClickHouse/ClickHouse/issues/44736). [#44755](https://github.com/ClickHouse/ClickHouse/pull/44755) ([SmitaRKulkarni](https://github.com/SmitaRKulkarni)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Fix assertion in async read buffer from remote [#41231](https://github.com/ClickHouse/ClickHouse/pull/41231) ([Kseniia Sumarokova](https://github.com/kssenii)).
* add retries on ConnectionError [#42991](https://github.com/ClickHouse/ClickHouse/pull/42991) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Refactor FunctionNode [#43761](https://github.com/ClickHouse/ClickHouse/pull/43761) ([Dmitry Novik](https://github.com/novikd)).
* Some cleanup: grace hash join [#43851](https://github.com/ClickHouse/ClickHouse/pull/43851) ([Igor Nikonov](https://github.com/devcrafter)).
* Randomize setting `enable_memory_bound_merging_of_aggregation_results` in tests [#43986](https://github.com/ClickHouse/ClickHouse/pull/43986) ([Nikita Taranov](https://github.com/nickitat)).
* Analyzer aggregate functions passes small fixes [#44013](https://github.com/ClickHouse/ClickHouse/pull/44013) ([Maksim Kita](https://github.com/kitaisreal)).
* Fix wrong char in command [#44018](https://github.com/ClickHouse/ClickHouse/pull/44018) ([alesapin](https://github.com/alesapin)).
* Analyzer support Set index [#44097](https://github.com/ClickHouse/ClickHouse/pull/44097) ([Maksim Kita](https://github.com/kitaisreal)).
* Avoid loading toolchain files multiple times [#44122](https://github.com/ClickHouse/ClickHouse/pull/44122) ([Azat Khuzhin](https://github.com/azat)).
* tests: exclude flaky columns from SHOW CLUSTERS test [#44123](https://github.com/ClickHouse/ClickHouse/pull/44123) ([Azat Khuzhin](https://github.com/azat)).
* Bump libdivide (to gain some new optimizations) [#44132](https://github.com/ClickHouse/ClickHouse/pull/44132) ([Azat Khuzhin](https://github.com/azat)).
* Make atomic counter relaxed in blockNumber() [#44193](https://github.com/ClickHouse/ClickHouse/pull/44193) ([Igor Nikonov](https://github.com/devcrafter)).
* Try fix flaky 01072_window_view_multiple_columns_groupby [#44195](https://github.com/ClickHouse/ClickHouse/pull/44195) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Apply new code of named collections (from [#43147](https://github.com/ClickHouse/ClickHouse/issues/43147)) to external table engines part 1 [#44204](https://github.com/ClickHouse/ClickHouse/pull/44204) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Add some settings under `compatibility` [#44209](https://github.com/ClickHouse/ClickHouse/pull/44209) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Recommend Slack over Telegram in the "Question" issue template [#44222](https://github.com/ClickHouse/ClickHouse/pull/44222) ([Ivan Blinkov](https://github.com/blinkov)).
* Forbid paths in timezone names [#44225](https://github.com/ClickHouse/ClickHouse/pull/44225) ([Kruglov Pavel](https://github.com/Avogar)).
* Analyzer storage view crash fix [#44230](https://github.com/ClickHouse/ClickHouse/pull/44230) ([Maksim Kita](https://github.com/kitaisreal)).
* Add ThreadsInOvercommitTracker metric [#44233](https://github.com/ClickHouse/ClickHouse/pull/44233) ([Dmitry Novik](https://github.com/novikd)).
* Analyzer expired Context crash fix [#44234](https://github.com/ClickHouse/ClickHouse/pull/44234) ([Maksim Kita](https://github.com/kitaisreal)).
* Fixed use-after-free of BLAKE3 error message [#44242](https://github.com/ClickHouse/ClickHouse/pull/44242) ([Joanna Hulboj](https://github.com/jh0x)).
* Fix deadlock in StorageSystemDatabases [#44272](https://github.com/ClickHouse/ClickHouse/pull/44272) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Get rid of global Git object [#44273](https://github.com/ClickHouse/ClickHouse/pull/44273) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Update version after release [#44275](https://github.com/ClickHouse/ClickHouse/pull/44275) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Update version_date.tsv and changelogs after v22.12.1.1752-stable [#44281](https://github.com/ClickHouse/ClickHouse/pull/44281) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Do not hold data parts during insert [#44299](https://github.com/ClickHouse/ClickHouse/pull/44299) ([Anton Popov](https://github.com/CurtizJ)).
* Another fix `test_server_reload` [#44306](https://github.com/ClickHouse/ClickHouse/pull/44306) ([Antonio Andelic](https://github.com/antonio2368)).
* Update version_date.tsv and changelogs after v22.9.7.34-stable [#44309](https://github.com/ClickHouse/ClickHouse/pull/44309) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* tests/perf: fix dependency check during DROP [#44312](https://github.com/ClickHouse/ClickHouse/pull/44312) ([Azat Khuzhin](https://github.com/azat)).
* (unused openssl integration, not for production) a follow-up [#44325](https://github.com/ClickHouse/ClickHouse/pull/44325) ([Boris Kuschel](https://github.com/bkuschel)).
* Replace old named collections code with new (from [#43147](https://github.com/ClickHouse/ClickHouse/issues/43147)) part 2 [#44327](https://github.com/ClickHouse/ClickHouse/pull/44327) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Disable "git-import" test in debug mode [#44328](https://github.com/ClickHouse/ClickHouse/pull/44328) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Check s3 part upload settings [#44335](https://github.com/ClickHouse/ClickHouse/pull/44335) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Lock table for share during startup for database ordinary [#44393](https://github.com/ClickHouse/ClickHouse/pull/44393) ([alesapin](https://github.com/alesapin)).
* Fix bug with merge/mutate pool size increase [#44436](https://github.com/ClickHouse/ClickHouse/pull/44436) ([alesapin](https://github.com/alesapin)).
* Update 01072_window_view_multiple_columns_groupby.sh [#44438](https://github.com/ClickHouse/ClickHouse/pull/44438) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Respect setting settings.schema_inference_make_columns_nullable in Parquet/ORC/Arrow formats [#44446](https://github.com/ClickHouse/ClickHouse/pull/44446) ([Kruglov Pavel](https://github.com/Avogar)).
* Add tests as examples with errors of date(time) and string comparison that we should eliminate [#44462](https://github.com/ClickHouse/ClickHouse/pull/44462) ([Ilya Yatsishin](https://github.com/qoega)).
* Parallel parts cleanup with zero copy replication [#44466](https://github.com/ClickHouse/ClickHouse/pull/44466) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Fix incorrect usages of `getPartName()` [#44468](https://github.com/ClickHouse/ClickHouse/pull/44468) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Fix flaky test `roaring_memory_tracking` [#44470](https://github.com/ClickHouse/ClickHouse/pull/44470) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Clarify query_id in test 01092_memory_profiler [#44483](https://github.com/ClickHouse/ClickHouse/pull/44483) ([Vladimir C](https://github.com/vdimir)).
* Do not try to remove WAL/move broken parts for static storage [#44495](https://github.com/ClickHouse/ClickHouse/pull/44495) ([Azat Khuzhin](https://github.com/azat)).
* Removed parent pid check that breaks in containers [#44499](https://github.com/ClickHouse/ClickHouse/pull/44499) ([Alexander Gololobov](https://github.com/davenger)).
* Add the lambda to collect data for workflow_jobs [#44520](https://github.com/ClickHouse/ClickHouse/pull/44520) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Introduce groupArrayLast() (useful to store last X values) [#44521](https://github.com/ClickHouse/ClickHouse/pull/44521) ([Azat Khuzhin](https://github.com/azat)).
* Infer numbers starting from zero as strings in TSV [#44522](https://github.com/ClickHouse/ClickHouse/pull/44522) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix wrong condition for enabling async reading from MergeTree. [#44530](https://github.com/ClickHouse/ClickHouse/pull/44530) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Followup [#43761](https://github.com/ClickHouse/ClickHouse/issues/43761) [#44541](https://github.com/ClickHouse/ClickHouse/pull/44541) ([Dmitry Novik](https://github.com/novikd)).
* Drop unused columns after join on/using [#44545](https://github.com/ClickHouse/ClickHouse/pull/44545) ([Vladimir C](https://github.com/vdimir)).
* Improve inferring arrays with nulls in JSON formats [#44550](https://github.com/ClickHouse/ClickHouse/pull/44550) ([Kruglov Pavel](https://github.com/Avogar)).
* Slightly cleanup interactive line reader code [#44601](https://github.com/ClickHouse/ClickHouse/pull/44601) ([Azat Khuzhin](https://github.com/azat)).
* Fix restart after quorum insert [#44628](https://github.com/ClickHouse/ClickHouse/pull/44628) ([alesapin](https://github.com/alesapin)).
* Function viewExplain accept SELECT and settings [#44641](https://github.com/ClickHouse/ClickHouse/pull/44641) ([Vladimir C](https://github.com/vdimir)).
* Add +x flag for run-fuzzer.sh [#44649](https://github.com/ClickHouse/ClickHouse/pull/44649) ([alesapin](https://github.com/alesapin)).
* Custom reading for mutation [#44653](https://github.com/ClickHouse/ClickHouse/pull/44653) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix flaky test test_backup_restore_on_cluster [#44660](https://github.com/ClickHouse/ClickHouse/pull/44660) ([Vitaly Baranov](https://github.com/vitlibar)).
* tests/integration: add missing kazoo client termination [#44666](https://github.com/ClickHouse/ClickHouse/pull/44666) ([Azat Khuzhin](https://github.com/azat)).
* Move dmesg dumping out from runner to ci-runner.py [#44667](https://github.com/ClickHouse/ClickHouse/pull/44667) ([Azat Khuzhin](https://github.com/azat)).
* Remove questdb (it makes a little sense but the test was flaky) [#44669](https://github.com/ClickHouse/ClickHouse/pull/44669) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix minor typo: replace validate_bugix_check with validate_bugfix_check [#44672](https://github.com/ClickHouse/ClickHouse/pull/44672) ([Pradeep Chhetri](https://github.com/chhetripradeep)).
* Fix parsing of ANY operator [#44678](https://github.com/ClickHouse/ClickHouse/pull/44678) ([Nikolay Degterinsky](https://github.com/evillique)).
* Fix test `01130_in_memory_parts` [#44683](https://github.com/ClickHouse/ClickHouse/pull/44683) ([Anton Popov](https://github.com/CurtizJ)).
* Add retries to HTTP requests in ClickHouse test [#44689](https://github.com/ClickHouse/ClickHouse/pull/44689) ([alesapin](https://github.com/alesapin)).
* Fix flaky tests [#44690](https://github.com/ClickHouse/ClickHouse/pull/44690) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Improve handling of old parts [#44694](https://github.com/ClickHouse/ClickHouse/pull/44694) ([Raúl Marín](https://github.com/Algunenano)).
* Update entrypoint.sh [#44699](https://github.com/ClickHouse/ClickHouse/pull/44699) ([Denny Crane](https://github.com/den-crane)).
* tests: more fixes for test_keeper_auth [#44702](https://github.com/ClickHouse/ClickHouse/pull/44702) ([Azat Khuzhin](https://github.com/azat)).
* Fix crash on delete from materialized view [#44705](https://github.com/ClickHouse/ClickHouse/pull/44705) ([Alexander Gololobov](https://github.com/davenger)).
* Fix flaky filelog tests with database ordinary [#44706](https://github.com/ClickHouse/ClickHouse/pull/44706) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Make lightweight deletes always synchronous [#44718](https://github.com/ClickHouse/ClickHouse/pull/44718) ([Alexander Gololobov](https://github.com/davenger)).
* Fix deadlock in attach thread [#44719](https://github.com/ClickHouse/ClickHouse/pull/44719) ([alesapin](https://github.com/alesapin)).
* A few improvements to AST Fuzzer [#44720](https://github.com/ClickHouse/ClickHouse/pull/44720) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix flaky test [#44721](https://github.com/ClickHouse/ClickHouse/pull/44721) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Rename log in stress test [#44722](https://github.com/ClickHouse/ClickHouse/pull/44722) ([alesapin](https://github.com/alesapin)).
* Debug deadlock in stress test [#44723](https://github.com/ClickHouse/ClickHouse/pull/44723) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix flaky test "02102_row_binary_with_names_and_types.sh" [#44724](https://github.com/ClickHouse/ClickHouse/pull/44724) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Slightly better some tests [#44725](https://github.com/ClickHouse/ClickHouse/pull/44725) ([alesapin](https://github.com/alesapin)).
* Fix cases when clickhouse-server takes long time to start in functional tests with MSan [#44726](https://github.com/ClickHouse/ClickHouse/pull/44726) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Perf test: Log the time spent waiting for file sync [#44737](https://github.com/ClickHouse/ClickHouse/pull/44737) ([Raúl Marín](https://github.com/Algunenano)).
* Fix flaky test 02448_clone_replica_lost_part [#44759](https://github.com/ClickHouse/ClickHouse/pull/44759) ([alesapin](https://github.com/alesapin)).
* Build rust modules from the binary directory [#44762](https://github.com/ClickHouse/ClickHouse/pull/44762) ([Azat Khuzhin](https://github.com/azat)).
* Remove database ordinary from stress test [#44763](https://github.com/ClickHouse/ClickHouse/pull/44763) ([alesapin](https://github.com/alesapin)).
* Fix flaky test 02479_mysql_connect_to_self [#44768](https://github.com/ClickHouse/ClickHouse/pull/44768) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Slightly better docs [#44808](https://github.com/ClickHouse/ClickHouse/pull/44808) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix misleading integration tests reports for parametrized tests [#44825](https://github.com/ClickHouse/ClickHouse/pull/44825) ([Azat Khuzhin](https://github.com/azat)).
* Fix data race in StorageS3 [#44842](https://github.com/ClickHouse/ClickHouse/pull/44842) ([Antonio Andelic](https://github.com/antonio2368)).
* Fix rare race which can lead to queue hang [#44847](https://github.com/ClickHouse/ClickHouse/pull/44847) ([alesapin](https://github.com/alesapin)).
* No more retries in integration tests [#44851](https://github.com/ClickHouse/ClickHouse/pull/44851) ([Ilya Yatsishin](https://github.com/qoega)).
* Document usage of check_cxx_source_compiles instead of check_cxx_source_runs [#44854](https://github.com/ClickHouse/ClickHouse/pull/44854) ([Robert Schulze](https://github.com/rschu1ze)).
* More cases of OOM in Fuzzer [#44855](https://github.com/ClickHouse/ClickHouse/pull/44855) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix: sorted DISTINCT with empty string [#44856](https://github.com/ClickHouse/ClickHouse/pull/44856) ([Igor Nikonov](https://github.com/devcrafter)).
* Try to fix MSan build [#44857](https://github.com/ClickHouse/ClickHouse/pull/44857) ([Nikolay Degterinsky](https://github.com/evillique)).
* Cleanup setup_minio.sh [#44858](https://github.com/ClickHouse/ClickHouse/pull/44858) ([Pradeep Chhetri](https://github.com/chhetripradeep)).
* Wait for ZK process to stop in tests using snapshot [#44859](https://github.com/ClickHouse/ClickHouse/pull/44859) ([Antonio Andelic](https://github.com/antonio2368)).
* Fix flaky test and several typos [#44870](https://github.com/ClickHouse/ClickHouse/pull/44870) ([alesapin](https://github.com/alesapin)).
* Upload status files to S3 report for bugfix check [#44871](https://github.com/ClickHouse/ClickHouse/pull/44871) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Fix flaky test `02503_insert_storage_snapshot` [#44873](https://github.com/ClickHouse/ClickHouse/pull/44873) ([alesapin](https://github.com/alesapin)).
* Revert some changes from [#42777](https://github.com/ClickHouse/ClickHouse/issues/42777) to fix performance tests [#44876](https://github.com/ClickHouse/ClickHouse/pull/44876) ([Kruglov Pavel](https://github.com/Avogar)).
* Rewrite test_postgres_protocol test [#44880](https://github.com/ClickHouse/ClickHouse/pull/44880) ([Ilya Yatsishin](https://github.com/qoega)).
* Fix ConcurrentBoundedQueue::emplace() return value in case of finished queue [#44881](https://github.com/ClickHouse/ClickHouse/pull/44881) ([Azat Khuzhin](https://github.com/azat)).
* Validate function arguments in query tree [#44882](https://github.com/ClickHouse/ClickHouse/pull/44882) ([Dmitry Novik](https://github.com/novikd)).
* Rework CI reports to have a class and clarify the logic [#44883](https://github.com/ClickHouse/ClickHouse/pull/44883) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* fix-typo [#44886](https://github.com/ClickHouse/ClickHouse/pull/44886) ([Enrique Herreros](https://github.com/eherrerosj)).
* Store ZK generated data in `test_keeper_snapshot_small_distance` [#44888](https://github.com/ClickHouse/ClickHouse/pull/44888) ([Antonio Andelic](https://github.com/antonio2368)).
* Fix "AttributeError: 'BuildResult' object has no attribute 'libraries'" in BuilderReport and BuilderSpecialReport [#44890](https://github.com/ClickHouse/ClickHouse/pull/44890) ([Robert Schulze](https://github.com/rschu1ze)).
* Convert integration test_dictionaries_update_field to a stateless [#44891](https://github.com/ClickHouse/ClickHouse/pull/44891) ([Azat Khuzhin](https://github.com/azat)).
* Upgrade googletest to latest HEAD [#44894](https://github.com/ClickHouse/ClickHouse/pull/44894) ([Robert Schulze](https://github.com/rschu1ze)).
* Try fix rabbitmq potential leak [#44897](https://github.com/ClickHouse/ClickHouse/pull/44897) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Try to fix flaky `test_storage_kafka::test_kafka_produce_key_timestamp` [#44898](https://github.com/ClickHouse/ClickHouse/pull/44898) ([Antonio Andelic](https://github.com/antonio2368)).
* Fix flaky `test_concurrent_queries_restriction_by_query_kind` [#44903](https://github.com/ClickHouse/ClickHouse/pull/44903) ([Antonio Andelic](https://github.com/antonio2368)).
* Avoid Keeper crash on shutdown (fix `test_keeper_snapshot_on_exit`) [#44908](https://github.com/ClickHouse/ClickHouse/pull/44908) ([Antonio Andelic](https://github.com/antonio2368)).
* Do not merge over a gap with outdated undeleted parts [#44909](https://github.com/ClickHouse/ClickHouse/pull/44909) ([Sema Checherinda](https://github.com/CheSema)).
* Fix logging message in MergeTreeDataMergerMutator (about merged parts) [#44917](https://github.com/ClickHouse/ClickHouse/pull/44917) ([Azat Khuzhin](https://github.com/azat)).
* Fix flaky test `test_lost_part` [#44921](https://github.com/ClickHouse/ClickHouse/pull/44921) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Add fast and cancellable shared_mutex alternatives [#44924](https://github.com/ClickHouse/ClickHouse/pull/44924) ([Sergei Trifonov](https://github.com/serxa)).
* Fix deadlock in Keeper's changelog [#44937](https://github.com/ClickHouse/ClickHouse/pull/44937) ([Antonio Andelic](https://github.com/antonio2368)).
* Stop merges to avoid a race between merge and freeze. [#44938](https://github.com/ClickHouse/ClickHouse/pull/44938) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix memory leak in Aws::InitAPI [#44942](https://github.com/ClickHouse/ClickHouse/pull/44942) ([Vitaly Baranov](https://github.com/vitlibar)).
* Change error code on invalid background_pool_size config [#44947](https://github.com/ClickHouse/ClickHouse/pull/44947) ([Raúl Marín](https://github.com/Algunenano)).
* Fix exception fix in TraceCollector dtor [#44948](https://github.com/ClickHouse/ClickHouse/pull/44948) ([Robert Schulze](https://github.com/rschu1ze)).
* Parallel distributed insert select with s3Cluster [3] [#44955](https://github.com/ClickHouse/ClickHouse/pull/44955) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Do not check read result consistency when unwinding [#44956](https://github.com/ClickHouse/ClickHouse/pull/44956) ([Alexander Gololobov](https://github.com/davenger)).
* Up the log level of tables dependencies graphs [#44957](https://github.com/ClickHouse/ClickHouse/pull/44957) ([Vitaly Baranov](https://github.com/vitlibar)).
* Hipster's HTML [#44961](https://github.com/ClickHouse/ClickHouse/pull/44961) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Docs: Mention non-standard DOTALL behavior of ClickHouse's match() [#44977](https://github.com/ClickHouse/ClickHouse/pull/44977) ([Robert Schulze](https://github.com/rschu1ze)).
* tests: fix test_replicated_users flakiness [#44978](https://github.com/ClickHouse/ClickHouse/pull/44978) ([Azat Khuzhin](https://github.com/azat)).
* Check what if disable some checks in storage Merge. [#44983](https://github.com/ClickHouse/ClickHouse/pull/44983) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix check for not existing input in ActionsDAG [#44987](https://github.com/ClickHouse/ClickHouse/pull/44987) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Update version_date.tsv and changelogs after v22.12.2.25-stable [#44988](https://github.com/ClickHouse/ClickHouse/pull/44988) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Fix test test_grpc_protocol/test.py::test_progress [#44996](https://github.com/ClickHouse/ClickHouse/pull/44996) ([Vitaly Baranov](https://github.com/vitlibar)).
* Improve S3 EC2 metadata tests [#45001](https://github.com/ClickHouse/ClickHouse/pull/45001) ([Vitaly Baranov](https://github.com/vitlibar)).
* Fix minmax_count_projection with _partition_value [#45003](https://github.com/ClickHouse/ClickHouse/pull/45003) ([Amos Bird](https://github.com/amosbird)).
* Fix strange trash in Fuzzer [#45006](https://github.com/ClickHouse/ClickHouse/pull/45006) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Add `dmesg.log` to Fuzzer [#45008](https://github.com/ClickHouse/ClickHouse/pull/45008) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix `01961_roaring_memory_tracking` test, again [#45009](https://github.com/ClickHouse/ClickHouse/pull/45009) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Recognize more ok cases for Fuzzer [#45012](https://github.com/ClickHouse/ClickHouse/pull/45012) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Supposedly fix the "Download script failed" error [#45013](https://github.com/ClickHouse/ClickHouse/pull/45013) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Add snapshot creation retry in Keeper tests using ZooKeeper [#45016](https://github.com/ClickHouse/ClickHouse/pull/45016) ([Antonio Andelic](https://github.com/antonio2368)).
* test for [#20098](https://github.com/ClickHouse/ClickHouse/issues/20098) [#45017](https://github.com/ClickHouse/ClickHouse/pull/45017) ([Denny Crane](https://github.com/den-crane)).
* test for [#26473](https://github.com/ClickHouse/ClickHouse/issues/26473) [#45018](https://github.com/ClickHouse/ClickHouse/pull/45018) ([Denny Crane](https://github.com/den-crane)).
* Remove the remainings of Testflows (2). [#45021](https://github.com/ClickHouse/ClickHouse/pull/45021) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Enable the check that was commented [#45022](https://github.com/ClickHouse/ClickHouse/pull/45022) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix false positive in Fuzzer [#45025](https://github.com/ClickHouse/ClickHouse/pull/45025) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix false positive in Fuzzer, alternative variant [#45026](https://github.com/ClickHouse/ClickHouse/pull/45026) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix function `range` (the bug was unreleased) [#45030](https://github.com/ClickHouse/ClickHouse/pull/45030) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix OOM in Fuzzer [#45032](https://github.com/ClickHouse/ClickHouse/pull/45032) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Less OOM in Stress test [#45033](https://github.com/ClickHouse/ClickHouse/pull/45033) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Add a test for [#31361](https://github.com/ClickHouse/ClickHouse/issues/31361) [#45034](https://github.com/ClickHouse/ClickHouse/pull/45034) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Add a test for [#38729](https://github.com/ClickHouse/ClickHouse/issues/38729) [#45035](https://github.com/ClickHouse/ClickHouse/pull/45035) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix typos [#45036](https://github.com/ClickHouse/ClickHouse/pull/45036) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* I didn't understand the logic of this test, @azat [#45037](https://github.com/ClickHouse/ClickHouse/pull/45037) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Small fixes for Coordination unit tests [#45039](https://github.com/ClickHouse/ClickHouse/pull/45039) ([Antonio Andelic](https://github.com/antonio2368)).
* Fix flaky test (hilarious) [#45042](https://github.com/ClickHouse/ClickHouse/pull/45042) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Non significant changes [#45046](https://github.com/ClickHouse/ClickHouse/pull/45046) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Don't fix parallel formatting [#45050](https://github.com/ClickHouse/ClickHouse/pull/45050) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix (benign) data race in clickhouse-client [#45053](https://github.com/ClickHouse/ClickHouse/pull/45053) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Analyzer aggregation without column fix [#45055](https://github.com/ClickHouse/ClickHouse/pull/45055) ([Maksim Kita](https://github.com/kitaisreal)).
* Analyzer ARRAY JOIN crash fix [#45059](https://github.com/ClickHouse/ClickHouse/pull/45059) ([Maksim Kita](https://github.com/kitaisreal)).
* Analyzer function IN crash fix [#45064](https://github.com/ClickHouse/ClickHouse/pull/45064) ([Maksim Kita](https://github.com/kitaisreal)).
* JIT compilation float to bool conversion fix [#45067](https://github.com/ClickHouse/ClickHouse/pull/45067) ([Maksim Kita](https://github.com/kitaisreal)).
* Update version_date.tsv and changelogs after v22.11.3.47-stable [#45069](https://github.com/ClickHouse/ClickHouse/pull/45069) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Update version_date.tsv and changelogs after v22.10.5.54-stable [#45071](https://github.com/ClickHouse/ClickHouse/pull/45071) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Update version_date.tsv and changelogs after v22.3.16.1190-lts [#45073](https://github.com/ClickHouse/ClickHouse/pull/45073) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Improve release scripts [#45074](https://github.com/ClickHouse/ClickHouse/pull/45074) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Change the color of links in dark reports a little bit [#45077](https://github.com/ClickHouse/ClickHouse/pull/45077) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Fix Fuzzer script [#45082](https://github.com/ClickHouse/ClickHouse/pull/45082) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Try fixing KeeperMap tests [#45094](https://github.com/ClickHouse/ClickHouse/pull/45094) ([Antonio Andelic](https://github.com/antonio2368)).
* Update version_date.tsv and changelogs after v22.8.12.45-lts [#45098](https://github.com/ClickHouse/ClickHouse/pull/45098) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Try to fix flaky test_create_user_and_login/test.py::test_login_as_dropped_user_xml [#45099](https://github.com/ClickHouse/ClickHouse/pull/45099) ([Ilya Yatsishin](https://github.com/qoega)).
* Update version_date.tsv and changelogs after v22.10.6.3-stable [#45107](https://github.com/ClickHouse/ClickHouse/pull/45107) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Docs: Make heading consistent with other headings in System Table docs [#45109](https://github.com/ClickHouse/ClickHouse/pull/45109) ([Robert Schulze](https://github.com/rschu1ze)).
* Update version_date.tsv and changelogs after v22.11.4.3-stable [#45110](https://github.com/ClickHouse/ClickHouse/pull/45110) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Update version_date.tsv and changelogs after v22.12.3.5-stable [#45113](https://github.com/ClickHouse/ClickHouse/pull/45113) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Docs: Rewrite awkwardly phrased sentence about flush interval [#45114](https://github.com/ClickHouse/ClickHouse/pull/45114) ([Robert Schulze](https://github.com/rschu1ze)).
* Fix data race in s3Cluster. [#45123](https://github.com/ClickHouse/ClickHouse/pull/45123) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Pull SQLancer image before check run [#45125](https://github.com/ClickHouse/ClickHouse/pull/45125) ([Ilya Yatsishin](https://github.com/qoega)).
* Fix flaky azure test [#45134](https://github.com/ClickHouse/ClickHouse/pull/45134) ([alesapin](https://github.com/alesapin)).
* Minor cleanup in stress/run.sh [#45136](https://github.com/ClickHouse/ClickHouse/pull/45136) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Performance report: "Partial queries" --> "Backward-incompatible queries [#45152](https://github.com/ClickHouse/ClickHouse/pull/45152) ([Robert Schulze](https://github.com/rschu1ze)).
* Fix flaky test_tcp_handler_interserver_listen_host [#45156](https://github.com/ClickHouse/ClickHouse/pull/45156) ([Ilya Yatsishin](https://github.com/qoega)).
* Clean trash from changelog for v22.3.16.1190-lts [#45159](https://github.com/ClickHouse/ClickHouse/pull/45159) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Disable `test_storage_rabbitmq` [#45161](https://github.com/ClickHouse/ClickHouse/pull/45161) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Disable test_ttl_move_memory_usage as too flaky. [#45162](https://github.com/ClickHouse/ClickHouse/pull/45162) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* More logging to facilitate debugging of flaky test_ttl_replicated [#45165](https://github.com/ClickHouse/ClickHouse/pull/45165) ([Alexander Gololobov](https://github.com/davenger)).
* Try to fix flaky test_ttl_move_memory_usage [#45168](https://github.com/ClickHouse/ClickHouse/pull/45168) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Fix flaky test test_multiple_disks/test.py::test_rename [#45180](https://github.com/ClickHouse/ClickHouse/pull/45180) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Calculate only required columns in system.detached_parts [#45181](https://github.com/ClickHouse/ClickHouse/pull/45181) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Restart NightlyBuilds if the runner died [#45187](https://github.com/ClickHouse/ClickHouse/pull/45187) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Fix part ID generation for IP types for backward compatibility [#45191](https://github.com/ClickHouse/ClickHouse/pull/45191) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Fix integration test test_replicated_users::test_rename_replicated [#45192](https://github.com/ClickHouse/ClickHouse/pull/45192) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Add CACHE_INVALIDATOR for sqlancer builds [#45201](https://github.com/ClickHouse/ClickHouse/pull/45201) ([Ilya Yatsishin](https://github.com/qoega)).
* Fix possible stack-use-after-return in LimitReadBuffer [#45203](https://github.com/ClickHouse/ClickHouse/pull/45203) ([Kruglov Pavel](https://github.com/Avogar)).
* Disable check to make test_overcommit_tracker not flaky [#45206](https://github.com/ClickHouse/ClickHouse/pull/45206) ([Dmitry Novik](https://github.com/novikd)).
* Fix flaky test `01961_roaring_memory_tracking` (3) [#45208](https://github.com/ClickHouse/ClickHouse/pull/45208) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Remove trash from stress test [#45211](https://github.com/ClickHouse/ClickHouse/pull/45211) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* remove unused function [#45212](https://github.com/ClickHouse/ClickHouse/pull/45212) ([flynn](https://github.com/ucasfl)).
* Fix flaky `test_keeper_three_nodes_two_alive` [#45213](https://github.com/ClickHouse/ClickHouse/pull/45213) ([Antonio Andelic](https://github.com/antonio2368)).
* Fuzz PREWHERE clause [#45222](https://github.com/ClickHouse/ClickHouse/pull/45222) ([Alexander Gololobov](https://github.com/davenger)).
* Added a test for merge join key condition with big int & decimal  [#45228](https://github.com/ClickHouse/ClickHouse/pull/45228) ([SmitaRKulkarni](https://github.com/SmitaRKulkarni)).
* Fix rare logical error: `Too large alignment` [#45229](https://github.com/ClickHouse/ClickHouse/pull/45229) ([Anton Popov](https://github.com/CurtizJ)).
* Update version_date.tsv and changelogs after v22.3.17.13-lts [#45234](https://github.com/ClickHouse/ClickHouse/pull/45234) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* More verbose logs about replication log entries [#45235](https://github.com/ClickHouse/ClickHouse/pull/45235) ([Alexander Tokmakov](https://github.com/tavplubix)).
* One more attempt to fix race in TCPHandler [#45240](https://github.com/ClickHouse/ClickHouse/pull/45240) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Update clickhouse-test [#45251](https://github.com/ClickHouse/ClickHouse/pull/45251) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Planner small fixes [#45254](https://github.com/ClickHouse/ClickHouse/pull/45254) ([Maksim Kita](https://github.com/kitaisreal)).
* Fix log level "Test" for send_logs_level in client [#45273](https://github.com/ClickHouse/ClickHouse/pull/45273) ([Azat Khuzhin](https://github.com/azat)).
* tests: fix clickhouse binaries detection [#45283](https://github.com/ClickHouse/ClickHouse/pull/45283) ([Azat Khuzhin](https://github.com/azat)).
* tests/ci: encode HTML entities in the reports [#45284](https://github.com/ClickHouse/ClickHouse/pull/45284) ([Azat Khuzhin](https://github.com/azat)).
* Disable `02151_hash_table_sizes_stats_distributed` under TSAN [#45287](https://github.com/ClickHouse/ClickHouse/pull/45287) ([Nikita Taranov](https://github.com/nickitat)).
* Fix wrong approved_at, simplify conditions [#45302](https://github.com/ClickHouse/ClickHouse/pull/45302) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Disable 02028_create_select_settings with Ordinary [#45307](https://github.com/ClickHouse/ClickHouse/pull/45307) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Save message format strings for DB::Exception [#45342](https://github.com/ClickHouse/ClickHouse/pull/45342) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Slightly better output for glibc check [#45353](https://github.com/ClickHouse/ClickHouse/pull/45353) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Add checks for compilation of regexps [#45356](https://github.com/ClickHouse/ClickHouse/pull/45356) ([Anton Popov](https://github.com/CurtizJ)).
* Analyzer compound identifier typo correction fix [#45357](https://github.com/ClickHouse/ClickHouse/pull/45357) ([Maksim Kita](https://github.com/kitaisreal)).
* Bump to newer version of debug-action [#45359](https://github.com/ClickHouse/ClickHouse/pull/45359) ([Ilya Yatsishin](https://github.com/qoega)).
* Improve failed kafka startup logging [#45369](https://github.com/ClickHouse/ClickHouse/pull/45369) ([Ilya Yatsishin](https://github.com/qoega)).
* Fix flaky ttl test [#45370](https://github.com/ClickHouse/ClickHouse/pull/45370) ([alesapin](https://github.com/alesapin)).
* Add detailed profile events for throttling [#45373](https://github.com/ClickHouse/ClickHouse/pull/45373) ([Sergei Trifonov](https://github.com/serxa)).
* Update .gitignore [#45378](https://github.com/ClickHouse/ClickHouse/pull/45378) ([Nikolay Degterinsky](https://github.com/evillique)).
* Make test simpler to see errors [#45402](https://github.com/ClickHouse/ClickHouse/pull/45402) ([Ilya Yatsishin](https://github.com/qoega)).
* Reduce an amount of trash in `tests_system_merges` [#45403](https://github.com/ClickHouse/ClickHouse/pull/45403) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Fix reading from encrypted disk with passed file size [#45418](https://github.com/ClickHouse/ClickHouse/pull/45418) ([Anton Popov](https://github.com/CurtizJ)).
* Add delete by ttl for zookeeper_log [#45419](https://github.com/ClickHouse/ClickHouse/pull/45419) ([Nikita Taranov](https://github.com/nickitat)).
* Minor improvements around reading from remote [#45442](https://github.com/ClickHouse/ClickHouse/pull/45442) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Docs: Beautify section on secondary index types [#45444](https://github.com/ClickHouse/ClickHouse/pull/45444) ([Robert Schulze](https://github.com/rschu1ze)).
* Fix Buffer's offsets mismatch logical error in stress test [#45446](https://github.com/ClickHouse/ClickHouse/pull/45446) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Better formatting for exception messages [#45449](https://github.com/ClickHouse/ClickHouse/pull/45449) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Add default GRANULARITY argument for secondary indexes [#45451](https://github.com/ClickHouse/ClickHouse/pull/45451) ([Nikolay Degterinsky](https://github.com/evillique)).
* Fix typos [#45470](https://github.com/ClickHouse/ClickHouse/pull/45470) ([Robert Schulze](https://github.com/rschu1ze)).
* Add more retries to AST Fuzzer [#45479](https://github.com/ClickHouse/ClickHouse/pull/45479) ([Nikolay Degterinsky](https://github.com/evillique)).
* Remove unnecessary getTotalRowCount function calls [#45485](https://github.com/ClickHouse/ClickHouse/pull/45485) ([Maksim Kita](https://github.com/kitaisreal)).
* Forward declaration of ConcurrentBoundedQueue in ThreadStatus [#45489](https://github.com/ClickHouse/ClickHouse/pull/45489) ([Azat Khuzhin](https://github.com/azat)).
* Revert "Merge pull request [#44922](https://github.com/ClickHouse/ClickHouse/issues/44922) from azat/dist/async-INSERT-metrics" [#45492](https://github.com/ClickHouse/ClickHouse/pull/45492) ([Azat Khuzhin](https://github.com/azat)).
* Docs: Fix weird formatting [#45495](https://github.com/ClickHouse/ClickHouse/pull/45495) ([Robert Schulze](https://github.com/rschu1ze)).
* Docs: Fix link to writing guide [#45496](https://github.com/ClickHouse/ClickHouse/pull/45496) ([Robert Schulze](https://github.com/rschu1ze)).
* Improve logging for TeePopen.timeout exceeded [#45504](https://github.com/ClickHouse/ClickHouse/pull/45504) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Update test_system_merges/test.py [#45516](https://github.com/ClickHouse/ClickHouse/pull/45516) ([Alexander Tokmakov](https://github.com/tavplubix)).

## [Changelog for 2022](https://clickhouse.com/docs/en/whats-new/changelog/2022)
