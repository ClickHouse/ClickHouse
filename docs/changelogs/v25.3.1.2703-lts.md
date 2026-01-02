---
sidebar_position: 1
sidebar_label: 2025
---

# 2025 Changelog

### ClickHouse release v25.3.1.2703-lts (b684e195f96) FIXME as compared to v25.3.1.1-new (217f67ec3ad)

#### Backward Incompatible Change
* Disallow truncate database for replicated databases. [#76651](https://github.com/ClickHouse/ClickHouse/pull/76651) ([Bharat Nallan](https://github.com/bharatnc)).
* Disable parallel replicas by default when analyzer is disabled regardless `compatibility` setting. It's still possible to change this behavior by explicitly setting `parallel_replicas_only_with_analyzer` to `false`. [#77115](https://github.com/ClickHouse/ClickHouse/pull/77115) ([Igor Nikonov](https://github.com/devcrafter)).
* It's no longer possible to use `NaN` or `inf` for float values as settings. [#77546](https://github.com/ClickHouse/ClickHouse/pull/77546) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).

#### New Feature
* A new implementation of the Userspace Page Cache, which allows caching data in the in-process memory instead of relying on the OS page cache, which is useful when the data is stored on a remote virtual filesystem without backing with the local filesystem cache. [#70509](https://github.com/ClickHouse/ClickHouse/pull/70509) ([Michael Kolupaev](https://github.com/al13n321)).
* Backported in [#77924](https://github.com/ClickHouse/ClickHouse/issues/77924): Support low cardinality decimal data types, fix [#72256](https://github.com/ClickHouse/ClickHouse/issues/72256). [#72833](https://github.com/ClickHouse/ClickHouse/pull/72833) ([zhanglistar](https://github.com/zhanglistar)).
* Added `concurrent_threads_scheduler` server setting that governs how CPU slots are distributed among concurrent queries. Could be set to `round_robin` (previous behavior) or `fair_round_robin` to address the issue of unfair CPU distribution between INSERTs and SELECTs. [#75949](https://github.com/ClickHouse/ClickHouse/pull/75949) ([Sergei Trifonov](https://github.com/serxa)).
* Added function `arraySymmetricDifference`. It returns all elements from multiple array arguments which do not occur in all arguments. Example: `SELECT arraySymmetricDifference([1, 2], [2, 3])` returns `[1, 3]`. (issue [#61673](https://github.com/ClickHouse/ClickHouse/issues/61673)). [#76231](https://github.com/ClickHouse/ClickHouse/pull/76231) ([Filipp Abapolov](https://github.com/pheepa)).
* Add `estimatecompressionratio` aggregate function- see [#70801](https://github.com/ClickHouse/ClickHouse/issues/70801). [#76661](https://github.com/ClickHouse/ClickHouse/pull/76661) ([Tariq Almawash](https://github.com/talmawash)).
* `FilterTransformPassedRows` and `FilterTransformPassedBytes` profile events will show the number of rows and number of bytes filtered during the query execution. [#76662](https://github.com/ClickHouse/ClickHouse/pull/76662) ([Onkar Deshpande](https://github.com/onkar)).
* Added the keccak256 hash function, commonly used in blockchain implementations, especially in EVM-based systems. [#76669](https://github.com/ClickHouse/ClickHouse/pull/76669) ([Arnaud Briche](https://github.com/arnaudbriche)).
* The functionality adds the ability to define a list of headers that are forwarded from the headers of the client request to the external http authenticator. [#77054](https://github.com/ClickHouse/ClickHouse/pull/77054) ([inv2004](https://github.com/inv2004)).
* Add 3 new functions. `icebergTruncate` according to specification. https://iceberg.apache.org/spec/#truncate-transform-details, `toYearNumSinceEpoch` and `toMonthNumSinceEpoch`. Support `truncate` transform in partition pruning for `Iceberg` engine. [#77403](https://github.com/ClickHouse/ClickHouse/pull/77403) ([alesapin](https://github.com/alesapin)).
* Backported in [#77975](https://github.com/ClickHouse/ClickHouse/issues/77975): `JSON` data type is production-ready. See https://jsonbench.com/. `Dynamic` and `Varaint` data types are production ready. [#77785](https://github.com/ClickHouse/ClickHouse/pull/77785) ([Alexey Milovidov](https://github.com/alexey-milovidov)).

#### Experimental Feature
* Added support for dynamic cluster autodiscovery. This extends the existing _node_ autodiscovery feature. ClickHouse can now automatically detect and register new _clusters_ under a common ZooKeeper path using `<multicluster_root_path>`. [#76001](https://github.com/ClickHouse/ClickHouse/pull/76001) ([Anton Ivashkin](https://github.com/ianton-ru)).
* Allow automatic cleanup merges of entire partitions after a configurable timeout with a new setting `enable_replacing_merge_with_cleanup_for_min_age_to_force_merge`. [#76440](https://github.com/ClickHouse/ClickHouse/pull/76440) ([Christoph Wurm](https://github.com/cwurm)).
* Add support [for Unity Catalog](https://www.databricks.com/product/unity-catalog) for DeltaLake tables on top of AWS S3 and local filesystem. [#76988](https://github.com/ClickHouse/ClickHouse/pull/76988) ([alesapin](https://github.com/alesapin)).
* Introduce experimental integration with AWS Glue service catalog for Iceberg tables. [#77257](https://github.com/ClickHouse/ClickHouse/pull/77257) ([alesapin](https://github.com/alesapin)).

#### Performance Improvement
* Apply `preserve_most` attribute at some places in code. [#67778](https://github.com/ClickHouse/ClickHouse/pull/67778) ([Nikita Taranov](https://github.com/nickitat)).
* Implement query condition cache to improve query performance using repeated conditions. The range of the portion of data that does not meet the condition is remembered as a temporary index in memory. Subsequent queries will use this index. close [#67768](https://github.com/ClickHouse/ClickHouse/issues/67768) ### Documentation entry for user-facing changes. [#69236](https://github.com/ClickHouse/ClickHouse/pull/69236) ([zhongyuankai](https://github.com/zhongyuankai)).
* Replace Int256 and UInt256 with clang builtin i256 in arithmetic calculation according to tests in [#70502](https://github.com/ClickHouse/ClickHouse/issues/70502). [#73658](https://github.com/ClickHouse/ClickHouse/pull/73658) ([李扬](https://github.com/taiyang-li)).
* In some cases (e.g. empty array column) data parts can contain empty files. We can skip writing empty blobs to ObjectStorage and only store metadata for such files when the table resides on disk with separated metadata and object storages. [#75860](https://github.com/ClickHouse/ClickHouse/pull/75860) ([Alexander Gololobov](https://github.com/davenger)).
* Improve min/max performance for Decimal32/Decimal64/DateTime64. [#76570](https://github.com/ClickHouse/ClickHouse/pull/76570) ([李扬](https://github.com/taiyang-li)).
* Actively evict data from the cache on parts removal. Do not let the cache grow to the maximum size if the amount of data is less. [#76641](https://github.com/ClickHouse/ClickHouse/pull/76641) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Query compilation (setting `compile_expressions`) now considers the machine type. This speeds up such queries significantly. [#76753](https://github.com/ClickHouse/ClickHouse/pull/76753) ([Robert Schulze](https://github.com/rschu1ze)).
* Backported in [#77880](https://github.com/ClickHouse/ClickHouse/issues/77880): Optimize arraySort. [#76850](https://github.com/ClickHouse/ClickHouse/pull/76850) ([李扬](https://github.com/taiyang-li)).
* Backported in [#77961](https://github.com/ClickHouse/ClickHouse/issues/77961): Disable `filesystem_cache_prefer_bigger_buffer_size` when the cache is used passively, such as for merges. [#77898](https://github.com/ClickHouse/ClickHouse/pull/77898) ([Kseniia Sumarokova](https://github.com/kssenii)).

#### Improvement
* Replace table functions with their -Cluster alternatives if parallel replicas are enabled. Fixes [#65024](https://github.com/ClickHouse/ClickHouse/issues/65024). [#70659](https://github.com/ClickHouse/ClickHouse/pull/70659) ([Konstantin Bogdanov](https://github.com/thevar1able)).
* Respect column insensitive column matching for fields in tuple columns. Close https://github.com/apache/incubator-gluten/issues/8324. [#73780](https://github.com/ClickHouse/ClickHouse/pull/73780) ([李扬](https://github.com/taiyang-li)).
* Parameters for the codec Gorilla will now always be saved in the table metadata in .sql file. This closes: [#70072](https://github.com/ClickHouse/ClickHouse/issues/70072). [#74814](https://github.com/ClickHouse/ClickHouse/pull/74814) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Backported in [#77949](https://github.com/ClickHouse/ClickHouse/issues/77949): Enabled SSH protocol back. Fixed some critical vulnerabilities so that it is no longer possible to use custom pager or specify `server-logs-file`. Disabled the ability to pass client options through the environment variables by default (it is still possible via `ssh-server.enable_client_options_passing` in config.xml). Supported progress table, query cancellation, completion, profile events progress, stdin and `send_logs_level` option. This closes: [#74340](https://github.com/ClickHouse/ClickHouse/issues/74340). [#74989](https://github.com/ClickHouse/ClickHouse/pull/74989) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Implemented parsing enhancements (Sequence ID parsing: Added functionality to parse sequence identifiers in manifest files AND Avro metadata parsing: Redesigned the Avro metadata parser to be easily extendable for future enhancements). [#75010](https://github.com/ClickHouse/ClickHouse/pull/75010) ([Daniil Ivanik](https://github.com/divanik)).
* We have support for gauge/counter metric types. However, they are insufficient for some metrics (e.g., the response times of requests to the keeper), so support for the histogram metric type is needed. The interface closely mirrors the Prometheus client, where you simply call `observe(value)` to increment the counter in the bucket corresponding to the value. The histogram metrics are exposed via system.histogram_metrics. [#75736](https://github.com/ClickHouse/ClickHouse/pull/75736) ([Miсhael Stetsyuk](https://github.com/mstetsyuk)).
* Remove trace_id from default ORDER BY for system.opentelemetry_span_log. [#75907](https://github.com/ClickHouse/ClickHouse/pull/75907) ([Azat Khuzhin](https://github.com/azat)).
* Encryption (XML attribute `encrypted_by`) can now be applied to any configuration file (config.xml, users.xml, nested configuration files). Previously, it worked only for the top-level config.xml file. [#75911](https://github.com/ClickHouse/ClickHouse/pull/75911) ([Mikhail Gorshkov](https://github.com/mgorshkov)).
* Convert join to in subquery if output column is tied to the left table, need a uniqueness step at first, so disabled by default until the step is added later. [#75942](https://github.com/ClickHouse/ClickHouse/pull/75942) ([Shichao Jin](https://github.com/jsc0218)).
* Improve the `system.warnings` table and add some dynamic warning messages that can be added, updated or removed. [#76029](https://github.com/ClickHouse/ClickHouse/pull/76029) ([Bharat Nallan](https://github.com/bharatnc)).
* This PR makes it impossible to run a query `ALTER USER user1 ADD PROFILES a, DROP ALL PROFILES` because all `DROP` operations should come first in the order. [#76242](https://github.com/ClickHouse/ClickHouse/pull/76242) ([pufit](https://github.com/pufit)).
* Various enhancements for SYNC REPLICA (better error messages, better tests, sanity checks). [#76307](https://github.com/ClickHouse/ClickHouse/pull/76307) ([Azat Khuzhin](https://github.com/azat)).
* Use correct fallback when multipart copy to S3 fails during backup with Access Denied. Multi part copy can generate Access Denied error when backup is done between buckets that have different credentials. [#76515](https://github.com/ClickHouse/ClickHouse/pull/76515) ([Antonio Andelic](https://github.com/antonio2368)).
* Faster ClickHouse Servers shutdown (get rid of 2.5sec delay). [#76550](https://github.com/ClickHouse/ClickHouse/pull/76550) ([Azat Khuzhin](https://github.com/azat)).
* Upgraded librdkafka to version 2.8.0 and improved the shutdown sequence for Kafka tables, reducing delays during table drops and server restarts. The `engine=Kafka` no longer explicitly leaves the consumer group when a table is dropped. Instead, the consumer remains in the group until it is automatically removed after `session_timeout_ms` (default: 45 seconds) of inactivity. [#76621](https://github.com/ClickHouse/ClickHouse/pull/76621) ([filimonov](https://github.com/filimonov)).
* Fix validation of s3 request settings. [#76658](https://github.com/ClickHouse/ClickHouse/pull/76658) ([Vitaly Baranov](https://github.com/vitlibar)).
* Avoid excess allocation in readbufferfroms3 and other remote reading buffers, reduce their memory consumption in half. [#76692](https://github.com/ClickHouse/ClickHouse/pull/76692) ([Sema Checherinda](https://github.com/CheSema)).
* System tables like `server_settings` or `settings` have a `default` value column which is convenient. only `merge_tree_settings` and `replicated_merge_tree_settings` do not have that column enabled. [#76942](https://github.com/ClickHouse/ClickHouse/pull/76942) ([Diego Nieto](https://github.com/lesandie)).
* Added `ProfileEvents::QueryPreempted`, which has the same logic as `CurrentMetrics::QueryPreempted`. [#77015](https://github.com/ClickHouse/ClickHouse/pull/77015) ([VicoWu](https://github.com/VicoWu)).
* Previously database replicated might print credentials specified in a query to logs. This behaviour is fixed. This closes: [#77123](https://github.com/ClickHouse/ClickHouse/issues/77123). [#77133](https://github.com/ClickHouse/ClickHouse/pull/77133) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Bump zstd from 1.5.5 to 1.5.7 which has pretty [good performance improvements](https://github.com/facebook/zstd/releases/tag/v1.5.7). [#77137](https://github.com/ClickHouse/ClickHouse/pull/77137) ([Pradeep Chhetri](https://github.com/chhetripradeep)).
* Allow ALTER TABLE DROP PARTITION for plain_rewritable disk. [#77138](https://github.com/ClickHouse/ClickHouse/pull/77138) ([Julia Kartseva](https://github.com/jkartseva)).
* Add the ability to randomly sleep up to 500ms independent of part sizes before merges/mutations execution in case of zero-copy replication. [#77165](https://github.com/ClickHouse/ClickHouse/pull/77165) ([Alexey Katsman](https://github.com/alexkats)).
* Support atomic rename when `TRUNCATE` is used with `INTO OUTFILE`. Resolves [#70323](https://github.com/ClickHouse/ClickHouse/issues/70323). [#77181](https://github.com/ClickHouse/ClickHouse/pull/77181) ([Onkar Deshpande](https://github.com/onkar)).
* Allow to explicitly specify metadata file to read for Iceberg with storage/table function setting `iceberg_metadata_file_path `. Fixes [#47412](https://github.com/ClickHouse/ClickHouse/issues/47412). [#77318](https://github.com/ClickHouse/ClickHouse/pull/77318) ([alesapin](https://github.com/alesapin)).
* Change reverted. [#77399](https://github.com/ClickHouse/ClickHouse/pull/77399) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).
* Backup/restore setting `allow_s3_native_copy` now supports value three possible values: - `False` - s3 native copy will not be used; - `True` (old default) - ClickHouse will try s3 native copy first, if it fails then fallback to the reading+writing approach; - `'auto'` (new default) - ClickHouse will compare the source and destination credentials first. If they are same, ClickHouse will try s3 native copy and then may fallback to the reading+writing approach. If they are different, ClickHouse will go directly to the reading+writing approach. [#77401](https://github.com/ClickHouse/ClickHouse/pull/77401) ([Vitaly Baranov](https://github.com/vitlibar)).
* Skipping index cache is reverted. [#77447](https://github.com/ClickHouse/ClickHouse/pull/77447) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Reduce memory usage during prefetches of JSON column in Wide parts. [#77640](https://github.com/ClickHouse/ClickHouse/pull/77640) ([Pavel Kruglov](https://github.com/Avogar)).
* Support aws session token and environment credentials usage in delta kernel for DeltaLake table engine. [#77661](https://github.com/ClickHouse/ClickHouse/pull/77661) ([Kseniia Sumarokova](https://github.com/kssenii)).

#### Bug Fix (user-visible misbehavior in an official stable release)
* Fix stuck while processing pending batch for async distributed INSERT (due to i.e. `No such file or directory`). [#72939](https://github.com/ClickHouse/ClickHouse/pull/72939) ([Azat Khuzhin](https://github.com/azat)).
* Improved datetime conversion during index analysis by enforcing saturating behavior for implicit Date to DateTime conversions. This resolves potential index analysis inaccuracies caused by datetime range limitations. This fixes [#73307](https://github.com/ClickHouse/ClickHouse/issues/73307). It also fixes explicit `toDateTime` conversion when `date_time_overflow_behavior = 'ignore'` which is the default value. [#73326](https://github.com/ClickHouse/ClickHouse/pull/73326) ([Amos Bird](https://github.com/amosbird)).
* Fix all sort of bugs due to race between UUID and table names (for instance it will fix the race between `RENAME` and `RESTART REPLICA`, in case of concurrent `RENAME` with `SYSTEM RESTART REPLICA` you may get end up restarting wrong replica, or/and leaving one of the tables in a `Table X is being restarted` state). [#76308](https://github.com/ClickHouse/ClickHouse/pull/76308) ([Azat Khuzhin](https://github.com/azat)).
* Fix data loss when enable async insert and insert into ... from file ... with unequal block size if the first block size < async_max_size but the second block > async_max_size, the second block will not be inserted. these data is left in `squashing`. [#76343](https://github.com/ClickHouse/ClickHouse/pull/76343) ([Han Fei](https://github.com/hanfei1991)).
* Renamed field 'marks' to 'marks_bytes' in `system.data_skipping_indices`. [#76374](https://github.com/ClickHouse/ClickHouse/pull/76374) ([Robert Schulze](https://github.com/rschu1ze)).
* Fix dynamic filesystem cache resize handling unexpected errors during eviction. [#76466](https://github.com/ClickHouse/ClickHouse/pull/76466) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fixed `used_flag` initialization in parallel hash. It might cause a server crash. [#76580](https://github.com/ClickHouse/ClickHouse/pull/76580) ([Nikita Taranov](https://github.com/nickitat)).
* Fix a logical error when calling `defaultProfiles()` function inside a projection. [#76627](https://github.com/ClickHouse/ClickHouse/pull/76627) ([pufit](https://github.com/pufit)).
* Do not request interactive basic auth in the browser in Web UI. Closes [#76319](https://github.com/ClickHouse/ClickHouse/issues/76319). [#76637](https://github.com/ClickHouse/ClickHouse/pull/76637) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix THERE_IS_NO_COLUMN exception when selecting boolean literal from distributed tables. [#76656](https://github.com/ClickHouse/ClickHouse/pull/76656) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* The subpath inside the table directory is chosen in a more profound way. [#76681](https://github.com/ClickHouse/ClickHouse/pull/76681) ([Daniil Ivanik](https://github.com/divanik)).
* Fix an error `Not found column in block` after altering a table with a subcolumn in PK. After https://github.com/ClickHouse/ClickHouse/pull/72644, requires https://github.com/ClickHouse/ClickHouse/pull/74403. [#76686](https://github.com/ClickHouse/ClickHouse/pull/76686) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Add performance tests for null shortcuit and fix bugs. [#76708](https://github.com/ClickHouse/ClickHouse/pull/76708) ([李扬](https://github.com/taiyang-li)).
* Flush output write buffers before finalizing them. Fix `LOGICAL_ERROR` generated during the finalization of some output format, e.g. `JSONEachRowWithProgressRowOutputFormat`. [#76726](https://github.com/ClickHouse/ClickHouse/pull/76726) ([Antonio Andelic](https://github.com/antonio2368)).
* Added support for MongoDB binary UUID ([#74452](https://github.com/ClickHouse/ClickHouse/issues/74452)) - Fixed WHERE pushdown to MongoDB when using the table function ([#72210](https://github.com/ClickHouse/ClickHouse/issues/72210)) - Changed the MongoDB - ClickHouse type mapping such that MongoDB's binary UUID can only be parsed to ClickHouse's UUID. This should avoid ambiguities and surprises in future. - Fixed OID mapping, preserving backward compatibility. [#76762](https://github.com/ClickHouse/ClickHouse/pull/76762) ([Kirill Nikiforov](https://github.com/allmazz)).
* Fix exception handling in parallel prefixes deserialization of JSON subcolumns. [#76809](https://github.com/ClickHouse/ClickHouse/pull/76809) ([Pavel Kruglov](https://github.com/Avogar)).
* Fix lgamma function behavior for negative integers. [#76840](https://github.com/ClickHouse/ClickHouse/pull/76840) ([Ilya Kataev](https://github.com/IlyaKataev)).
* Fix reverse key analysis for explicitly defined primary keys. Similar to [#76654](https://github.com/ClickHouse/ClickHouse/issues/76654). [#76846](https://github.com/ClickHouse/ClickHouse/pull/76846) ([Amos Bird](https://github.com/amosbird)).
* Fix pretty print of Bool values in JSON format. [#76905](https://github.com/ClickHouse/ClickHouse/pull/76905) ([Pavel Kruglov](https://github.com/Avogar)).
* Fix possible crash because of bad JSON column rollback on error during async inserts. [#76908](https://github.com/ClickHouse/ClickHouse/pull/76908) ([Pavel Kruglov](https://github.com/Avogar)).
* Previously, `multi_if` may return different types of columns during planning and main execution. This resulted in code producing undefined behavior from the C++ perspective. [#76914](https://github.com/ClickHouse/ClickHouse/pull/76914) ([Nikita Taranov](https://github.com/nickitat)).
* Fixed incorrect serialization of constant nullable keys in MergeTree. This fixes [#76939](https://github.com/ClickHouse/ClickHouse/issues/76939). [#76985](https://github.com/ClickHouse/ClickHouse/pull/76985) ([Amos Bird](https://github.com/amosbird)).
* Fix sorting of `BFloat16` values. This closes [#75487](https://github.com/ClickHouse/ClickHouse/issues/75487). This closes [#75669](https://github.com/ClickHouse/ClickHouse/issues/75669). [#77000](https://github.com/ClickHouse/ClickHouse/pull/77000) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Bug fix json with variant subcolumn by adding check to skip ephemeral subcolumns in part consistency check. [#72187](https://github.com/ClickHouse/ClickHouse/issues/72187). [#77034](https://github.com/ClickHouse/ClickHouse/pull/77034) ([Smita Kulkarni](https://github.com/SmitaRKulkarni)).
* Fix crash in template parsing in Values format in case of types mismatch. [#77071](https://github.com/ClickHouse/ClickHouse/pull/77071) ([Pavel Kruglov](https://github.com/Avogar)).
* Don't allow creating EmbeddedRocksDB table with subcolumn in primary key. Previosly such table could be created but select queries were failing. [#77074](https://github.com/ClickHouse/ClickHouse/pull/77074) ([Pavel Kruglov](https://github.com/Avogar)).
* Fix illegal comparison in distributed queries because pushing down predicates to remote doesn't respect literal types. [#77093](https://github.com/ClickHouse/ClickHouse/pull/77093) ([Duc Canh Le](https://github.com/canhld94)).
* Fix crash during Kafka table creation with exception. [#77121](https://github.com/ClickHouse/ClickHouse/pull/77121) ([Pavel Kruglov](https://github.com/Avogar)).
* Support new JSON and subcolumns in Kafka and RabbitMQ engines. [#77122](https://github.com/ClickHouse/ClickHouse/pull/77122) ([Pavel Kruglov](https://github.com/Avogar)).
* Fix exceptions stack unwinding on MacOS. [#77126](https://github.com/ClickHouse/ClickHouse/pull/77126) ([Eduard Karacharov](https://github.com/korowa)).
* Fix reading 'null' subcolumn in getSubcolumn function. [#77163](https://github.com/ClickHouse/ClickHouse/pull/77163) ([Pavel Kruglov](https://github.com/Avogar)).
* Fix bloom filter index with Array and not supported functions. [#77271](https://github.com/ClickHouse/ClickHouse/pull/77271) ([Pavel Kruglov](https://github.com/Avogar)).
* We should only check the restriction on the amount of tables during the initial CREATE query. [#77274](https://github.com/ClickHouse/ClickHouse/pull/77274) ([Nikolay Degterinsky](https://github.com/evillique)).
* `SELECT toBFloat16(-0.0) == toBFloat16(0.0)` now correctly returns `true` (from previously `false`). This makes the behavior consistent with `Float32` and `Float64`. [#77290](https://github.com/ClickHouse/ClickHouse/pull/77290) ([Shankar Iyer](https://github.com/shankar-iyer)).
* Fix posbile incorrect reference to unintialized key_index variable, which may lead to crash in debug builds (this uninitialized reference won't cause issues in release builds because subsequent code are likely to throw errors.) ### documentation entry for user-facing changes. [#77305](https://github.com/ClickHouse/ClickHouse/pull/77305) ([wxybear](https://github.com/wxybear)).
* Reverted. [#77307](https://github.com/ClickHouse/ClickHouse/pull/77307) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix name for partition with a Bool value. It was broken in https://github.com/ClickHouse/ClickHouse/pull/74533. [#77319](https://github.com/ClickHouse/ClickHouse/pull/77319) ([Pavel Kruglov](https://github.com/Avogar)).
* Fix comparison between tuples with nullable elements inside and strings. As an example, before the change comparison between a Tuple `(1, null)` and a String `'(1,null)'` would result in an error. Another example would be a comparison between a Tuple `(1, a)`, where `a` is a Nullable column, and a String `'(1, 2)'`. This change addresses these issues. [#77323](https://github.com/ClickHouse/ClickHouse/pull/77323) ([Alexey Katsman](https://github.com/alexkats)).
* Fix crash in ObjectStorageQueueSource. Was intoduced in https://github.com/ClickHouse/ClickHouse/pull/76358. [#77325](https://github.com/ClickHouse/ClickHouse/pull/77325) ([Pavel Kruglov](https://github.com/Avogar)).
* Backported in [#77877](https://github.com/ClickHouse/ClickHouse/issues/77877): Fix a bug when `close_session` query parameter didn't have any effect leading to named sessions being closed only after `session_timeout`. [#77336](https://github.com/ClickHouse/ClickHouse/pull/77336) ([Alexey Katsman](https://github.com/alexkats)).
* Fix `async_insert` with `input()`. [#77340](https://github.com/ClickHouse/ClickHouse/pull/77340) ([Azat Khuzhin](https://github.com/azat)).
* Fix: `WITH FILL` may fail with NOT_FOUND_COLUMN_IN_BLOCK when sorting column is removed by planer. Similar issue related to inconsistent DAG calculated for INTERPOLATE expression. [#77343](https://github.com/ClickHouse/ClickHouse/pull/77343) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Reverted. [#77390](https://github.com/ClickHouse/ClickHouse/pull/77390) ([Vladimir Cherkasov](https://github.com/vdimir)).
* Fix several LOGICAL_ERRORs around setting alias of invalid AST nodes. [#77445](https://github.com/ClickHouse/ClickHouse/pull/77445) ([Raúl Marín](https://github.com/Algunenano)).
* In filesystem cache impementation fix error processing during file segment write. [#77471](https://github.com/ClickHouse/ClickHouse/pull/77471) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Make DatabaseIceberg use correct metadata file provided by catalog. Closes [#75187](https://github.com/ClickHouse/ClickHouse/issues/75187). [#77486](https://github.com/ClickHouse/ClickHouse/pull/77486) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Revert 'Avoid toAST() in execution of scalar subqueries'. [#77584](https://github.com/ClickHouse/ClickHouse/pull/77584) ([Raúl Marín](https://github.com/Algunenano)).
* The query cache now assumes that UDFs are non-deterministic. Accordingly, results of queries with UDFs are no longer cached. Previously, users were able to define non-deterministic UDFs whose result would erronously be cached (issue [#77553](https://github.com/ClickHouse/ClickHouse/issues/77553)). [#77633](https://github.com/ClickHouse/ClickHouse/pull/77633) ([Jimmy Aguilar Mena](https://github.com/Ergus)).
* Fix system.filesystem_cache_log working only under setting `enable_filesystem_cache_log`. [#77650](https://github.com/ClickHouse/ClickHouse/pull/77650) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fix a logical error when calling `defaultRoles()` function inside a projection. Follow-up for [#76627](https://github.com/ClickHouse/ClickHouse/issues/76627). [#77667](https://github.com/ClickHouse/ClickHouse/pull/77667) ([pufit](https://github.com/pufit)).
* Second arguments of type `Nullable` for function `arrayResize` are now disallowed. Previously, anything from errors to wrong results could happen with `Nullable` as second argument. (issue [#48398](https://github.com/ClickHouse/ClickHouse/issues/48398)). [#77724](https://github.com/ClickHouse/ClickHouse/pull/77724) ([Manish Gill](https://github.com/mgill25)).
* Backported in [#77821](https://github.com/ClickHouse/ClickHouse/issues/77821): Regularly check if merges and mutations were cancelled even in case when the operation doesn't produce any blocks to write. [#77766](https://github.com/ClickHouse/ClickHouse/pull/77766) ([János Benjamin Antal](https://github.com/antaljanosbenjamin)).

#### Build/Testing/Packaging Improvement
* Restore deleted nats integration tests and fix errors. - fixed some race conditions in nats engine - fixed data loss when streaming data to nats in case of connection loss - fixed freeze of receiving the last chunk of data when streaming from nats ended - nats_max_reconnect is deprecated and has no effect, reconnect is performed permanently with nats_reconnect_wait timeout. [#75850](https://github.com/ClickHouse/ClickHouse/pull/75850) ([Dmitry Novikov](https://github.com/dmitry-sles-novikov)).
* `clickhouse-odbc-bridge` and `clickhouse-library-bridge` are moved to a separate repository, https://github.com/ClickHouse/odbc-bridge/. [#76225](https://github.com/ClickHouse/ClickHouse/pull/76225) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Update fmt to 11.1.3. [#76547](https://github.com/ClickHouse/ClickHouse/pull/76547) ([Raúl Marín](https://github.com/Algunenano)).
* Bump `libcpuid` to `0.7.1`. [#76573](https://github.com/ClickHouse/ClickHouse/pull/76573) ([Konstantin Bogdanov](https://github.com/thevar1able)).
* Use machine-readable format to parse pytest results. [#76910](https://github.com/ClickHouse/ClickHouse/pull/76910) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Fix rust cross-compilation and allow disabling Rust completely. [#76921](https://github.com/ClickHouse/ClickHouse/pull/76921) ([Raúl Marín](https://github.com/Algunenano)).
* Require clang 19 to build the project. [#76945](https://github.com/ClickHouse/ClickHouse/pull/76945) ([Raúl Marín](https://github.com/Algunenano)).
* The test is executed for 10+ seconds in the serial mode. It's too long for fast tests. [#76948](https://github.com/ClickHouse/ClickHouse/pull/76948) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Bump `sccache` to `0.10.0`. [#77580](https://github.com/ClickHouse/ClickHouse/pull/77580) ([Konstantin Bogdanov](https://github.com/thevar1able)).

#### NO CL CATEGORY

* Backported in [#77982](https://github.com/ClickHouse/ClickHouse/issues/77982):. [#77759](https://github.com/ClickHouse/ClickHouse/pull/77759) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).

#### NO CL ENTRY

* NO CL ENTRY:  'Revert "Move bridges to a separate repository"'. [#76593](https://github.com/ClickHouse/ClickHouse/pull/76593) ([Alexander Tokmakov](https://github.com/tavplubix)).
* NO CL ENTRY:  'Revert "Revert "Move bridges to a separate repository""'. [#76635](https://github.com/ClickHouse/ClickHouse/pull/76635) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* NO CL ENTRY:  'Revert "Userspace page cache v2"'. [#77113](https://github.com/ClickHouse/ClickHouse/pull/77113) ([Alexander Tokmakov](https://github.com/tavplubix)).
* NO CL ENTRY:  'Revert "Revert "Userspace page cache v2""'. [#77114](https://github.com/ClickHouse/ClickHouse/pull/77114) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* NO CL ENTRY:  'Revert "Fix THERE_IS_NO_COLUMN for parallel replicas with constant table ALIAS"'. [#77568](https://github.com/ClickHouse/ClickHouse/pull/77568) ([Raúl Marín](https://github.com/Algunenano)).
* NO CL ENTRY:  'Revert "Partial merge join uses fs cache for temp files"'. [#77574](https://github.com/ClickHouse/ClickHouse/pull/77574) ([Raúl Marín](https://github.com/Algunenano)).
* NO CL ENTRY:  'Revert "Join to Subquery "'. [#77581](https://github.com/ClickHouse/ClickHouse/pull/77581) ([Raúl Marín](https://github.com/Algunenano)).
* NO CL ENTRY:  'Revert "Non-constant `CASE` support"'. [#77713](https://github.com/ClickHouse/ClickHouse/pull/77713) ([Raúl Marín](https://github.com/Algunenano)).
* NO CL ENTRY:  'Revert "Fix `multi_if`"'. [#77715](https://github.com/ClickHouse/ClickHouse/pull/77715) ([Raúl Marín](https://github.com/Algunenano)).
* NO CL ENTRY:  'Revert "Docs: replace HTML img tag with image component"'. [#77786](https://github.com/ClickHouse/ClickHouse/pull/77786) ([Shaun Struwig](https://github.com/Blargian)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Use delta-kernel-rs for delta lake integration. [#74884](https://github.com/ClickHouse/ClickHouse/pull/74884) ([Kseniia Sumarokova](https://github.com/kssenii)).
* A few additions to the AST fuzzer: add join clauses to known tables, add optimize statements, and `DISTINCT`. Added benchmark functionality when compiled with BuzzHouse. Using a peer ClickHouse server, compare the performance of each modified query (query time and memory usage). Sometimes settings are added to the query such as the join algorithm and block size. [#75279](https://github.com/ClickHouse/ClickHouse/pull/75279) ([Pedro Ferreira](https://github.com/PedroTadim)).
* Tune test_storage_rabbitmq flaky test. [#75656](https://github.com/ClickHouse/ClickHouse/pull/75656) ([Pablo Marcos](https://github.com/pamarcos)).
* Some random minor changes in code. [#76111](https://github.com/ClickHouse/ClickHouse/pull/76111) ([Nikita Taranov](https://github.com/nickitat)).
* Fix undefined behaviour while exception casting. [#76211](https://github.com/ClickHouse/ClickHouse/pull/76211) ([Aleksei Filatov](https://github.com/aalexfvk)).
* Fix flaky SYSTEM WAIT VIEW (fixes flaky 02932_refreshable_materialized_views_1). [#76232](https://github.com/ClickHouse/ClickHouse/pull/76232) ([Azat Khuzhin](https://github.com/azat)).
* Flatten expression in optimize_and_compare_chain. [#76273](https://github.com/ClickHouse/ClickHouse/pull/76273) ([Vladimir Cherkasov](https://github.com/vdimir)).
* A method for refreshing Disks' state. [#76321](https://github.com/ClickHouse/ClickHouse/pull/76321) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* tests: fix 02597_column_* flakiness. [#76345](https://github.com/ClickHouse/ClickHouse/pull/76345) ([Azat Khuzhin](https://github.com/azat)).
* In the case of the 1-1 resize processing, a fast path is enabled, which allows data to go through resize without redundant operations. [#76354](https://github.com/ClickHouse/ClickHouse/pull/76354) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).
* Use existing client for creating AzureObjectStorage constructor. [#76412](https://github.com/ClickHouse/ClickHouse/pull/76412) ([Aris Tritas](https://github.com/aris-aiven)).
* Fix recursive copy in clickhouse-disks. [#76413](https://github.com/ClickHouse/ClickHouse/pull/76413) ([Alexander Gololobov](https://github.com/davenger)).
* Fix clustered Azure Blob Storage function query parser. [#76414](https://github.com/ClickHouse/ClickHouse/pull/76414) ([Aris Tritas](https://github.com/aris-aiven)).
* ci: add some comments about differences in artifacts paths. [#76416](https://github.com/ClickHouse/ClickHouse/pull/76416) ([Azat Khuzhin](https://github.com/azat)).
* Using parallel replicas without analyzer is not supported. New setting `parallel_replicas_only_with_analyzer` is introduced to prevent using parallel replicas without analyzer in backward-compatible way. [#76427](https://github.com/ClickHouse/ClickHouse/pull/76427) ([Igor Nikonov](https://github.com/devcrafter)).
* Follow-up for [#74070](https://github.com/ClickHouse/ClickHouse/issues/74070). [#76448](https://github.com/ClickHouse/ClickHouse/pull/76448) ([Robert Schulze](https://github.com/rschu1ze)).
* Fix strange logical error messages. [#76468](https://github.com/ClickHouse/ClickHouse/pull/76468) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).
* Better exceptions when invalid constraint is used. [#76497](https://github.com/ClickHouse/ClickHouse/pull/76497) ([Antonio Andelic](https://github.com/antonio2368)).
* Setting `ignore_drop_queries_probability` should only change DROP to TRUNCATE when it is the initial query. Otherwise, it will cause cluster inconsistency with some replicas dropping and some truncating the table in question. [#76510](https://github.com/ClickHouse/ClickHouse/pull/76510) ([Nikolay Degterinsky](https://github.com/evillique)).
* log exceptions that occurred in PipelineExecutor. [#76511](https://github.com/ClickHouse/ClickHouse/pull/76511) ([Miсhael Stetsyuk](https://github.com/mstetsyuk)).
* Add `LoggerElapsedNanoseconds` to Keeper metrics. [#76517](https://github.com/ClickHouse/ClickHouse/pull/76517) ([Antonio Andelic](https://github.com/antonio2368)).
* Add test for ReadBufferFromEncryptedFile & AsynchronousBoundedReadBuffer working together. This test is designed to check fix https://github.com/ClickHouse/ClickHouse/pull/76429. [#76553](https://github.com/ClickHouse/ClickHouse/pull/76553) ([Vitaly Baranov](https://github.com/vitlibar)).
* Return back progress for merges. [#76556](https://github.com/ClickHouse/ClickHouse/pull/76556) ([Anton Popov](https://github.com/CurtizJ)).
* Build `libunwind` from `llvm-project`. [#76574](https://github.com/ClickHouse/ClickHouse/pull/76574) ([Konstantin Bogdanov](https://github.com/thevar1able)).
* Use `MultiRead` when querying `system.distributed_ddl_queue`. [#76575](https://github.com/ClickHouse/ClickHouse/pull/76575) ([Antonio Andelic](https://github.com/antonio2368)).
* Remove unused private field prev_reader from MergeTreeRangeReader (fixes clang20). [#76578](https://github.com/ClickHouse/ClickHouse/pull/76578) ([Azat Khuzhin](https://github.com/azat)).
* Use proper macro in `HashJoin`. [#76582](https://github.com/ClickHouse/ClickHouse/pull/76582) ([Nikita Taranov](https://github.com/nickitat)).
* Remove print_in_format from ASTSetQuery. [#76585](https://github.com/ClickHouse/ClickHouse/pull/76585) ([Azat Khuzhin](https://github.com/azat)).
* Update README.md. [#76586](https://github.com/ClickHouse/ClickHouse/pull/76586) ([Tyler Hannan](https://github.com/tylerhannan)).
* Fix flaky s3queue tests. [#76587](https://github.com/ClickHouse/ClickHouse/pull/76587) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Untangle InterpreterCreateQuery.h (requires ~700 modules to recompile). [#76588](https://github.com/ClickHouse/ClickHouse/pull/76588) ([Azat Khuzhin](https://github.com/azat)).
* Update version after release. [#76589](https://github.com/ClickHouse/ClickHouse/pull/76589) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Add log for HTTP Bad Request. [#76594](https://github.com/ClickHouse/ClickHouse/pull/76594) ([Christoph Wurm](https://github.com/cwurm)).
* tests: fix flaky test_parts_delete_zookeeper. [#76612](https://github.com/ClickHouse/ClickHouse/pull/76612) ([Azat Khuzhin](https://github.com/azat)).
* Refactor cache key calculation for joins. [#76613](https://github.com/ClickHouse/ClickHouse/pull/76613) ([Nikita Taranov](https://github.com/nickitat)).
* Fix Digest of local metadata (X) is not equal to in-memory digest (Y) error in Replicated database. [#76622](https://github.com/ClickHouse/ClickHouse/pull/76622) ([Azat Khuzhin](https://github.com/azat)).
* Docs: add `python3` and `rsync` to docs container. [#76629](https://github.com/ClickHouse/ClickHouse/pull/76629) ([Shaun Struwig](https://github.com/Blargian)).
* Update README.md - Update meetups. [#76631](https://github.com/ClickHouse/ClickHouse/pull/76631) ([Tanya Bragin](https://github.com/tbragin)).
* Previously `{server_uuid}` macro was substituted before the metadata was written to the disk, or to ZooKeeper. This could cause the `REPLICA_ALREADY_EXISTS` exception in Database Replicated when we add a new replica to the cluster. [#76636](https://github.com/ClickHouse/ClickHouse/pull/76636) ([Nikolay Degterinsky](https://github.com/evillique)).
* The test has an unreasonable small timeout (one second). [#76640](https://github.com/ClickHouse/ClickHouse/pull/76640) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Enable reverse key support in PartsSplitter. This fixes [#75928](https://github.com/ClickHouse/ClickHouse/issues/75928). [#76654](https://github.com/ClickHouse/ClickHouse/pull/76654) ([Amos Bird](https://github.com/amosbird)).
* Update CONTRIBUTING.md. [#76657](https://github.com/ClickHouse/ClickHouse/pull/76657) ([Robert Schulze](https://github.com/rschu1ze)).
* Improve right-bounded read support in ReadBufferFromEncryptedFile. [#76659](https://github.com/ClickHouse/ClickHouse/pull/76659) ([Vitaly Baranov](https://github.com/vitlibar)).
* Update Dockerfile. [#76665](https://github.com/ClickHouse/ClickHouse/pull/76665) ([Shaun Struwig](https://github.com/Blargian)).
* CI: Add log compression to praktika. [#76668](https://github.com/ClickHouse/ClickHouse/pull/76668) ([Max Kainov](https://github.com/maxknv)).
* Fix alignment * avoid unnecessary report auto reload on the job level. [#76672](https://github.com/ClickHouse/ClickHouse/pull/76672) ([Max Kainov](https://github.com/maxknv)).
* Changelog for 25.2. [#76673](https://github.com/ClickHouse/ClickHouse/pull/76673) ([Raúl Marín](https://github.com/Algunenano)).
* Fix build with `-DENABLE_LIBRARIES=0`. [#76678](https://github.com/ClickHouse/ClickHouse/pull/76678) ([Robert Schulze](https://github.com/rschu1ze)).
* Make the links on the report page for integration tests ordered by name. [#76679](https://github.com/ClickHouse/ClickHouse/pull/76679) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Fix crash in outer join. [#76683](https://github.com/ClickHouse/ClickHouse/pull/76683) ([Vladimir Cherkasov](https://github.com/vdimir)).
* CI: Add job report link to GH annotations. [#76685](https://github.com/ClickHouse/ClickHouse/pull/76685) ([Max Kainov](https://github.com/maxknv)).
* Support sending SYSTEM DROP DISK METADATA CACHE from public client binary to cloud. [#76689](https://github.com/ClickHouse/ClickHouse/pull/76689) ([Alexander Gololobov](https://github.com/davenger)).
* Use our own assertion handling for libcxx hardening. [#76701](https://github.com/ClickHouse/ClickHouse/pull/76701) ([Robert Schulze](https://github.com/rschu1ze)).
* Add comment for `Context::getDatabaseDisk`. [#76703](https://github.com/ClickHouse/ClickHouse/pull/76703) ([Tuan Pham Anh](https://github.com/tuanpach)).
* Fix OptimizeAndCompareChain Setting. [#76704](https://github.com/ClickHouse/ClickHouse/pull/76704) ([Shichao Jin](https://github.com/jsc0218)).
* Update s3.md. [#76710](https://github.com/ClickHouse/ClickHouse/pull/76710) ([otlxm](https://github.com/otlxm)).
* Replace manual pause/unpause calls in integration tests with a new RAII-style context manager to ensure containers are automatically unpaused on exit, prevent cascading failures when tests fail. [#76711](https://github.com/ClickHouse/ClickHouse/pull/76711) ([filimonov](https://github.com/filimonov)).
* Minor change in report. [#76714](https://github.com/ClickHouse/ClickHouse/pull/76714) ([Ilya Yatsishin](https://github.com/qoega)).
* Small BuzzHouse fix. [#76715](https://github.com/ClickHouse/ClickHouse/pull/76715) ([Pedro Ferreira](https://github.com/PedroTadim)).
* Update version_date.tsv and changelog after v25.1.6.34-stable. [#76716](https://github.com/ClickHouse/ClickHouse/pull/76716) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* CI: Refactoring pipeline configs. [#76719](https://github.com/ClickHouse/ClickHouse/pull/76719) ([Max Kainov](https://github.com/maxknv)).
* Fix logical error while using query_plan_join_swap_table. [#76722](https://github.com/ClickHouse/ClickHouse/pull/76722) ([Vladimir Cherkasov](https://github.com/vdimir)).
* Fix https://github.com/clickhouse/clickhouse/issues/76718. [#76723](https://github.com/ClickHouse/ClickHouse/pull/76723) ([Han Fei](https://github.com/hanfei1991)).
* Fix typos. [#76727](https://github.com/ClickHouse/ClickHouse/pull/76727) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Optimize PartsSplitter.cpp compilation. [#76734](https://github.com/ClickHouse/ClickHouse/pull/76734) ([Raúl Marín](https://github.com/Algunenano)).
* Backport settings changes to public. [#76736](https://github.com/ClickHouse/ClickHouse/pull/76736) ([Mikhail Artemenko](https://github.com/Michicosun)).
* fix flaky 03002_analyzer_prewhere. [#76742](https://github.com/ClickHouse/ClickHouse/pull/76742) ([Han Fei](https://github.com/hanfei1991)).
* Fix memcpy-bench after recent changes. [#76743](https://github.com/ClickHouse/ClickHouse/pull/76743) ([Raúl Marín](https://github.com/Algunenano)).
* Improve 02995_new_settings_history error messages on new features. [#76745](https://github.com/ClickHouse/ClickHouse/pull/76745) ([Raúl Marín](https://github.com/Algunenano)).
* Fix chcache build. [#76747](https://github.com/ClickHouse/ClickHouse/pull/76747) ([Raúl Marín](https://github.com/Algunenano)).
* Keep some files in sync. [#76748](https://github.com/ClickHouse/ClickHouse/pull/76748) ([Raúl Marín](https://github.com/Algunenano)).
* Commit message in sha selector * move branch and pr title info to status bar * do not test label support added to praktika hook * nightly jepsen workflow fixed. [#76749](https://github.com/ClickHouse/ClickHouse/pull/76749) ([Max Kainov](https://github.com/maxknv)).
* fix test_merge_tree_load_parts/test.py::test_merge_tree_load_parts_corrupted. [#76751](https://github.com/ClickHouse/ClickHouse/pull/76751) ([Sema Checherinda](https://github.com/CheSema)).
* Hopefully fix test_refreshable_mv_in_replicated_db flakiness. [#76760](https://github.com/ClickHouse/ClickHouse/pull/76760) ([Michael Kolupaev](https://github.com/al13n321)).
* Close: [#76795](https://github.com/ClickHouse/ClickHouse/issues/76795). [#76803](https://github.com/ClickHouse/ClickHouse/pull/76803) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Add test on prewhere with parallel replicas. [#76806](https://github.com/ClickHouse/ClickHouse/pull/76806) ([Nikita Taranov](https://github.com/nickitat)).
* CI: Disable cross-compile for ARM in release and backport. [#76808](https://github.com/ClickHouse/ClickHouse/pull/76808) ([Max Kainov](https://github.com/maxknv)).
* A better comment in generated files. [#76811](https://github.com/ClickHouse/ClickHouse/pull/76811) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Rename shard_name to name in config. [#76826](https://github.com/ClickHouse/ClickHouse/pull/76826) ([Igor Nikonov](https://github.com/devcrafter)).
* Parallelize data download for workflow-level data and job level-data * Fixes report links in commit statuses: Links in commit statuses point to the current sha, link in the PR comment keeps using "latest" alias for sha. [#76827](https://github.com/ClickHouse/ClickHouse/pull/76827) ([Max Kainov](https://github.com/maxknv)).
* RMV tests: fix flakiness caused by premature refreshes. [#76838](https://github.com/ClickHouse/ClickHouse/pull/76838) ([Nikita Fomichev](https://github.com/fm4v)).
* Update README.md. [#76865](https://github.com/ClickHouse/ClickHouse/pull/76865) ([Tyler Hannan](https://github.com/tylerhannan)).
* Add default disk properly in stress tests. [#76894](https://github.com/ClickHouse/ClickHouse/pull/76894) ([Antonio Andelic](https://github.com/antonio2368)).
* Fix TO parameter in Materialized views in BuzzHouse. [#76898](https://github.com/ClickHouse/ClickHouse/pull/76898) ([Pedro Ferreira](https://github.com/PedroTadim)).
* Update README.md. [#76902](https://github.com/ClickHouse/ClickHouse/pull/76902) ([Tyler Hannan](https://github.com/tylerhannan)).
* Settings backport. [#76915](https://github.com/ClickHouse/ClickHouse/pull/76915) ([Mikhail Artemenko](https://github.com/Michicosun)).
* fix flaky test_manipulate_statistics/test.py::test_single_node_wide. [#76917](https://github.com/ClickHouse/ClickHouse/pull/76917) ([Han Fei](https://github.com/hanfei1991)).
* Allow empty chunks in FinishSortingTransfrom. [#76919](https://github.com/ClickHouse/ClickHouse/pull/76919) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Don't run 03247_ghdata_string_to_json_alter and 03270_object_to_json_alter_wide_merge_tree in azure - they are too slow. [#76924](https://github.com/ClickHouse/ClickHouse/pull/76924) ([Pavel Kruglov](https://github.com/Avogar)).
* Fix flaky test_profile_events_s3. [#76935](https://github.com/ClickHouse/ClickHouse/pull/76935) ([Tuan Pham Anh](https://github.com/tuanpach)).
* Fix flaky test_keeper_three_nodes_start. [#76937](https://github.com/ClickHouse/ClickHouse/pull/76937) ([Tuan Pham Anh](https://github.com/tuanpach)).
* fix fast test 02783_parsedatetimebesteffort leap year. [#76940](https://github.com/ClickHouse/ClickHouse/pull/76940) ([Han Fei](https://github.com/hanfei1991)).
* Add licence key error code. [#76941](https://github.com/ClickHouse/ClickHouse/pull/76941) ([Max Kainov](https://github.com/maxknv)).
* Update version_date.tsv and changelog after v25.2.1.3085-stable. [#76943](https://github.com/ClickHouse/ClickHouse/pull/76943) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Mark more server settings as changable without restart. [#76946](https://github.com/ClickHouse/ClickHouse/pull/76946) ([Pavel Kruglov](https://github.com/Avogar)).
* Adjust a few old tests to flush individual logs. [#76949](https://github.com/ClickHouse/ClickHouse/pull/76949) ([Raúl Marín](https://github.com/Algunenano)).
* A follow-up for [#75724](https://github.com/ClickHouse/ClickHouse/issues/75724), the `checkout_submodule` just silently failed to fetch some submodules. [#76971](https://github.com/ClickHouse/ClickHouse/pull/76971) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Adding Tokyo and Melbourne meetups for March. [#76973](https://github.com/ClickHouse/ClickHouse/pull/76973) ([Cheryl Tuquib](https://github.com/chetuquib)).
* Closes [#69300](https://github.com/ClickHouse/ClickHouse/issues/69300). [#76982](https://github.com/ClickHouse/ClickHouse/pull/76982) ([Pavel Kruglov](https://github.com/Avogar)).
* Improve error message in MergeTreeDataPartWide::doCheckConsistency. [#76983](https://github.com/ClickHouse/ClickHouse/pull/76983) ([Pavel Kruglov](https://github.com/Avogar)).
* Fix https://github.com/clickhouse/clickhouse/issues/70319. [#76986](https://github.com/ClickHouse/ClickHouse/pull/76986) ([Han Fei](https://github.com/hanfei1991)).
* Update README.md - Meetup update. [#77007](https://github.com/ClickHouse/ClickHouse/pull/77007) ([Tanya Bragin](https://github.com/tbragin)).
* CI: Enable praktika workflow for CH Inc sync. [#77031](https://github.com/ClickHouse/ClickHouse/pull/77031) ([Max Kainov](https://github.com/maxknv)).
* Don't run 03271_ghdata_object_to_json_alter in azure, it's too slow. [#77037](https://github.com/ClickHouse/ClickHouse/pull/77037) ([Pavel Kruglov](https://github.com/Avogar)).
* Increase log level for dictionary loading. [#77052](https://github.com/ClickHouse/ClickHouse/pull/77052) ([Michael Lex](https://github.com/mlex)).
* Remove context from join step logical. [#77067](https://github.com/ClickHouse/ClickHouse/pull/77067) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* safer code for Statistics.cpp. [#77069](https://github.com/ClickHouse/ClickHouse/pull/77069) ([Han Fei](https://github.com/hanfei1991)).
* Replace some `__BYTE_ORDER__` macros. [#77072](https://github.com/ClickHouse/ClickHouse/pull/77072) ([Robert Schulze](https://github.com/rschu1ze)).
* Added BFloat16 test for found assertion. [#77095](https://github.com/ClickHouse/ClickHouse/pull/77095) ([Pedro Ferreira](https://github.com/PedroTadim)).
* CI: Merge Queue workflow migration to praktika. [#77098](https://github.com/ClickHouse/ClickHouse/pull/77098) ([Max Kainov](https://github.com/maxknv)).
* Remove broken test 02944_variant_as_common_type. [#77101](https://github.com/ClickHouse/ClickHouse/pull/77101) ([Pavel Kruglov](https://github.com/Avogar)).
* Update version_date.tsv and changelog after v25.1.7.20-stable. [#77103](https://github.com/ClickHouse/ClickHouse/pull/77103) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* ci: fix errors due to "unable to read tree" in performance tests. [#77104](https://github.com/ClickHouse/ClickHouse/pull/77104) ([Azat Khuzhin](https://github.com/azat)).
* Update trusted contributors. [#77105](https://github.com/ClickHouse/ClickHouse/pull/77105) ([Nikita Taranov](https://github.com/nickitat)).
* Start 25.3 section in SettingsChangesHistory. [#77106](https://github.com/ClickHouse/ClickHouse/pull/77106) ([Pavel Kruglov](https://github.com/Avogar)).
* Disable PR for mutations. [#77111](https://github.com/ClickHouse/ClickHouse/pull/77111) ([Igor Nikonov](https://github.com/devcrafter)).
* Remove the `force` flag for resizes. [#77112](https://github.com/ClickHouse/ClickHouse/pull/77112) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).
* Disable test `03367_bfloat16_tuple_final`. [#77130](https://github.com/ClickHouse/ClickHouse/pull/77130) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Rename `QueryCache` to `QueryResultCache` (only in C++). [#77134](https://github.com/ClickHouse/ClickHouse/pull/77134) ([zhongyuankai](https://github.com/zhongyuankai)).
* Add backup/restore functionality to BuzzHouse. [#77149](https://github.com/ClickHouse/ClickHouse/pull/77149) ([Pedro Ferreira](https://github.com/PedroTadim)).
* CI: Merge sync PR automatically from praktika hook. [#77150](https://github.com/ClickHouse/ClickHouse/pull/77150) ([Max Kainov](https://github.com/maxknv)).
* Update README.md with Sydney meetup details. [#77155](https://github.com/ClickHouse/ClickHouse/pull/77155) ([Cheryl Tuquib](https://github.com/chetuquib)).
* Update ANTLR grammar. [#77158](https://github.com/ClickHouse/ClickHouse/pull/77158) ([Mark Roberts](https://github.com/markandrus)).
* tests: fix test_quorum_inserts flakiness. [#77160](https://github.com/ClickHouse/ClickHouse/pull/77160) ([Azat Khuzhin](https://github.com/azat)).
* Sync changes in critical files. [#77161](https://github.com/ClickHouse/ClickHouse/pull/77161) ([Max Kainov](https://github.com/maxknv)).
* CI: Fix automatic sync merge. [#77164](https://github.com/ClickHouse/ClickHouse/pull/77164) ([Max Kainov](https://github.com/maxknv)).
* tests: fix 02982_perf_introspection_for_inserts flakiness. [#77167](https://github.com/ClickHouse/ClickHouse/pull/77167) ([Azat Khuzhin](https://github.com/azat)).
* This closes [#45389](https://github.com/ClickHouse/ClickHouse/issues/45389). [#77169](https://github.com/ClickHouse/ClickHouse/pull/77169) ([Pedro Ferreira](https://github.com/PedroTadim)).
* This change is needed to be able to detect them written to logs and system tables. [#77173](https://github.com/ClickHouse/ClickHouse/pull/77173) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Fix uninitialized-value `CoordinationZnode::last_success_duration` in `RefreshTask`. [#77174](https://github.com/ClickHouse/ClickHouse/pull/77174) ([Tuan Pham Anh](https://github.com/tuanpach)).
* ci: cleanup system.*_log artifacts. [#77175](https://github.com/ClickHouse/ClickHouse/pull/77175) ([Azat Khuzhin](https://github.com/azat)).
* tests: retry kazoo operations and idempotence of [zoo]keeper tests. [#77178](https://github.com/ClickHouse/ClickHouse/pull/77178) ([Azat Khuzhin](https://github.com/azat)).
* Link fuzzers with contribs. [#77179](https://github.com/ClickHouse/ClickHouse/pull/77179) ([Mikhail Artemenko](https://github.com/Michicosun)).
* Add join settings to serialization settings. [#77180](https://github.com/ClickHouse/ClickHouse/pull/77180) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix stopping server on OSX (shutdown() returns ENOTCONN for socket blocked in accept()). [#77182](https://github.com/ClickHouse/ClickHouse/pull/77182) ([Azat Khuzhin](https://github.com/azat)).
* remove a left over comment from test. [#77184](https://github.com/ClickHouse/ClickHouse/pull/77184) ([Bharat Nallan](https://github.com/bharatnc)).
* CI: Remove unit tests from Merge Queue. [#77186](https://github.com/ClickHouse/ClickHouse/pull/77186) ([Max Kainov](https://github.com/maxknv)).
* tests: avoid joining thread from itself in coordinator unit tests. [#77190](https://github.com/ClickHouse/ClickHouse/pull/77190) ([Azat Khuzhin](https://github.com/azat)).
* Add debug logging to S3Queue metadata. [#77209](https://github.com/ClickHouse/ClickHouse/pull/77209) ([Julia Kartseva](https://github.com/jkartseva)).
* Fix flaky test_old_dirs_cleanup. [#77211](https://github.com/ClickHouse/ClickHouse/pull/77211) ([Tuan Pham Anh](https://github.com/tuanpach)).
* CI: Change job order in PR. [#77223](https://github.com/ClickHouse/ClickHouse/pull/77223) ([Max Kainov](https://github.com/maxknv)).
* "ci-performance" label to run only performance related jobs in PR CI - set red Mergeable check to prohibit merge with "ci-performance" label. [#77228](https://github.com/ClickHouse/ClickHouse/pull/77228) ([Max Kainov](https://github.com/maxknv)).
* Fix flaky test_s3_table_function_with_http_proxy. [#77230](https://github.com/ClickHouse/ClickHouse/pull/77230) ([Tuan Pham Anh](https://github.com/tuanpach)).
* Fix flaky test_keeper_auth. [#77232](https://github.com/ClickHouse/ClickHouse/pull/77232) ([Tuan Pham Anh](https://github.com/tuanpach)).
* CI: Fix for sync PR auto merge. [#77268](https://github.com/ClickHouse/ClickHouse/pull/77268) ([Max Kainov](https://github.com/maxknv)).
* Remove more fields from JoinLogical. [#77269](https://github.com/ClickHouse/ClickHouse/pull/77269) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix stuck mongodb queries by adding a timeout check. [#77272](https://github.com/ClickHouse/ClickHouse/pull/77272) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).
* Follow-up to [#69236](https://github.com/ClickHouse/ClickHouse/issues/69236). [#77280](https://github.com/ClickHouse/ClickHouse/pull/77280) ([Robert Schulze](https://github.com/rschu1ze)).
* CI: Migrate Release and Backport CIs to praktika. [#77282](https://github.com/ClickHouse/ClickHouse/pull/77282) ([Max Kainov](https://github.com/maxknv)).
* CI: Yet another Sync auto merge fix. [#77285](https://github.com/ClickHouse/ClickHouse/pull/77285) ([Max Kainov](https://github.com/maxknv)).
* Fix 1852 flaky second attempt. [#77286](https://github.com/ClickHouse/ClickHouse/pull/77286) ([Shichao Jin](https://github.com/jsc0218)).
* Minor follow-up to [#69236](https://github.com/ClickHouse/ClickHouse/issues/69236). [#77293](https://github.com/ClickHouse/ClickHouse/pull/77293) ([Robert Schulze](https://github.com/rschu1ze)).
* Remove some unused includes. [#77299](https://github.com/ClickHouse/ClickHouse/pull/77299) ([Igor Nikonov](https://github.com/devcrafter)).
* StackTrace(): capture source line where it was called correctly. [#77302](https://github.com/ClickHouse/ClickHouse/pull/77302) ([Igor Nikonov](https://github.com/devcrafter)).
* CI: Enable test docker images handling by praktika. [#77306](https://github.com/ClickHouse/ClickHouse/pull/77306) ([Max Kainov](https://github.com/maxknv)).
* Sync parser files. [#77308](https://github.com/ClickHouse/ClickHouse/pull/77308) ([Raúl Marín](https://github.com/Algunenano)).
* Adjust a few old tests to flush individual logs. [#77315](https://github.com/ClickHouse/ClickHouse/pull/77315) ([Raúl Marín](https://github.com/Algunenano)).
* Make a few tests parallel ready. [#77322](https://github.com/ClickHouse/ClickHouse/pull/77322) ([Raúl Marín](https://github.com/Algunenano)).
* ci: use TSVWithNamesAndTypes for system.*_log artifacts. [#77338](https://github.com/ClickHouse/ClickHouse/pull/77338) ([Azat Khuzhin](https://github.com/azat)).
* tests: fix 03357_replacing_min_age_cleanup flakiness. [#77341](https://github.com/ClickHouse/ClickHouse/pull/77341) ([Azat Khuzhin](https://github.com/azat)).
* Update README.md - update meetups. [#77342](https://github.com/ClickHouse/ClickHouse/pull/77342) ([Tanya Bragin](https://github.com/tbragin)).
* tests: unify object storage tags exceptions. [#77344](https://github.com/ClickHouse/ClickHouse/pull/77344) ([Azat Khuzhin](https://github.com/azat)).
* tests: better mocks for S3 (add some logging) and decrease tune failure rate. [#77345](https://github.com/ClickHouse/ClickHouse/pull/77345) ([Azat Khuzhin](https://github.com/azat)).
* tests: use more reliable hits_s3 for 00156_max_execution_speed_sample_merge. [#77346](https://github.com/ClickHouse/ClickHouse/pull/77346) ([Azat Khuzhin](https://github.com/azat)).
* Fix doc building runner image. [#77347](https://github.com/ClickHouse/ClickHouse/pull/77347) ([scanhex12](https://github.com/scanhex12)).
* Fix setting type. [#77349](https://github.com/ClickHouse/ClickHouse/pull/77349) ([Antonio Andelic](https://github.com/antonio2368)).
* Follow-up for query condition cache. [#77356](https://github.com/ClickHouse/ClickHouse/pull/77356) ([Robert Schulze](https://github.com/rschu1ze)).
* Update README.md to remove Melbourne meetup on March 27. [#77367](https://github.com/ClickHouse/ClickHouse/pull/77367) ([Cheryl Tuquib](https://github.com/chetuquib)).
* CI: Support GH Vars in praktika. [#77373](https://github.com/ClickHouse/ClickHouse/pull/77373) ([Max Kainov](https://github.com/maxknv)).
* tests: fix 01175_distributed_ddl_output_mode_long flakiness. [#77376](https://github.com/ClickHouse/ClickHouse/pull/77376) ([Azat Khuzhin](https://github.com/azat)).
* tests: fix 02161_addressToLineWithInlines (incorrect SYSTEM FLUSH LOGS). [#77393](https://github.com/ClickHouse/ClickHouse/pull/77393) ([Azat Khuzhin](https://github.com/azat)).
* Use fmt::format instead of std::format. [#77402](https://github.com/ClickHouse/ClickHouse/pull/77402) ([Raúl Marín](https://github.com/Algunenano)).
* ci: fix collecting system.*_log for Replicated and SC. [#77409](https://github.com/ClickHouse/ClickHouse/pull/77409) ([Azat Khuzhin](https://github.com/azat)).
* ci: detect dead server after stress testing. [#77411](https://github.com/ClickHouse/ClickHouse/pull/77411) ([Azat Khuzhin](https://github.com/azat)).
* Next BuzzHouse update. [#77415](https://github.com/ClickHouse/ClickHouse/pull/77415) ([Pedro Ferreira](https://github.com/PedroTadim)).
* Increase timeout for cancel_backup integration tests. [#77421](https://github.com/ClickHouse/ClickHouse/pull/77421) ([Raúl Marín](https://github.com/Algunenano)).
* Decouple JOIN serialization. [#77423](https://github.com/ClickHouse/ClickHouse/pull/77423) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* ci: run stress tests on tests/ci/stress.py changes. [#77425](https://github.com/ClickHouse/ClickHouse/pull/77425) ([Azat Khuzhin](https://github.com/azat)).
* Update version_date.tsv and changelog after v24.12.6.70-stable. [#77426](https://github.com/ClickHouse/ClickHouse/pull/77426) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Split materialized pg tests to be less heavy. [#77429](https://github.com/ClickHouse/ClickHouse/pull/77429) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Split s3queue tests to be less heavy. [#77432](https://github.com/ClickHouse/ClickHouse/pull/77432) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Add CREATE TABLE retries in `test_replicated_merge_tree_s3_zero_copy`. [#77433](https://github.com/ClickHouse/ClickHouse/pull/77433) ([Sergei Trifonov](https://github.com/serxa)).
* ci: secure reports dropdown (preserve tags). [#77437](https://github.com/ClickHouse/ClickHouse/pull/77437) ([Azat Khuzhin](https://github.com/azat)).
* Forbid empty databases (leaked via default_database). [#77438](https://github.com/ClickHouse/ClickHouse/pull/77438) ([Azat Khuzhin](https://github.com/azat)).
* Remove some dependencies from Settings.cpp. [#77442](https://github.com/ClickHouse/ClickHouse/pull/77442) ([Raúl Marín](https://github.com/Algunenano)).
* Add perf test for BFloat16 arithmetics. [#77449](https://github.com/ClickHouse/ClickHouse/pull/77449) ([Robert Schulze](https://github.com/rschu1ze)).
* Fix compatibility with newer rustup (bump corrosion). [#77454](https://github.com/ClickHouse/ClickHouse/pull/77454) ([Azat Khuzhin](https://github.com/azat)).
* Update README.md. [#77466](https://github.com/ClickHouse/ClickHouse/pull/77466) ([Tyler Hannan](https://github.com/tylerhannan)).
* Fix retriable exception. [#77481](https://github.com/ClickHouse/ClickHouse/pull/77481) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Generate new certificates for test_reload_certificate. [#77495](https://github.com/ClickHouse/ClickHouse/pull/77495) ([Pervakov Grigorii](https://github.com/GrigoryPervakov)).
* Bump AWS to decrease severity of "Encountered enum member {} which is not modeled in your clients". [#77518](https://github.com/ClickHouse/ClickHouse/pull/77518) ([Azat Khuzhin](https://github.com/azat)).
* Now all the AST formatting functions are located inside the IAST. The names of the methods speak for themselves e.g. `formatWithSecrets*` should be used with caution. [#77523](https://github.com/ClickHouse/ClickHouse/pull/77523) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Fix documentation for format settings. [#77526](https://github.com/ClickHouse/ClickHouse/pull/77526) ([Azat Khuzhin](https://github.com/azat)).
* Various CI fixes to simplify running it locally. [#77543](https://github.com/ClickHouse/ClickHouse/pull/77543) ([Konstantin Bogdanov](https://github.com/thevar1able)).
* Update README.md with Kuala Lumpur meetup on April 16. [#77551](https://github.com/ClickHouse/ClickHouse/pull/77551) ([Cheryl Tuquib](https://github.com/chetuquib)).
* Update version_date.tsv and changelog after v25.1.8.25-stable. [#77560](https://github.com/ClickHouse/ClickHouse/pull/77560) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Virtualize IDisk::isDirectoryEmpty(). [#77561](https://github.com/ClickHouse/ClickHouse/pull/77561) ([Azat Khuzhin](https://github.com/azat)).
* Update version_date.tsv and changelog after v25.2.2.39-stable. [#77566](https://github.com/ClickHouse/ClickHouse/pull/77566) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Make Distributed storage stateless compatible. [#77573](https://github.com/ClickHouse/ClickHouse/pull/77573) ([Azat Khuzhin](https://github.com/azat)).
* tests: attempt to fix flakiness of tests with MySQL client with expect (--no-auto-rehash). [#77599](https://github.com/ClickHouse/ClickHouse/pull/77599) ([Azat Khuzhin](https://github.com/azat)).
* Fix incremental builds for Rust. [#77604](https://github.com/ClickHouse/ClickHouse/pull/77604) ([Azat Khuzhin](https://github.com/azat)).
* Fix "While sending batch" error message is too long. [#77609](https://github.com/ClickHouse/ClickHouse/pull/77609) ([zhongyuankai](https://github.com/zhongyuankai)).
* CI: Fix for invalid CI cache reuse. [#77610](https://github.com/ClickHouse/ClickHouse/pull/77610) ([Max Kainov](https://github.com/maxknv)).
* Fix hive partitioning flakiness for aarch64. [#77611](https://github.com/ClickHouse/ClickHouse/pull/77611) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).
* Fix inconsistent formatting of lambda. Closes [#68454](https://github.com/ClickHouse/ClickHouse/issues/68454). [#77621](https://github.com/ClickHouse/ClickHouse/pull/77621) ([Vitaly Baranov](https://github.com/vitlibar)).
* Follow up to [#74884](https://github.com/ClickHouse/ClickHouse/issues/74884). [#77628](https://github.com/ClickHouse/ClickHouse/pull/77628) ([Kseniia Sumarokova](https://github.com/kssenii)).
* CI: Add configs for clickbench sqltest images. [#77637](https://github.com/ClickHouse/ClickHouse/pull/77637) ([Max Kainov](https://github.com/maxknv)).
* Less aggressive flaky check of integration tests. [#77643](https://github.com/ClickHouse/ClickHouse/pull/77643) ([Azat Khuzhin](https://github.com/azat)).
* This closes: [#77660](https://github.com/ClickHouse/ClickHouse/issues/77660). [#77645](https://github.com/ClickHouse/ClickHouse/pull/77645) ([Pedro Ferreira](https://github.com/PedroTadim)).
* Maybe fix materialized pg tests flakyness. [#77649](https://github.com/ClickHouse/ClickHouse/pull/77649) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Make the move operator for class `ColumnDefault` truly noexcept. [#77670](https://github.com/ClickHouse/ClickHouse/pull/77670) ([Vitaly Baranov](https://github.com/vitlibar)).
* CI: Style check refactored. [#77673](https://github.com/ClickHouse/ClickHouse/pull/77673) ([Max Kainov](https://github.com/maxknv)).
* docs: add a note that keep_alive_timeout server setting is about HTTP. [#77679](https://github.com/ClickHouse/ClickHouse/pull/77679) ([Azat Khuzhin](https://github.com/azat)).
* Bump usearch to v2.17.2. [#77689](https://github.com/ClickHouse/ClickHouse/pull/77689) ([Robert Schulze](https://github.com/rschu1ze)).
* Update README.md. [#77697](https://github.com/ClickHouse/ClickHouse/pull/77697) ([Tyler Hannan](https://github.com/tylerhannan)).
* The reason was I was letting parameter expressions be selected in the predicate and join generations. Ignore those for now. [#77726](https://github.com/ClickHouse/ClickHouse/pull/77726) ([Pedro Ferreira](https://github.com/PedroTadim)).
* tests: retry delete in ZooKeeper/keeper in integration tests. [#77749](https://github.com/ClickHouse/ClickHouse/pull/77749) ([Azat Khuzhin](https://github.com/azat)).
* tests: fix 03293_tty_binary_confirmation flakiness. [#77753](https://github.com/ClickHouse/ClickHouse/pull/77753) ([Azat Khuzhin](https://github.com/azat)).
* Partially fix restoring backups containing refreshable materialized views. It could fail with Replicated database + SharedMergeTree table engine combination. Now it doesn't fail, but the refresh target table often ends up empty, requiring another refresh (can be triggered manually using `SYSTEM REFRESH VIEW <name>`). [#77770](https://github.com/ClickHouse/ClickHouse/pull/77770) ([Michael Kolupaev](https://github.com/al13n321)).
* Backported in [#77833](https://github.com/ClickHouse/ClickHouse/issues/77833): Add new setting blocks. [#77796](https://github.com/ClickHouse/ClickHouse/pull/77796) ([Raúl Marín](https://github.com/Algunenano)).
* Backported in [#77935](https://github.com/ClickHouse/ClickHouse/issues/77935): Mark query condition cache as refreshable. [#77809](https://github.com/ClickHouse/ClickHouse/pull/77809) ([Robert Schulze](https://github.com/rschu1ze)).
* Backported in [#77928](https://github.com/ClickHouse/ClickHouse/issues/77928): Make `nats_max_reconnect` obsolete. [#77829](https://github.com/ClickHouse/ClickHouse/pull/77829) ([János Benjamin Antal](https://github.com/antaljanosbenjamin)).
* Backported in [#77864](https://github.com/ClickHouse/ClickHouse/issues/77864): Less frequent logs from CgroupsReader. [#77831](https://github.com/ClickHouse/ClickHouse/pull/77831) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Backported in [#77915](https://github.com/ClickHouse/ClickHouse/issues/77915): Some tweaks for 25.3 settings. [#77839](https://github.com/ClickHouse/ClickHouse/pull/77839) ([Raúl Marín](https://github.com/Algunenano)).
* Backported in [#77971](https://github.com/ClickHouse/ClickHouse/issues/77971): Revert [#77584](https://github.com/ClickHouse/ClickHouse/issues/77584). [#77897](https://github.com/ClickHouse/ClickHouse/pull/77897) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Backported in [#78010](https://github.com/ClickHouse/ClickHouse/issues/78010): Query condition cache: Fix race in debug build + add comment about error handling. [#77988](https://github.com/ClickHouse/ClickHouse/pull/77988) ([Robert Schulze](https://github.com/rschu1ze)).

