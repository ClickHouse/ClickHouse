---
sidebar_position: 1
sidebar_label: 2024
---

# 2024 Changelog

### ClickHouse release v24.7.1.2915-stable (a37d2d43da7) FIXME as compared to v24.7.1.1-new (aa023477a92)

#### Backward Incompatible Change
* Change binary serialization of Variant data type: add `compact` mode to avoid writing the same discriminator multiple times for granules with single variant or with only NULL values. Add MergeTree setting `use_compact_variant_discriminators_serialization` that is enabled by default. Note that Variant type is still experimental and backward-incompatible change in serialization is ok. [#62774](https://github.com/ClickHouse/ClickHouse/pull/62774) ([Kruglov Pavel](https://github.com/Avogar)).
* Forbid `CREATE MATERIALIZED VIEW ... ENGINE Replicated*MergeTree POPULATE AS SELECT ...` with Replicated databases. [#63963](https://github.com/ClickHouse/ClickHouse/pull/63963) ([vdimir](https://github.com/vdimir)).
* `clickhouse-keeper-client` will only accept paths in string literals, such as `ls '/hello/world'`, not bare strings such as `ls /hello/world`. [#65494](https://github.com/ClickHouse/ClickHouse/pull/65494) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Metric `KeeperOutstandingRequets` was renamed to `KeeperOutstandingRequests`. This fixes a typo reported in [#66179](https://github.com/ClickHouse/ClickHouse/issues/66179). [#66206](https://github.com/ClickHouse/ClickHouse/pull/66206) ([Robert Schulze](https://github.com/rschu1ze)).
* Remove `is_deterministic` field from the `system.functions` table. [#66630](https://github.com/ClickHouse/ClickHouse/pull/66630) ([Alexey Milovidov](https://github.com/alexey-milovidov)).

#### New Feature
* Extend function `tuple` to construct named tuples in query. Introduce function `tupleNames` to extract names from tuples. [#54881](https://github.com/ClickHouse/ClickHouse/pull/54881) ([Amos Bird](https://github.com/amosbird)).
* `ASOF JOIN` support for `full_sorting_join` algorithm Close [#54493](https://github.com/ClickHouse/ClickHouse/issues/54493). [#55051](https://github.com/ClickHouse/ClickHouse/pull/55051) ([vdimir](https://github.com/vdimir)).
* A new table function, `fuzzQuery,` was added. This function allows you to modify a given query string with random variations. Example: `SELECT query FROM fuzzQuery('SELECT 1');`. [#62103](https://github.com/ClickHouse/ClickHouse/pull/62103) ([pufit](https://github.com/pufit)).
* Add new window function `percent_rank`. [#62747](https://github.com/ClickHouse/ClickHouse/pull/62747) ([lgbo](https://github.com/lgbo-ustc)).
* Support JWT authentication in `clickhouse-client`. [#62829](https://github.com/ClickHouse/ClickHouse/pull/62829) ([Konstantin Bogdanov](https://github.com/thevar1able)).
* Add SQL functions `changeYear`, `changeMonth`, `changeDay`, `changeHour`, `changeMinute`, `changeSecond`. For example, `SELECT changeMonth(toDate('2024-06-14'), 7)` returns date `2024-07-14`. [#63186](https://github.com/ClickHouse/ClickHouse/pull/63186) ([cucumber95](https://github.com/cucumber95)).
* Introduce startup scripts, which allow the execution of preconfigured queries at the startup stage. [#64889](https://github.com/ClickHouse/ClickHouse/pull/64889) ([pufit](https://github.com/pufit)).
* Support accept_invalid_certificate in client's config in order to allow for client to connect over secure TCP to a server running with self-signed certificate - can be used as a shorthand for corresponding `openSSL` client settings `verificationMode=none` + `invalidCertificateHandler.name=AcceptCertificateHandler`. [#65238](https://github.com/ClickHouse/ClickHouse/pull/65238) ([peacewalker122](https://github.com/peacewalker122)).
* Add system.error_log which contains history of error values from table system.errors, periodically flushed to disk. [#65381](https://github.com/ClickHouse/ClickHouse/pull/65381) ([Pablo Marcos](https://github.com/pamarcos)).
* Add aggregate function `groupConcat`. About the same as `arrayStringConcat( groupArray(column), ',')` Can receive 2 parameters: a string delimiter and the number of elements to be processed. [#65451](https://github.com/ClickHouse/ClickHouse/pull/65451) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).
* Add AzureQueue storage. [#65458](https://github.com/ClickHouse/ClickHouse/pull/65458) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Add a new setting to disable/enable writing page index into parquet files. [#65475](https://github.com/ClickHouse/ClickHouse/pull/65475) ([lgbo](https://github.com/lgbo-ustc)).
* Allow system administrators to configure `logger.console_log_level`. [#65559](https://github.com/ClickHouse/ClickHouse/pull/65559) ([Azat Khuzhin](https://github.com/azat)).
* Automatically append a wildcard `*` to the end of a directory path with table function `file`. [#66019](https://github.com/ClickHouse/ClickHouse/pull/66019) ([Zhidong (David) Guo](https://github.com/Gun9niR)).
* Add `--memory-usage` option to client in non interactive mode. [#66393](https://github.com/ClickHouse/ClickHouse/pull/66393) ([vdimir](https://github.com/vdimir)).

#### Performance Improvement
* Enable `optimize_functions_to_subcolumns` by default. [#58661](https://github.com/ClickHouse/ClickHouse/pull/58661) ([Anton Popov](https://github.com/CurtizJ)).
* Replace int to string algorithm with a faster one (from a modified amdn/itoa to a modified jeaiii/itoa). [#61661](https://github.com/ClickHouse/ClickHouse/pull/61661) ([Raúl Marín](https://github.com/Algunenano)).
* Sizes of hash tables created by join (`parallel_hash` algorithm) is collected and cached now. This information will be used to preallocate space in hash tables for subsequent query executions and save time on hash table resizes. [#64553](https://github.com/ClickHouse/ClickHouse/pull/64553) ([Nikita Taranov](https://github.com/nickitat)).
* Optimized queries with `ORDER BY` primary key and `WHERE` that have a condition with high selectivity by using of buffering. It is controlled by setting `read_in_order_use_buffering` (enabled by default) and can increase memory usage of query. [#64607](https://github.com/ClickHouse/ClickHouse/pull/64607) ([Anton Popov](https://github.com/CurtizJ)).
* Improve performance of loading `plain_rewritable` metadata. [#65634](https://github.com/ClickHouse/ClickHouse/pull/65634) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Attaching tables on read-only disks will use fewer resources by not loading outdated parts. [#65635](https://github.com/ClickHouse/ClickHouse/pull/65635) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Support minmax hyperrectangle for Set indices. [#65676](https://github.com/ClickHouse/ClickHouse/pull/65676) ([AntiTopQuark](https://github.com/AntiTopQuark)).
* Unload primary index of outdated parts to reduce total memory usage. [#65852](https://github.com/ClickHouse/ClickHouse/pull/65852) ([Anton Popov](https://github.com/CurtizJ)).
* Functions `replaceRegexpAll` and `replaceRegexpOne` are now significantly faster if the pattern is trivial, i.e. contains no metacharacters, pattern classes, flags, grouping characters etc. (Thanks to Taiyang Li). [#66185](https://github.com/ClickHouse/ClickHouse/pull/66185) ([Robert Schulze](https://github.com/rschu1ze)).

#### Improvement
* Support rocksdb as backend storage of keeper. [#56626](https://github.com/ClickHouse/ClickHouse/pull/56626) ([Han Fei](https://github.com/hanfei1991)).
* The setting `optimize_trivial_insert_select` is disabled by default. In most cases, it should be beneficial. Nevertheless, if you are seeing slower INSERT SELECT or increased memory usage, you can enable it back or `SET compatibility = '24.6'`. [#58970](https://github.com/ClickHouse/ClickHouse/pull/58970) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Print stacktrace and diagnostic info if `clickhouse-client` or `clickhouse-local` crashes. [#61109](https://github.com/ClickHouse/ClickHouse/pull/61109) ([Alexander Tokmakov](https://github.com/tavplubix)).
* The result of `SHOW INDEX | INDEXES | INDICES | KEYS` was previously sorted by the primary key column names. Since this was unintuitive, the result is now sorted by the position of the primary key columns within the primary key. [#61131](https://github.com/ClickHouse/ClickHouse/pull/61131) ([Robert Schulze](https://github.com/rschu1ze)).
* This PR changes how deduplication for MV works. Fixed a lot of cases like: - on destination table: data is split for 2 or more blocks and that blocks is considered as duplicate when that block is inserted in parallel. - on MV destination table: the equal blocks are deduplicated, that happens when MV often produces equal data as a result for different input data due to performing aggregation. - on MV destination table: the equal blocks which comes from different MV are deduplicated. [#61601](https://github.com/ClickHouse/ClickHouse/pull/61601) ([Sema Checherinda](https://github.com/CheSema)).
* Allow matching column names in a case insensitive manner when reading json files (`input_format_json_case_insensitive_column_matching`). [#61750](https://github.com/ClickHouse/ClickHouse/pull/61750) ([kevinyhzou](https://github.com/KevinyhZou)).
* Support reading partitioned data DeltaLake data. Infer DeltaLake schema by reading metadata instead of data. [#63201](https://github.com/ClickHouse/ClickHouse/pull/63201) ([Kseniia Sumarokova](https://github.com/kssenii)).
* In composable protocols TLS layer accepted only `certificateFile` and `privateKeyFile` parameters. https://clickhouse.com/docs/en/operations/settings/composable-protocols. [#63985](https://github.com/ClickHouse/ClickHouse/pull/63985) ([Anton Ivashkin](https://github.com/ianton-ru)).
* Make an interactive client for clickhouse-disks, add local disk from the local directory. Fixes [#56791](https://github.com/ClickHouse/ClickHouse/issues/56791). [#64446](https://github.com/ClickHouse/ClickHouse/pull/64446) ([Daniil Ivanik](https://github.com/divanik)).
* Added profile event `SelectQueriesWithPrimaryKeyUsage` which indicates how many SELECT queries use the primary key to evaluate the WHERE clause. [#64492](https://github.com/ClickHouse/ClickHouse/pull/64492) ([0x01f](https://github.com/0xfei)).
* `StorageS3Queue` related fixes and improvements. Deduce a default value of `s3queue_processing_threads_num` according to the number of physical cpu cores on the server (instead of the previous default value as 1). Set default value of `s3queue_loading_retries` to 10. Fix possible vague "Uncaught exception" in exception column of `system.s3queue`. Do not increment retry count on `MEMORY_LIMIT_EXCEEDED` exception. Move files commit to a stage after insertion into table fully finished to avoid files being commited while not inserted. Add settings `s3queue_max_processed_files_before_commit`, `s3queue_max_processed_rows_before_commit`, `s3queue_max_processed_bytes_before_commit`, `s3queue_max_processing_time_sec_before_commit`, to better control commit and flush time. [#65046](https://github.com/ClickHouse/ClickHouse/pull/65046) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fixed broken multiple columns aggregation on s390x. [#65062](https://github.com/ClickHouse/ClickHouse/pull/65062) ([Harry Lee](https://github.com/HarryLeeIBM)).
* Support aliases in parameterized view function (only new analyzer). [#65190](https://github.com/ClickHouse/ClickHouse/pull/65190) ([Kseniia Sumarokova](https://github.com/kssenii)).
* S3. reduce retires time for queries, increase retries count for backups. 8.5 minutes and 100 retires for queries, 1.2 hours and 1000 retries for backup restore. [#65232](https://github.com/ClickHouse/ClickHouse/pull/65232) ([Sema Checherinda](https://github.com/CheSema)).
* Updated to mask account key in logs in azureBlobStorage. [#65273](https://github.com/ClickHouse/ClickHouse/pull/65273) ([SmitaRKulkarni](https://github.com/SmitaRKulkarni)).
* Partition pruning for `IN` predicates when filter expression is a part of `PARTITION BY` expression. [#65335](https://github.com/ClickHouse/ClickHouse/pull/65335) ([Eduard Karacharov](https://github.com/korowa)).
* Add system tables with main information about all detached tables. [#65400](https://github.com/ClickHouse/ClickHouse/pull/65400) ([Konstantin Morozov](https://github.com/k-morozov)).
* Add support for `cluster_for_parallel_replicas` when using custom key parallel replicas. It allows you to use parallel replicas with custom key with MergeTree tables. [#65453](https://github.com/ClickHouse/ClickHouse/pull/65453) ([Antonio Andelic](https://github.com/antonio2368)).
* Support query plan LIMIT optimization. Support LIMIT pushdown for PostgreSQL storage and table function. [#65454](https://github.com/ClickHouse/ClickHouse/pull/65454) ([Maksim Kita](https://github.com/kitaisreal)).
* Arraymin/max can be applicable to all data types that are comparable. [#65455](https://github.com/ClickHouse/ClickHouse/pull/65455) ([pn](https://github.com/chloro-pn)).
* Improved memory accounting for cgroups v2 to exclude the amount occupied by the page cache. [#65470](https://github.com/ClickHouse/ClickHouse/pull/65470) ([Nikita Taranov](https://github.com/nickitat)).
* Do not create format settings for each rows when serializing chunks to insert to EmbeddedRocksDB table. [#65474](https://github.com/ClickHouse/ClickHouse/pull/65474) ([Duc Canh Le](https://github.com/canhld94)).
* Fixed out-of-range exception in parsing Dwarf5 on s390x. [#65501](https://github.com/ClickHouse/ClickHouse/pull/65501) ([Harry Lee](https://github.com/HarryLeeIBM)).
* Reduce `clickhouse-local` prompt to just `:)`. `getFQDNOrHostName()` takes too long on macOS, and we don't want a hostname in the prompt for `clickhouse-local` anyway. [#65510](https://github.com/ClickHouse/ClickHouse/pull/65510) ([Konstantin Bogdanov](https://github.com/thevar1able)).
* Avoid printing a message from jemalloc about per-CPU arenas on low-end virtual machines. [#65532](https://github.com/ClickHouse/ClickHouse/pull/65532) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Disable filesystem cache background download by default. It will be enabled back when we fix the issue with possible "Memory limit exceeded" because memory deallocation is done outside of query context (while buffer is allocated inside of query context) if we use background download threads. Plus we need to add a separate setting to define max size to download for background workers (currently it is limited by max_file_segment_size, which might be too big). [#65534](https://github.com/ClickHouse/ClickHouse/pull/65534) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Add new option to config `<config_reload_interval_ms>` which allow to specify how often clickhouse will reload config. [#65545](https://github.com/ClickHouse/ClickHouse/pull/65545) ([alesapin](https://github.com/alesapin)).
* Implement binary encoding for ClickHouse data types and add its specification in docs. Use it in Dynamic binary serialization, allow to use it in RowBinaryWithNamesAndTypes and Native formats under settings. [#65546](https://github.com/ClickHouse/ClickHouse/pull/65546) ([Kruglov Pavel](https://github.com/Avogar)).
* Improved ZooKeeper load balancing. The current session doesn't expire until the optimal nodes become available despite `fallback_session_lifetime`. Added support for AZ-aware balancing. [#65570](https://github.com/ClickHouse/ClickHouse/pull/65570) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Server settings `compiled_expression_cache_size` and `compiled_expression_cache_elements_size` are now shown in `system.server_settings`. [#65584](https://github.com/ClickHouse/ClickHouse/pull/65584) ([Robert Schulze](https://github.com/rschu1ze)).
* When lightweight delete happens on a table with projection(s), users have choices either throw an exception (by default) or drop the projection lightweight delete would happen. [#65594](https://github.com/ClickHouse/ClickHouse/pull/65594) ([jsc0218](https://github.com/jsc0218)).
* Add support for user identification based on x509 SubjectAltName extension. [#65626](https://github.com/ClickHouse/ClickHouse/pull/65626) ([Anton Kozlov](https://github.com/tonickkozlov)).
* `clickhouse-local` will respect the `max_server_memory_usage` and `max_server_memory_usage_to_ram_ratio` from the configuration file. It will also set the max memory usage to 90% of the system memory by default, like `clickhouse-server` does. This closes [#65695](https://github.com/ClickHouse/ClickHouse/issues/65695). [#65697](https://github.com/ClickHouse/ClickHouse/pull/65697) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Add a script to backup your files to ClickHouse. This is strange, but works. [#65699](https://github.com/ClickHouse/ClickHouse/pull/65699) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* PostgreSQL source support cancel. [#65722](https://github.com/ClickHouse/ClickHouse/pull/65722) ([Maksim Kita](https://github.com/kitaisreal)).
* Make allow_experimental_analyzer be controlled by the initiator for distributed queries. This ensures compatibility and correctness during operations in mixed version clusters. [#65777](https://github.com/ClickHouse/ClickHouse/pull/65777) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Respect cgroup CPU limit in Keeper. [#65819](https://github.com/ClickHouse/ClickHouse/pull/65819) ([Antonio Andelic](https://github.com/antonio2368)).
* Allow to use `concat` function with empty arguments ``` sql :) select concat();. [#65887](https://github.com/ClickHouse/ClickHouse/pull/65887) ([李扬](https://github.com/taiyang-li)).
* Allow controlling named collections in clickhouse-local. [#65973](https://github.com/ClickHouse/ClickHouse/pull/65973) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Improve Azure profile events. [#65999](https://github.com/ClickHouse/ClickHouse/pull/65999) ([alesapin](https://github.com/alesapin)).
* `Query was cancelled` might have been printed twice in clickhouse-client. This behaviour is fixed. [#66005](https://github.com/ClickHouse/ClickHouse/pull/66005) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Support ORC file read by writer time zone. [#66025](https://github.com/ClickHouse/ClickHouse/pull/66025) ([kevinyhzou](https://github.com/KevinyhZou)).
* Refactor JSONExtract functions, support more types including experimental Dynamic type. [#66046](https://github.com/ClickHouse/ClickHouse/pull/66046) ([Kruglov Pavel](https://github.com/Avogar)).
* DatabaseCatalog drops tables faster by using up to database_catalog_drop_table_concurrency threads. [#66065](https://github.com/ClickHouse/ClickHouse/pull/66065) ([Sema Checherinda](https://github.com/CheSema)).
* This PR changes how deduplication for MV works. Fixed a lot of cases like: - on destination table: data is split for 2 or more blocks and that blocks is considered as duplicate when that block is inserted in parallel. - on MV destination table: the equal blocks are deduplicated, that happens when MV often produces equal data as a result for different input data due to performing aggregation. - on MV destination table: the equal blocks which comes from different MV are deduplicated. [#66144](https://github.com/ClickHouse/ClickHouse/pull/66144) ([Sema Checherinda](https://github.com/CheSema)).
* Support null map subcolumn for Variant and Dynamic subcolumns. [#66178](https://github.com/ClickHouse/ClickHouse/pull/66178) ([Kruglov Pavel](https://github.com/Avogar)).
* Add settings to control connection to the PostgreSQL. * Setting `postgresql_connection_attempt_timeout` specifies the value passed to `connect_timeout` parameter of connection URL. * Setting `postgresql_connection_pool_retries` specifies the number of retries to establish a connection to the PostgreSQL end-point. [#66232](https://github.com/ClickHouse/ClickHouse/pull/66232) ([Dmitry Novik](https://github.com/novikd)).
* Reduce inaccuracy of input_wait_elapsed_us/input_wait_elapsed_us/elapsed_us. [#66239](https://github.com/ClickHouse/ClickHouse/pull/66239) ([Azat Khuzhin](https://github.com/azat)).
* Improve FilesystemCache ProfileEvents. [#66249](https://github.com/ClickHouse/ClickHouse/pull/66249) ([zhukai](https://github.com/nauu)).
* Add settings to ignore ON CLUSTER clause in queries for named collection management with replicated storage. [#66288](https://github.com/ClickHouse/ClickHouse/pull/66288) ([MikhailBurdukov](https://github.com/MikhailBurdukov)).
* Upgraded `pocketfft` dependency to the recent commit https://github.com/mreineck/pocketfft/commit/f4c1aa8aa9ce79ad39e80f2c9c41b92ead90fda3. [#66291](https://github.com/ClickHouse/ClickHouse/pull/66291) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Upgraded `azure-sdk-for-cpp` to the recent commit https://github.com/ClickHouse/azure-sdk-for-cpp/commit/ea3e19a7be08519134c643177d56c7484dfec884. [#66292](https://github.com/ClickHouse/ClickHouse/pull/66292) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Function `generateSnowflakeID` now allows to specify a machine ID as a parameter to prevent collisions in large clusters. [#66374](https://github.com/ClickHouse/ClickHouse/pull/66374) ([ZAWA_ll](https://github.com/Zawa-ll)).
* Disable suspending on Ctrl+Z in interactive mode. This is a common trap and is not expected behavior for almost all users. I imagine only a few extreme power users could appreciate suspending terminal applications to the background, but I don't know any. [#66511](https://github.com/ClickHouse/ClickHouse/pull/66511) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Add option for validating the Primary key type in Dictionaries. Without this option for simple layouts any column type will be implicitly converted to UInt64. ### Documentation entry for user-facing changes. [#66595](https://github.com/ClickHouse/ClickHouse/pull/66595) ([MikhailBurdukov](https://github.com/MikhailBurdukov)).
* Fix SHOW MERGES remaining time. [#66735](https://github.com/ClickHouse/ClickHouse/pull/66735) ([Alexey Milovidov](https://github.com/alexey-milovidov)).

#### Critical Bug Fix (crash, LOGICAL_ERROR, data loss, RBAC)
* Fix unexpeced size of low cardinality column in function calls. [#65298](https://github.com/ClickHouse/ClickHouse/pull/65298) ([Raúl Marín](https://github.com/Algunenano)).
* Check cyclic dependencies on CREATE/REPLACE/RENAME/EXCHANGE queries and throw an exception if there is a cyclic dependency. Previously such cyclic dependencies could lead to a deadlock during server startup. Closes [#65355](https://github.com/ClickHouse/ClickHouse/issues/65355). Also fix some bugs in dependencies creation. [#65405](https://github.com/ClickHouse/ClickHouse/pull/65405) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix crash in maxIntersections. [#65689](https://github.com/ClickHouse/ClickHouse/pull/65689) ([Raúl Marín](https://github.com/Algunenano)).
* Fix the VALID UNTIL clause in the user definition resetting after a restart. Closes [#66405](https://github.com/ClickHouse/ClickHouse/issues/66405). [#66409](https://github.com/ClickHouse/ClickHouse/pull/66409) ([Nikolay Degterinsky](https://github.com/evillique)).

#### Bug Fix (user-visible misbehavior in an official stable release)
* Fixed crash while using MaterializedMySQL with TABLE OVERRIDE that maps MySQL NULL field into ClickHouse not NULL field. [#54649](https://github.com/ClickHouse/ClickHouse/pull/54649) ([Filipp Ozinov](https://github.com/bakwc)).
* Fix logical error when PREWHERE expression read no columns and table has no adaptive index granularity (very old table). Fix [#56640](https://github.com/ClickHouse/ClickHouse/issues/56640). [#59173](https://github.com/ClickHouse/ClickHouse/pull/59173) ([Alexander Gololobov](https://github.com/davenger)).
* Fix bug with cancelation buffer when canceling a query. [#64478](https://github.com/ClickHouse/ClickHouse/pull/64478) ([Sema Checherinda](https://github.com/CheSema)).
* Fix filling parts columns from metadata (when columns.txt does not exists). [#64757](https://github.com/ClickHouse/ClickHouse/pull/64757) ([Azat Khuzhin](https://github.com/azat)).
* Fix AST formatting of 'create table b empty as a'. [#64951](https://github.com/ClickHouse/ClickHouse/pull/64951) ([Michael Kolupaev](https://github.com/al13n321)).
* Fix crash for `ALTER TABLE ... ON CLUSTER ... MODIFY SQL SECURITY`. [#64957](https://github.com/ClickHouse/ClickHouse/pull/64957) ([pufit](https://github.com/pufit)).
* Fix crash on destroying AccessControl: add explicit shutdown. [#64993](https://github.com/ClickHouse/ClickHouse/pull/64993) ([Vitaly Baranov](https://github.com/vitlibar)).
* Eliminate injective function in argument of functions `uniq*` recursively. This used to work correctly but was broken in the new analyzer. [#65140](https://github.com/ClickHouse/ClickHouse/pull/65140) ([Duc Canh Le](https://github.com/canhld94)).
* Fix unexpected projection name when query with CTE. [#65267](https://github.com/ClickHouse/ClickHouse/pull/65267) ([wudidapaopao](https://github.com/wudidapaopao)).
* Require `dictGet` privilege when accessing dictionaries via direct query or the `Dictionary` table engine. [#65359](https://github.com/ClickHouse/ClickHouse/pull/65359) ([Joe Lynch](https://github.com/joelynch)).
* Fix user-specific S3 auth with incremental backups. [#65481](https://github.com/ClickHouse/ClickHouse/pull/65481) ([Antonio Andelic](https://github.com/antonio2368)).
* Disable `non-intersecting-parts` optimization for queries with `FINAL` in case of `read-in-order` optimization was enabled. This could lead to an incorrect query result. As a workaround, disable `do_not_merge_across_partitions_select_final` and `split_parts_ranges_into_intersecting_and_non_intersecting_final` before this fix is merged. [#65505](https://github.com/ClickHouse/ClickHouse/pull/65505) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix getting exception `Index out of bound for blob metadata` in case all files from list batch were filtered out. [#65523](https://github.com/ClickHouse/ClickHouse/pull/65523) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fix NOT_FOUND_COLUMN_IN_BLOCK for deduplicate merge of projection. [#65573](https://github.com/ClickHouse/ClickHouse/pull/65573) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Fixed bug in MergeJoin. Column in sparse serialisation might be treated as a column of its nested type though the required conversion wasn't performed. [#65632](https://github.com/ClickHouse/ClickHouse/pull/65632) ([Nikita Taranov](https://github.com/nickitat)).
* Fixed a bug that compatibility level '23.4' was not properly applied. [#65737](https://github.com/ClickHouse/ClickHouse/pull/65737) ([cw5121](https://github.com/cw5121)).
* Fix odbc table with nullable fields. [#65738](https://github.com/ClickHouse/ClickHouse/pull/65738) ([Rodolphe Dugé de Bernonville](https://github.com/RodolpheDuge)).
* Fix data race in `TCPHandler`, which could happen on fatal error. [#65744](https://github.com/ClickHouse/ClickHouse/pull/65744) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fix invalid exceptions in function `parseDateTime` with `%F` and `%D` placeholders. [#65768](https://github.com/ClickHouse/ClickHouse/pull/65768) ([Antonio Andelic](https://github.com/antonio2368)).
* For queries that read from `PostgreSQL`, cancel the internal `PostgreSQL` query if the ClickHouse query is finished. Otherwise, `ClickHouse` query cannot be canceled until the internal `PostgreSQL` query is finished. [#65771](https://github.com/ClickHouse/ClickHouse/pull/65771) ([Maksim Kita](https://github.com/kitaisreal)).
* Fix a bug in short circuit logic when old analyzer and dictGetOrDefault is used. [#65802](https://github.com/ClickHouse/ClickHouse/pull/65802) ([jsc0218](https://github.com/jsc0218)).
* Fix a bug leads to EmbeddedRocksDB with TTL write corrupted SST files. [#65816](https://github.com/ClickHouse/ClickHouse/pull/65816) ([Duc Canh Le](https://github.com/canhld94)).
* Functions `bitTest`, `bitTestAll`, and `bitTestAny` now return an error if the specified bit index is out-of-bounds (issue [#65517](https://github.com/ClickHouse/ClickHouse/issues/65517)). [#65818](https://github.com/ClickHouse/ClickHouse/pull/65818) ([Pablo Marcos](https://github.com/pamarcos)).
* Setting `join_any_take_last_row` is supported in any query with hash join. [#65820](https://github.com/ClickHouse/ClickHouse/pull/65820) ([vdimir](https://github.com/vdimir)).
* Better handling of join conditions involving `IS NULL` checks (for example `ON (a = b AND (a IS NOT NULL) AND (b IS NOT NULL) ) OR ( (a IS NULL) AND (b IS NULL) )` is rewritten to `ON a <=> b`), fix incorrect optimization when condition other then `IS NULL` are present. [#65835](https://github.com/ClickHouse/ClickHouse/pull/65835) ([vdimir](https://github.com/vdimir)).
* Functions `bitShiftLeft` and `bitShitfRight` return an error for out of bounds shift positions (issue [#65516](https://github.com/ClickHouse/ClickHouse/issues/65516)). [#65838](https://github.com/ClickHouse/ClickHouse/pull/65838) ([Pablo Marcos](https://github.com/pamarcos)).
* Fix growing memory usage in S3Queue. [#65839](https://github.com/ClickHouse/ClickHouse/pull/65839) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fix tie handling in `arrayAUC` to match sklearn. [#65840](https://github.com/ClickHouse/ClickHouse/pull/65840) ([gabrielmcg44](https://github.com/gabrielmcg44)).
* Fix possible issues with MySQL server protocol TLS connections. [#65917](https://github.com/ClickHouse/ClickHouse/pull/65917) ([Azat Khuzhin](https://github.com/azat)).
* Fix possible issues with MySQL client protocol TLS connections. [#65938](https://github.com/ClickHouse/ClickHouse/pull/65938) ([Azat Khuzhin](https://github.com/azat)).
* Fix handling of `SSL_ERROR_WANT_READ`/`SSL_ERROR_WANT_WRITE` with zero timeout. [#65941](https://github.com/ClickHouse/ClickHouse/pull/65941) ([Azat Khuzhin](https://github.com/azat)).
* Add missing settings `input_format_csv_skip_first_lines/input_format_tsv_skip_first_lines/input_format_csv_try_infer_numbers_from_strings/input_format_csv_try_infer_strings_from_quoted_tuples` in schema inference cache because they can change the resulting schema. It prevents from incorrect result of schema inference with these settings changed. [#65980](https://github.com/ClickHouse/ClickHouse/pull/65980) ([Kruglov Pavel](https://github.com/Avogar)).
* Column _size in s3 engine and s3 table function denotes the size of a file inside the archive, not a size of the archive itself. [#65993](https://github.com/ClickHouse/ClickHouse/pull/65993) ([Daniil Ivanik](https://github.com/divanik)).
* Fix resolving dynamic subcolumns in analyzer, avoid reading the whole column on dynamic subcolumn reading. [#66004](https://github.com/ClickHouse/ClickHouse/pull/66004) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix config merging for from_env with replace overrides. [#66034](https://github.com/ClickHouse/ClickHouse/pull/66034) ([Azat Khuzhin](https://github.com/azat)).
* Fix a possible hanging in `GRPCServer` during shutdown. This PR fixes [#65622](https://github.com/ClickHouse/ClickHouse/issues/65622). [#66061](https://github.com/ClickHouse/ClickHouse/pull/66061) ([Vitaly Baranov](https://github.com/vitlibar)).
* Fix reading dynamic subcolumns from altered Memory table. Previously if `max_types` peremeter of a Dynamic type was changed in Memory table via alter, further subcolumns reading can return wrong result. [#66066](https://github.com/ClickHouse/ClickHouse/pull/66066) ([Kruglov Pavel](https://github.com/Avogar)).
* Fixed several cases in function `has` with non-constant `LowCardinality` arguments. [#66088](https://github.com/ClickHouse/ClickHouse/pull/66088) ([Anton Popov](https://github.com/CurtizJ)).
* Fix for `groupArrayIntersect`. It had incorrect behavior in the `merge()` function. Also, fixed behavior in `deserialise()` for numeric and general data. [#66103](https://github.com/ClickHouse/ClickHouse/pull/66103) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).
* Fixed buffer overflow bug in `unbin`/`unhex` implementation. [#66106](https://github.com/ClickHouse/ClickHouse/pull/66106) ([Nikita Taranov](https://github.com/nickitat)).
* Disable the `merge-filters` optimization introduced in [#64760](https://github.com/ClickHouse/ClickHouse/issues/64760). It may cause an exception if optimization merges two filter expressions and does not apply a short-circuit evaluation. [#66126](https://github.com/ClickHouse/ClickHouse/pull/66126) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fixed the issue when the server failed to parse Avro files with negative block size arrays encoded, which is now allowed by the Avro specification. [#66130](https://github.com/ClickHouse/ClickHouse/pull/66130) ([Serge Klochkov](https://github.com/slvrtrn)).
* Fixed a bug in ZooKeeper client: a session could get stuck in unusable state after receiving a hardware error from ZooKeeper. For example, this might happen due to "soft memory limit" in ClickHouse Keeper. [#66140](https://github.com/ClickHouse/ClickHouse/pull/66140) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Fix issue in SumIfToCountIfVisitor and signed integers. [#66146](https://github.com/ClickHouse/ClickHouse/pull/66146) ([Raúl Marín](https://github.com/Algunenano)).
* Fix rare case with missing data in the result of distributed query, close [#61432](https://github.com/ClickHouse/ClickHouse/issues/61432). [#66174](https://github.com/ClickHouse/ClickHouse/pull/66174) ([vdimir](https://github.com/vdimir)).
* Fix order of parsing metadata fields in StorageDeltaLake. [#66211](https://github.com/ClickHouse/ClickHouse/pull/66211) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Don't throw `TIMEOUT_EXCEEDED` for `none_only_active` mode of `distributed_ddl_output_mode`. [#66218](https://github.com/ClickHouse/ClickHouse/pull/66218) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Fix handling limit for `system.numbers_mt` when no index can be used. [#66231](https://github.com/ClickHouse/ClickHouse/pull/66231) ([János Benjamin Antal](https://github.com/antaljanosbenjamin)).
* Fixed how the ClickHouse server detects the maximum number of usable CPU cores as specified by cgroups v2 if the server runs in a container such as Docker. In more detail, containers often run their process in the root cgroup which has an empty name. In that case, ClickHouse ignored the CPU limits set by cgroups v2. [#66237](https://github.com/ClickHouse/ClickHouse/pull/66237) ([filimonov](https://github.com/filimonov)).
* Fix the `Not-ready set` error when a subquery with `IN` is used in the constraint. [#66261](https://github.com/ClickHouse/ClickHouse/pull/66261) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix indexHint function case found by fuzzer. [#66286](https://github.com/ClickHouse/ClickHouse/pull/66286) ([Anton Popov](https://github.com/CurtizJ)).
* Fix error reporting while copying to S3 or AzureBlobStorage. [#66295](https://github.com/ClickHouse/ClickHouse/pull/66295) ([Vitaly Baranov](https://github.com/vitlibar)).
* Prevent watchdog from keeping descriptors of unlinked(rotated) log files. [#66334](https://github.com/ClickHouse/ClickHouse/pull/66334) ([Aleksei Filatov](https://github.com/aalexfvk)).
* Fix the bug that logicalexpressionoptimizerpass lost logical type of constant. closes [#64487](https://github.com/ClickHouse/ClickHouse/issues/64487). [#66344](https://github.com/ClickHouse/ClickHouse/pull/66344) ([pn](https://github.com/chloro-pn)).
* Fix `Column identifier is already registered` error with `group_by_use_nulls=true` and new analyzer. [#66400](https://github.com/ClickHouse/ClickHouse/pull/66400) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix possible incorrect result for queries joining and filtering table external engine (like PostgreSQL), due to too aggressive filter pushdown. Since now, conditions from where section won't be send to external database in case of outer join with external table. [#66402](https://github.com/ClickHouse/ClickHouse/pull/66402) ([vdimir](https://github.com/vdimir)).
* Added missing column materialization for cross join. [#66413](https://github.com/ClickHouse/ClickHouse/pull/66413) ([lgbo](https://github.com/lgbo-ustc)).
* Fix `Cannot find column` error for queries with constant expression in `GROUP BY` key and new analyzer enabled. [#66433](https://github.com/ClickHouse/ClickHouse/pull/66433) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Avoid possible logical error during import from Npy format in case of bad array nesting level, fix testing of other kinds of errors. [#66461](https://github.com/ClickHouse/ClickHouse/pull/66461) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).
* Fix wrong count() result when there is non-deterministic function in predicate. [#66510](https://github.com/ClickHouse/ClickHouse/pull/66510) ([Duc Canh Le](https://github.com/canhld94)).
* Correctly track memory for `Allocator::realloc`. [#66548](https://github.com/ClickHouse/ClickHouse/pull/66548) ([Antonio Andelic](https://github.com/antonio2368)).
* Fix reading of uninitialized memory when hashing empty tuples. This closes [#66559](https://github.com/ClickHouse/ClickHouse/issues/66559). [#66562](https://github.com/ClickHouse/ClickHouse/pull/66562) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix an invalid result for queries with `WINDOW`. This could happen when `PARTITION` columns have sparse serialization and window functions are executed in parallel. [#66579](https://github.com/ClickHouse/ClickHouse/pull/66579) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix removing named collections in local storage. [#66599](https://github.com/ClickHouse/ClickHouse/pull/66599) ([János Benjamin Antal](https://github.com/antaljanosbenjamin)).
* Fix logical error in `PrometheusRequestHandler`. [#66621](https://github.com/ClickHouse/ClickHouse/pull/66621) ([Vitaly Baranov](https://github.com/vitlibar)).
* `column_length` is not updated in `ColumnTuple::insertManyFrom`. [#66626](https://github.com/ClickHouse/ClickHouse/pull/66626) ([lgbo](https://github.com/lgbo-ustc)).
* Fix `Unknown identifier` and `Column is not under aggregate function` errors for queries with the expression `(column IS NULL).` The bug was triggered by [#65088](https://github.com/ClickHouse/ClickHouse/issues/65088), with the disabled analyzer only. [#66654](https://github.com/ClickHouse/ClickHouse/pull/66654) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix `Method getResultType is not supported for QUERY query node` error when scalar subquery was used as the first argument of IN (with new analyzer). [#66655](https://github.com/ClickHouse/ClickHouse/pull/66655) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix possible PARAMETER_OUT_OF_BOUND error during reading variant subcolumn. [#66659](https://github.com/ClickHouse/ClickHouse/pull/66659) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix rare case of stuck merge after drop column. [#66707](https://github.com/ClickHouse/ClickHouse/pull/66707) ([Raúl Marín](https://github.com/Algunenano)).
* Fix assertion `isUniqTypes` when insert select from remote sources. [#66722](https://github.com/ClickHouse/ClickHouse/pull/66722) ([Sema Checherinda](https://github.com/CheSema)).
* Backported in [#67026](https://github.com/ClickHouse/ClickHouse/issues/67026): In rare cases ClickHouse could consider parts as broken because of some unexpected projections on disk. Now it's fixed. [#66898](https://github.com/ClickHouse/ClickHouse/pull/66898) ([alesapin](https://github.com/alesapin)).
* Backported in [#67443](https://github.com/ClickHouse/ClickHouse/issues/67443): Forbid create as select even when database_replicated_allow_heavy_create is set. It was unconditionally forbidden in 23.12 and accidentally allowed under the setting in unreleased 24.7. [#66980](https://github.com/ClickHouse/ClickHouse/pull/66980) ([vdimir](https://github.com/vdimir)).
* Backported in [#67201](https://github.com/ClickHouse/ClickHouse/issues/67201): TRUNCATE DATABASE used to stop replication as if it was a DROP DATABASE query, it's fixed. [#67129](https://github.com/ClickHouse/ClickHouse/pull/67129) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Backported in [#67383](https://github.com/ClickHouse/ClickHouse/issues/67383): Fix error `Cannot convert column because it is non constant in source stream but must be constant in result.` for a query that reads from the `Merge` table over the `Distriburted` table with one shard. [#67146](https://github.com/ClickHouse/ClickHouse/pull/67146) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Backported in [#67246](https://github.com/ClickHouse/ClickHouse/issues/67246): This closes [#67156](https://github.com/ClickHouse/ClickHouse/issues/67156). This closes [#66447](https://github.com/ClickHouse/ClickHouse/issues/66447). The bug was introduced in https://github.com/ClickHouse/ClickHouse/pull/62907. [#67178](https://github.com/ClickHouse/ClickHouse/pull/67178) ([Maksim Kita](https://github.com/kitaisreal)).

#### Build/Testing/Packaging Improvement
* Instantiate template methods ahead in different .cpp files, avoid too large translation units during compiling. [#64818](https://github.com/ClickHouse/ClickHouse/pull/64818) ([lgbo](https://github.com/lgbo-ustc)).
* Stateless tests: Improve tests speed and decrease number of parallel jobs. [#65186](https://github.com/ClickHouse/ClickHouse/pull/65186) ([Nikita Fomichev](https://github.com/fm4v)).
* Add tests for `base64URLEncode` and `base64URLDecode`. Add analyzer tests. [#65979](https://github.com/ClickHouse/ClickHouse/pull/65979) ([Nikita Fomichev](https://github.com/fm4v)).
* Fix problem when github terminate instances by timeout and artifacts are not collected and full test report is not generated. [#66036](https://github.com/ClickHouse/ClickHouse/pull/66036) ([Nikita Fomichev](https://github.com/fm4v)).
* Fix test [test_grpc_protocol/test.py::test_progress](https://s3.amazonaws.com/clickhouse-test-reports/57695/188f8a3df74caf830ad1ced3c4cf6dfb0aa90093/integration_tests__asan__old_analyzer__[4_6].html). [#66063](https://github.com/ClickHouse/ClickHouse/pull/66063) ([Vitaly Baranov](https://github.com/vitlibar)).
* Stateless tests: Improve tests speed and decrease number of parallel jobs. [#66305](https://github.com/ClickHouse/ClickHouse/pull/66305) ([Nikita Fomichev](https://github.com/fm4v)).
* Stateless tests: Improve tests speed and decrease number of parallel jobs 3. [#66363](https://github.com/ClickHouse/ClickHouse/pull/66363) ([Nikita Fomichev](https://github.com/fm4v)).
* Tests: fix tests hang up in cases when gdb catches error. [#66411](https://github.com/ClickHouse/ClickHouse/pull/66411) ([Nikita Fomichev](https://github.com/fm4v)).
* ... since [Release v24.6.1.4423-stable](https://github.com/ClickHouse/ClickHouse/releases/tag/v24.6.1.4423-stable) when build in ppc64le with dynamic openssl build (`cmake -DENABLE_OPENSSL_DYNAMIC=1 -DCMAKE_TOOLCHAIN_FILE= cmake/linux/toolchain-ppc64le.cmake `) got error: ` ld.lld: error: duplicate symbol: OPENSSL_cleanse`. [#66733](https://github.com/ClickHouse/ClickHouse/pull/66733) ([Yong Wang](https://github.com/kashwy)).

#### NO CL CATEGORY

* Backported in [#67084](https://github.com/ClickHouse/ClickHouse/issues/67084):. [#67040](https://github.com/ClickHouse/ClickHouse/pull/67040) ([Raúl Marín](https://github.com/Algunenano)).
* Backported in [#67452](https://github.com/ClickHouse/ClickHouse/issues/67452):. [#67392](https://github.com/ClickHouse/ClickHouse/pull/67392) ([alesapin](https://github.com/alesapin)).

#### NO CL ENTRY

* NO CL ENTRY:  'Revert "Revert "Small fix for 02340_parts_refcnt_mergetree""'. [#65155](https://github.com/ClickHouse/ClickHouse/pull/65155) ([Nikita Taranov](https://github.com/nickitat)).
* NO CL ENTRY:  'Revert "Use 1MB HTTP buffers to avoid frequent send syscalls"'. [#65498](https://github.com/ClickHouse/ClickHouse/pull/65498) ([Sergei Trifonov](https://github.com/serxa)).
* NO CL ENTRY:  'Revert "Resubmit http_external_tables_memory_tracking test"'. [#65500](https://github.com/ClickHouse/ClickHouse/pull/65500) ([Nikita Taranov](https://github.com/nickitat)).
* NO CL ENTRY:  'Revert "Add an assertion in ReplicatedMergeTreeQueue"'. [#65686](https://github.com/ClickHouse/ClickHouse/pull/65686) ([Raúl Marín](https://github.com/Algunenano)).
* NO CL ENTRY:  'Revert "insertion deduplication on retries for materialised views"'. [#66134](https://github.com/ClickHouse/ClickHouse/pull/66134) ([Sema Checherinda](https://github.com/CheSema)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Return and fix 01600_parts_states_metrics_long test. [#58748](https://github.com/ClickHouse/ClickHouse/pull/58748) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Add azure_cache as storage policy to tests. [#59943](https://github.com/ClickHouse/ClickHouse/pull/59943) ([SmitaRKulkarni](https://github.com/SmitaRKulkarni)).
* Minor: replaced expression with LEGACY_MAX_LEVEL. [#61268](https://github.com/ClickHouse/ClickHouse/pull/61268) ([Vasily Nemkov](https://github.com/Enmk)).
* Make write to temporary data in cache do all checks and assertions as during write to ordinary cache. [#63348](https://github.com/ClickHouse/ClickHouse/pull/63348) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Refactoring near azure blob storage. [#63636](https://github.com/ClickHouse/ClickHouse/pull/63636) ([Anton Popov](https://github.com/CurtizJ)).
* Everything should work with Analyzer. [#63643](https://github.com/ClickHouse/ClickHouse/pull/63643) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* add some log for non using detached tables. [#64992](https://github.com/ClickHouse/ClickHouse/pull/64992) ([Konstantin Morozov](https://github.com/k-morozov)).
* Remove dag flags. [#65234](https://github.com/ClickHouse/ClickHouse/pull/65234) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Fix flaky autocompletion test. [#65246](https://github.com/ClickHouse/ClickHouse/pull/65246) ([Konstantin Bogdanov](https://github.com/thevar1able)).
* Disable userspace page cache by default. [#65305](https://github.com/ClickHouse/ClickHouse/pull/65305) ([Michael Kolupaev](https://github.com/al13n321)).
* Update version_date.tsv and changelogs after v24.4.3.25-stable. [#65308](https://github.com/ClickHouse/ClickHouse/pull/65308) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Do not raise a NOT_IMPLEMENTED error when getting s3 metrics with a multiple disk configuration. [#65403](https://github.com/ClickHouse/ClickHouse/pull/65403) ([Elena Torró](https://github.com/elenatorro)).
* Dodging reading from wrong table with parallel replicas. [#65417](https://github.com/ClickHouse/ClickHouse/pull/65417) ([Nikita Taranov](https://github.com/nickitat)).
* Fix: return error if can't connect to any replicas chosen for query execution. [#65467](https://github.com/ClickHouse/ClickHouse/pull/65467) ([Igor Nikonov](https://github.com/devcrafter)).
* Fix `AzureObjectStorage::exists` method. [#65471](https://github.com/ClickHouse/ClickHouse/pull/65471) ([Anton Popov](https://github.com/CurtizJ)).
* Update version after release. [#65483](https://github.com/ClickHouse/ClickHouse/pull/65483) ([Raúl Marín](https://github.com/Algunenano)).
* Generate 24.6 changelog. [#65485](https://github.com/ClickHouse/ClickHouse/pull/65485) ([Raúl Marín](https://github.com/Algunenano)).
* Fix of `PlanSquashingTransform`: pipeline stuck. [#65487](https://github.com/ClickHouse/ClickHouse/pull/65487) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).
* Fix bad test `02922_deduplication_with_zero_copy`. [#65492](https://github.com/ClickHouse/ClickHouse/pull/65492) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Disable clang-format in special areas. [#65495](https://github.com/ClickHouse/ClickHouse/pull/65495) ([Nikita Taranov](https://github.com/nickitat)).
* Fix `test_keeper_snapshots`. [#65497](https://github.com/ClickHouse/ClickHouse/pull/65497) ([Antonio Andelic](https://github.com/antonio2368)).
* Update to libunwind 8.1.7. [#65509](https://github.com/ClickHouse/ClickHouse/pull/65509) ([Michael Kolupaev](https://github.com/al13n321)).
* Setting `uniform_snowflake_conversion_functions` (not in any release yet) was replaced by setting `allow_deprecated_snowflake_conversion_functions`. The latter controls if the legacy snowflake conversion functions are available (by default, they are not). [#65522](https://github.com/ClickHouse/ClickHouse/pull/65522) ([Robert Schulze](https://github.com/rschu1ze)).
* Try CI without RerunCheck, jobs can be easily rerun manually though extra amount of work in CI will follow on workflow restart. [#65524](https://github.com/ClickHouse/ClickHouse/pull/65524) ([Max K.](https://github.com/maxknv)).
* Bump re2 to latest HEAD. [#65526](https://github.com/ClickHouse/ClickHouse/pull/65526) ([Robert Schulze](https://github.com/rschu1ze)).
* OpenSSL: Replace temporary fix for unsynchronized access by official fix. [#65529](https://github.com/ClickHouse/ClickHouse/pull/65529) ([Robert Schulze](https://github.com/rschu1ze)).
* Update README.md. [#65531](https://github.com/ClickHouse/ClickHouse/pull/65531) ([Tyler Hannan](https://github.com/tylerhannan)).
* CI: some time there are timeouts on DROP TABLES for random tests. [#65535](https://github.com/ClickHouse/ClickHouse/pull/65535) ([Sema Checherinda](https://github.com/CheSema)).
* Synchronize `MARK_CACHE_SIZE` value in default settings and config. [#65547](https://github.com/ClickHouse/ClickHouse/pull/65547) ([Denny Crane](https://github.com/den-crane)).
* CI: Skip removed test files in stateless flaky check job. [#65553](https://github.com/ClickHouse/ClickHouse/pull/65553) ([Max K.](https://github.com/maxknv)).
* Renames Build report jobs. [#65554](https://github.com/ClickHouse/ClickHouse/pull/65554) ([Max K.](https://github.com/maxknv)).
* Parse user from URL for dashboard.html (useful for sharing). [#65556](https://github.com/ClickHouse/ClickHouse/pull/65556) ([Azat Khuzhin](https://github.com/azat)).
* Remove tech debt. [#65561](https://github.com/ClickHouse/ClickHouse/pull/65561) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Maybe fix test `00763_lock_buffer_long.sh`. [#65562](https://github.com/ClickHouse/ClickHouse/pull/65562) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix clickhouse-keeper with not system-wide directories and provide override for local development. [#65563](https://github.com/ClickHouse/ClickHouse/pull/65563) ([Azat Khuzhin](https://github.com/azat)).
* Re-configure yamllint to allow document-start. [#65565](https://github.com/ClickHouse/ClickHouse/pull/65565) ([Azat Khuzhin](https://github.com/azat)).
* Fix flaky test `01254_dict_load_after_detach_attach.sql`. [#65571](https://github.com/ClickHouse/ClickHouse/pull/65571) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Improve flaky test to provide more diagnostics. [#65586](https://github.com/ClickHouse/ClickHouse/pull/65586) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix test_parallel_replicas_distributed_skip_shards flakiness. [#65588](https://github.com/ClickHouse/ClickHouse/pull/65588) ([Igor Nikonov](https://github.com/devcrafter)).
* Fix an error in the test about metadata_type. [#65592](https://github.com/ClickHouse/ClickHouse/pull/65592) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix slow test. [#65593](https://github.com/ClickHouse/ClickHouse/pull/65593) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* fix flaky 02864_statistics_uniq. [#65599](https://github.com/ClickHouse/ClickHouse/pull/65599) ([Han Fei](https://github.com/hanfei1991)).
* Fix 03172_error_log_table_not_empty. [#65604](https://github.com/ClickHouse/ClickHouse/pull/65604) ([Pablo Marcos](https://github.com/pamarcos)).
* Enable realtime digest for Jepsen tests. [#65608](https://github.com/ClickHouse/ClickHouse/pull/65608) ([Antonio Andelic](https://github.com/antonio2368)).
* CI: Return Job Rerun check. [#65613](https://github.com/ClickHouse/ClickHouse/pull/65613) ([Max K.](https://github.com/maxknv)).
* Update CHANGELOG.md. [#65624](https://github.com/ClickHouse/ClickHouse/pull/65624) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Module is required for authenticating in GH (in cloud). [#65628](https://github.com/ClickHouse/ClickHouse/pull/65628) ([Max K.](https://github.com/maxknv)).
* Update IObjectStorage.h. [#65631](https://github.com/ClickHouse/ClickHouse/pull/65631) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fix 02834_apache_arrow_abort flakiness with MSAN. [#65640](https://github.com/ClickHouse/ClickHouse/pull/65640) ([Michael Kolupaev](https://github.com/al13n321)).
* Fix overflow in StorageWindowView. [#65641](https://github.com/ClickHouse/ClickHouse/pull/65641) ([Michael Kolupaev](https://github.com/al13n321)).
* Fix inconsistent AST formatting when a keyword is used as type name. [#65648](https://github.com/ClickHouse/ClickHouse/pull/65648) ([Michael Kolupaev](https://github.com/al13n321)).
* CI: Single point of setting mergeable check status. [#65658](https://github.com/ClickHouse/ClickHouse/pull/65658) ([Max K.](https://github.com/maxknv)).
* Miscellaneous and insignificant changes around Client/ClientBase. [#65669](https://github.com/ClickHouse/ClickHouse/pull/65669) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Add Replicated database names to ZooKeeper for introspection. [#65675](https://github.com/ClickHouse/ClickHouse/pull/65675) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Introduce type contract checks in `IColumn`. [#65687](https://github.com/ClickHouse/ClickHouse/pull/65687) ([Nikita Taranov](https://github.com/nickitat)).
* Print slightly more information in 02982_aggregation_states_destruction. [#65688](https://github.com/ClickHouse/ClickHouse/pull/65688) ([Michael Kolupaev](https://github.com/al13n321)).
* Disable stacktrace collection in GWPAsan by default. [#65701](https://github.com/ClickHouse/ClickHouse/pull/65701) ([Antonio Andelic](https://github.com/antonio2368)).
* Build jemalloc with profiler. [#65702](https://github.com/ClickHouse/ClickHouse/pull/65702) ([Antonio Andelic](https://github.com/antonio2368)).
* Fix duplicate symbol linkage error. [#65705](https://github.com/ClickHouse/ClickHouse/pull/65705) ([Nikita Taranov](https://github.com/nickitat)).
* Fix server restarts in performance tests. [#65717](https://github.com/ClickHouse/ClickHouse/pull/65717) ([Antonio Andelic](https://github.com/antonio2368)).
* Update 03002_part_log_rmt_fetch_mutate_error.sql. [#65720](https://github.com/ClickHouse/ClickHouse/pull/65720) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Fix bug with session closing in Keeper. [#65735](https://github.com/ClickHouse/ClickHouse/pull/65735) ([Antonio Andelic](https://github.com/antonio2368)).
* Fix flaky `02265_column_ttl`. Closes [#65719](https://github.com/ClickHouse/ClickHouse/issues/65719). [#65742](https://github.com/ClickHouse/ClickHouse/pull/65742) ([János Benjamin Antal](https://github.com/antaljanosbenjamin)).
* See [#65745](https://github.com/ClickHouse/ClickHouse/issues/65745). It doesn't solve the issue, but helps a bit. [#65746](https://github.com/ClickHouse/ClickHouse/pull/65746) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Update CHANGELOG.md. [#65752](https://github.com/ClickHouse/ClickHouse/pull/65752) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* document declarative ssh-keys authentication. [#65756](https://github.com/ClickHouse/ClickHouse/pull/65756) ([Tobias Florek](https://github.com/ibotty)).
* `base64En/Decode64Url` --> `base64En/Decode64URL`. [#65760](https://github.com/ClickHouse/ClickHouse/pull/65760) ([Robert Schulze](https://github.com/rschu1ze)).
* Fix for issue [#65757](https://github.com/ClickHouse/ClickHouse/issues/65757). [#65763](https://github.com/ClickHouse/ClickHouse/pull/65763) ([Yarik Briukhovetskyi](https://github.com/yariks5s)).
* Fix flaky `test_replicated_database::test_alter_attach`. [#65766](https://github.com/ClickHouse/ClickHouse/pull/65766) ([Antonio Andelic](https://github.com/antonio2368)).
* Fix: progress bar for read in order queries. [#65769](https://github.com/ClickHouse/ClickHouse/pull/65769) ([Igor Nikonov](https://github.com/devcrafter)).
* CI: Fix for Builds report job in backports and releases. [#65774](https://github.com/ClickHouse/ClickHouse/pull/65774) ([Max K.](https://github.com/maxknv)).
* CI: New create release workflow. [#65775](https://github.com/ClickHouse/ClickHouse/pull/65775) ([Max K.](https://github.com/maxknv)).
* fixed misspelled word. [#65778](https://github.com/ClickHouse/ClickHouse/pull/65778) ([Linh Giang](https://github.com/linhgiang24)).
* Refactor statistics interface. [#65792](https://github.com/ClickHouse/ClickHouse/pull/65792) ([Robert Schulze](https://github.com/rschu1ze)).
* Try to make `test_ldap_external_user_directory` less flaky. [#65794](https://github.com/ClickHouse/ClickHouse/pull/65794) ([Andrey Zvonov](https://github.com/zvonand)).
* AMI image with gh and jwt. [#65795](https://github.com/ClickHouse/ClickHouse/pull/65795) ([Max K.](https://github.com/maxknv)).
* Forbid join algorithm randomisation for 03094_one_thousand_joins. [#65798](https://github.com/ClickHouse/ClickHouse/pull/65798) ([Nikita Taranov](https://github.com/nickitat)).
* Fix 02931_rewrite_sum_column_and_constant flakiness. [#65800](https://github.com/ClickHouse/ClickHouse/pull/65800) ([Michael Kolupaev](https://github.com/al13n321)).
* Update StorageMaterializedView.cpp. [#65801](https://github.com/ClickHouse/ClickHouse/pull/65801) ([Nikolay Degterinsky](https://github.com/evillique)).
* Fix slow `getFQDNOrHostNameImpl` on macOS. [#65803](https://github.com/ClickHouse/ClickHouse/pull/65803) ([Konstantin Bogdanov](https://github.com/thevar1able)).
* No jemalloc profiler for non-Linux. [#65834](https://github.com/ClickHouse/ClickHouse/pull/65834) ([Antonio Andelic](https://github.com/antonio2368)).
* Add missing workload identity changes. [#65848](https://github.com/ClickHouse/ClickHouse/pull/65848) ([SmitaRKulkarni](https://github.com/SmitaRKulkarni)).
* Fix rocksdb. [#65858](https://github.com/ClickHouse/ClickHouse/pull/65858) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Update the list of easy tasks. [#65865](https://github.com/ClickHouse/ClickHouse/pull/65865) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Update CHANGELOG.md. [#65866](https://github.com/ClickHouse/ClickHouse/pull/65866) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* This closes [#43003](https://github.com/ClickHouse/ClickHouse/issues/43003). [#65870](https://github.com/ClickHouse/ClickHouse/pull/65870) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Uninteresting changes. [#65871](https://github.com/ClickHouse/ClickHouse/pull/65871) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Max sessions for user tests improvements. [#65888](https://github.com/ClickHouse/ClickHouse/pull/65888) ([Alexey Gerasimchuck](https://github.com/Demilivor)).
* Update version_date.tsv and changelogs after v24.6.1.4423-stable. [#65909](https://github.com/ClickHouse/ClickHouse/pull/65909) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Remove standalone Keeper build. [#65910](https://github.com/ClickHouse/ClickHouse/pull/65910) ([Antonio Andelic](https://github.com/antonio2368)).
* Add extra profiling helpers for Keeper. [#65918](https://github.com/ClickHouse/ClickHouse/pull/65918) ([Antonio Andelic](https://github.com/antonio2368)).
* PostgreSQL source cancel query comments. [#65919](https://github.com/ClickHouse/ClickHouse/pull/65919) ([Maksim Kita](https://github.com/kitaisreal)).
* Remove mysqlxx::Pool::Entry assignment operator. [#65920](https://github.com/ClickHouse/ClickHouse/pull/65920) ([Azat Khuzhin](https://github.com/azat)).
* No random settings for a test with `Object(JSON)`. [#65921](https://github.com/ClickHouse/ClickHouse/pull/65921) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Follow up to [#65046](https://github.com/ClickHouse/ClickHouse/issues/65046). [#65928](https://github.com/ClickHouse/ClickHouse/pull/65928) ([Kseniia Sumarokova](https://github.com/kssenii)).
* add restriction for storage join. [#65936](https://github.com/ClickHouse/ClickHouse/pull/65936) ([Han Fei](https://github.com/hanfei1991)).
* Update version_date.tsv and changelogs after v24.5.4.49-stable. [#65937](https://github.com/ClickHouse/ClickHouse/pull/65937) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Add table name to MergeTreeSource spans. [#65940](https://github.com/ClickHouse/ClickHouse/pull/65940) ([Nikita Taranov](https://github.com/nickitat)).
* Fix SettingsChangesHistory 24.7. [#65945](https://github.com/ClickHouse/ClickHouse/pull/65945) ([Raúl Marín](https://github.com/Algunenano)).
* Fix logical error "Expected ReadBufferFromFile, but got DB::EmptyReadBuffer". [#65949](https://github.com/ClickHouse/ClickHouse/pull/65949) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Use -Og instead of -O0 for debug builds. [#65953](https://github.com/ClickHouse/ClickHouse/pull/65953) ([Michael Kolupaev](https://github.com/al13n321)).
* Fix data race for Keeper snapshot queue. [#65970](https://github.com/ClickHouse/ClickHouse/pull/65970) ([Antonio Andelic](https://github.com/antonio2368)).
* Minor changes in CHANGELOG. [#65971](https://github.com/ClickHouse/ClickHouse/pull/65971) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Remove unnatural punctuation from Parquet. [#65972](https://github.com/ClickHouse/ClickHouse/pull/65972) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Try fix "Check timeout expired" without any server logs in report in stateless tests. [#65977](https://github.com/ClickHouse/ClickHouse/pull/65977) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fix support of non-const scale arguments in rounding functions. [#65983](https://github.com/ClickHouse/ClickHouse/pull/65983) ([Mikhail Gorshkov](https://github.com/mgorshkov)).
* More aesthetic error messages. [#65985](https://github.com/ClickHouse/ClickHouse/pull/65985) ([Robert Schulze](https://github.com/rschu1ze)).
* Fix race in s3queue. [#65986](https://github.com/ClickHouse/ClickHouse/pull/65986) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Now it's possible to specify `s3-storage`, `azure-object-storage` and in general `object-storage`. [#65988](https://github.com/ClickHouse/ClickHouse/pull/65988) ([alesapin](https://github.com/alesapin)).
* Fix flaky test_storage_s3_queue tests. [#66009](https://github.com/ClickHouse/ClickHouse/pull/66009) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Relax the check in 02982_aggregation_states_destruction. [#66011](https://github.com/ClickHouse/ClickHouse/pull/66011) ([Nikita Taranov](https://github.com/nickitat)).
* Fix `01158_zookeeper_log_long`. [#66012](https://github.com/ClickHouse/ClickHouse/pull/66012) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Remove scary jemalloc log. [#66028](https://github.com/ClickHouse/ClickHouse/pull/66028) ([Antonio Andelic](https://github.com/antonio2368)).
* Move experimental settings to the experimental block. [#66030](https://github.com/ClickHouse/ClickHouse/pull/66030) ([Raúl Marín](https://github.com/Algunenano)).
* Fix lock-order-inversion in DatabaseCatalog. [#66038](https://github.com/ClickHouse/ClickHouse/pull/66038) ([Nikolay Degterinsky](https://github.com/evillique)).
* Try disabling jemalloc background threads. [#66041](https://github.com/ClickHouse/ClickHouse/pull/66041) ([Antonio Andelic](https://github.com/antonio2368)).
* Try to avoid conflicts in `SettingsChangesHistory.cpp`. [#66042](https://github.com/ClickHouse/ClickHouse/pull/66042) ([Anton Popov](https://github.com/CurtizJ)).
* Add profile events for regex cache. [#66050](https://github.com/ClickHouse/ClickHouse/pull/66050) ([Antonio Andelic](https://github.com/antonio2368)).
* Bump vectorscan to 5.4.10.1. [#66056](https://github.com/ClickHouse/ClickHouse/pull/66056) ([Robert Schulze](https://github.com/rschu1ze)).
* Remove obsolete comment. [#66059](https://github.com/ClickHouse/ClickHouse/pull/66059) ([Robert Schulze](https://github.com/rschu1ze)).
* Maybe fix tsan assert in `test_mysql_killed_while_insert_8_0`. [#66064](https://github.com/ClickHouse/ClickHouse/pull/66064) ([Robert Schulze](https://github.com/rschu1ze)).
* Move some of `HTTPHandler` stuff to separate source files in order to reuse it in `PrometheusRequestHandler`. This PR is required for https://github.com/ClickHouse/ClickHouse/pull/64183. [#66067](https://github.com/ClickHouse/ClickHouse/pull/66067) ([Vitaly Baranov](https://github.com/vitlibar)).
* Bump rocksdb to v6.23.3. [#66068](https://github.com/ClickHouse/ClickHouse/pull/66068) ([Robert Schulze](https://github.com/rschu1ze)).
* Add protobufs for `Prometheus` `remote-write` / `remote-read` protocols to our repository. Fix cmake script for compiling protobufs. [#66069](https://github.com/ClickHouse/ClickHouse/pull/66069) ([Vitaly Baranov](https://github.com/vitlibar)).
* Use pinned versions of all python packages in CI docker images. Also makes clang-18.1.8 work with sanitizers and surprisingly fixes [#66049](https://github.com/ClickHouse/ClickHouse/issues/66049). [#66070](https://github.com/ClickHouse/ClickHouse/pull/66070) ([alesapin](https://github.com/alesapin)).
* Clean-up custom LLVM 15 patches. [#66072](https://github.com/ClickHouse/ClickHouse/pull/66072) ([Robert Schulze](https://github.com/rschu1ze)).
* Minor JWT client fixes. [#66073](https://github.com/ClickHouse/ClickHouse/pull/66073) ([Konstantin Bogdanov](https://github.com/thevar1able)).
* Bump vectorscan to 5.4.11. [#66082](https://github.com/ClickHouse/ClickHouse/pull/66082) ([Robert Schulze](https://github.com/rschu1ze)).
* Print stacktrace in case of abort after logical error. [#66091](https://github.com/ClickHouse/ClickHouse/pull/66091) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* fix flaky 03172_error_log_table_not_empty. [#66093](https://github.com/ClickHouse/ClickHouse/pull/66093) ([Sema Checherinda](https://github.com/CheSema)).
* Bump s2geometry to latest master. [#66094](https://github.com/ClickHouse/ClickHouse/pull/66094) ([Robert Schulze](https://github.com/rschu1ze)).
* update keeper bench example config file. [#66095](https://github.com/ClickHouse/ClickHouse/pull/66095) ([Han Fei](https://github.com/hanfei1991)).
* Avoid using source directory for generated files. [#66097](https://github.com/ClickHouse/ClickHouse/pull/66097) ([Azat Khuzhin](https://github.com/azat)).
* More precise warning message about sanitizers. [#66098](https://github.com/ClickHouse/ClickHouse/pull/66098) ([Anton Popov](https://github.com/CurtizJ)).
* Slightly better calculation of primary index. [#66099](https://github.com/ClickHouse/ClickHouse/pull/66099) ([Anton Popov](https://github.com/CurtizJ)).
* Bump Azure to 1.12. [#66100](https://github.com/ClickHouse/ClickHouse/pull/66100) ([Robert Schulze](https://github.com/rschu1ze)).
* Add a test for [#58998](https://github.com/ClickHouse/ClickHouse/issues/58998). [#66101](https://github.com/ClickHouse/ClickHouse/pull/66101) ([Anton Popov](https://github.com/CurtizJ)).
* CI: Fix sync pr merge. [#66105](https://github.com/ClickHouse/ClickHouse/pull/66105) ([Max K.](https://github.com/maxknv)).
* Remove flaky case from 02956_rocksdb_bulk_sink. [#66107](https://github.com/ClickHouse/ClickHouse/pull/66107) ([vdimir](https://github.com/vdimir)).
* Fix bugfix checker. [#66120](https://github.com/ClickHouse/ClickHouse/pull/66120) ([Raúl Marín](https://github.com/Algunenano)).
* Correctly print long processing requests in Keeper. [#66124](https://github.com/ClickHouse/ClickHouse/pull/66124) ([Antonio Andelic](https://github.com/antonio2368)).
* Update version_date.tsv and changelogs after v24.6.2.17-stable. [#66127](https://github.com/ClickHouse/ClickHouse/pull/66127) ([robot-clickhouse](https://github.com/robot-clickhouse)).
* Bump s2geometry again. [#66136](https://github.com/ClickHouse/ClickHouse/pull/66136) ([Robert Schulze](https://github.com/rschu1ze)).
* Switch submodule `contrib/orc` to a proper commit in the [main](https://github.com/ClickHouse/orc/tree/main) branch. Previously a commit from a removed branch was used ([see](https://github.com/ClickHouse/orc/pull/13)). [#66137](https://github.com/ClickHouse/ClickHouse/pull/66137) ([Vitaly Baranov](https://github.com/vitlibar)).
* Finalize MergedBlockOutputStream in dtor. [#66138](https://github.com/ClickHouse/ClickHouse/pull/66138) ([Nikita Taranov](https://github.com/nickitat)).
* Proper destruction order of AsyncLoader::Pool fields. [#66145](https://github.com/ClickHouse/ClickHouse/pull/66145) ([Sergei Trifonov](https://github.com/serxa)).
* Playing minesweeper with build system. [#66147](https://github.com/ClickHouse/ClickHouse/pull/66147) ([Nikita Taranov](https://github.com/nickitat)).
* Fix clang-tidy error in BufferWithOwnMemory.h. [#66161](https://github.com/ClickHouse/ClickHouse/pull/66161) ([Nikita Taranov](https://github.com/nickitat)).
* Use peak_threads_usage instead of arrayUniq(thread_ids) in tests. [#66162](https://github.com/ClickHouse/ClickHouse/pull/66162) ([Azat Khuzhin](https://github.com/azat)).
* Fix crash when adding empty tuple to query cache. [#66168](https://github.com/ClickHouse/ClickHouse/pull/66168) ([Michael Kolupaev](https://github.com/al13n321)).
* tests: fix 01563_distributed_query_finish flakiness (due to system.*_log_sender). [#66171](https://github.com/ClickHouse/ClickHouse/pull/66171) ([Azat Khuzhin](https://github.com/azat)).
* Refactor `OptimizeIfWithConstantConditionVisitor` using `InDepthNodeVisitor`. [#66184](https://github.com/ClickHouse/ClickHouse/pull/66184) ([zhongyuankai](https://github.com/zhongyuankai)).
* Update README.md. [#66186](https://github.com/ClickHouse/ClickHouse/pull/66186) ([Tyler Hannan](https://github.com/tylerhannan)).
* Fix 01246_buffer_flush flakiness. [#66188](https://github.com/ClickHouse/ClickHouse/pull/66188) ([Azat Khuzhin](https://github.com/azat)).
* Avoid using harmful function `rand()` in grpc. [#66191](https://github.com/ClickHouse/ClickHouse/pull/66191) ([Vitaly Baranov](https://github.com/vitlibar)).
* Bump RocksDB. [#66216](https://github.com/ClickHouse/ClickHouse/pull/66216) ([Robert Schulze](https://github.com/rschu1ze)).
* Update README.md. [#66217](https://github.com/ClickHouse/ClickHouse/pull/66217) ([Tyler Hannan](https://github.com/tylerhannan)).
* Fixes peak_threads_usage metric when materialised views are involved. [#66230](https://github.com/ClickHouse/ClickHouse/pull/66230) ([Sema Checherinda](https://github.com/CheSema)).
* Remove test as requested in https://github.com/ClickHouse/ClickHouse/pull/65277#issuecomment-2211361465. [#66233](https://github.com/ClickHouse/ClickHouse/pull/66233) ([Arthur Passos](https://github.com/arthurpassos)).
* Fix test `00504_mergetree_arrays_rw.sql`. [#66248](https://github.com/ClickHouse/ClickHouse/pull/66248) ([Anton Popov](https://github.com/CurtizJ)).
* CI: Do not finalize CI running status unless all success. [#66276](https://github.com/ClickHouse/ClickHouse/pull/66276) ([Max K.](https://github.com/maxknv)).
* Collect core dumps in more tests. [#66281](https://github.com/ClickHouse/ClickHouse/pull/66281) ([Antonio Andelic](https://github.com/antonio2368)).
* Add a stateless test for gRPC protocol. [#66284](https://github.com/ClickHouse/ClickHouse/pull/66284) ([Vitaly Baranov](https://github.com/vitlibar)).
* Log message: Failed to connect to replica ... [#66289](https://github.com/ClickHouse/ClickHouse/pull/66289) ([Igor Nikonov](https://github.com/devcrafter)).
* Update run.sh. [#66290](https://github.com/ClickHouse/ClickHouse/pull/66290) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Some changes in the codebase as a preparation for LLVM 18. [#66293](https://github.com/ClickHouse/ClickHouse/pull/66293) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* There's some problem with iptables in parallel tests. [#66304](https://github.com/ClickHouse/ClickHouse/pull/66304) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Disable broken cases from 02911_join_on_nullsafe_optimization. [#66310](https://github.com/ClickHouse/ClickHouse/pull/66310) ([vdimir](https://github.com/vdimir)).
* Build failure if ENABLE_AWS_S3 is OFF fixed. [#66335](https://github.com/ClickHouse/ClickHouse/pull/66335) ([Ilya Golshtein](https://github.com/ilejn)).
* Enable checks in assert_cast under sanitizers. [#66336](https://github.com/ClickHouse/ClickHouse/pull/66336) ([Nikita Taranov](https://github.com/nickitat)).
* Create release workflow. [#66339](https://github.com/ClickHouse/ClickHouse/pull/66339) ([Max K.](https://github.com/maxknv)).
* Fix invalid XML. [#66342](https://github.com/ClickHouse/ClickHouse/pull/66342) ([János Benjamin Antal](https://github.com/antaljanosbenjamin)).
* Fix possible deadlock for jemalloc with enabled profiler. [#66346](https://github.com/ClickHouse/ClickHouse/pull/66346) ([Azat Khuzhin](https://github.com/azat)).
* Fix test_parallel_replicas_custom_key. [#66349](https://github.com/ClickHouse/ClickHouse/pull/66349) ([Antonio Andelic](https://github.com/antonio2368)).
* Collect logs from `minio` in stateless and statefull tests. [#66353](https://github.com/ClickHouse/ClickHouse/pull/66353) ([János Benjamin Antal](https://github.com/antaljanosbenjamin)).
* Fix "Sending a batch of X files to Y (0.00 rows, 0.00 B bytes)." in case of batch restoring. [#66375](https://github.com/ClickHouse/ClickHouse/pull/66375) ([Azat Khuzhin](https://github.com/azat)).
* Fix 03030_system_flush_distributed_settings flakiness. [#66376](https://github.com/ClickHouse/ClickHouse/pull/66376) ([Azat Khuzhin](https://github.com/azat)).
* PR cleanup: remove redundant code. [#66380](https://github.com/ClickHouse/ClickHouse/pull/66380) ([Igor Nikonov](https://github.com/devcrafter)).
* New slack bot to post messages about CI events - Post message if OOM. [#66392](https://github.com/ClickHouse/ClickHouse/pull/66392) ([Max K.](https://github.com/maxknv)).
* Ignore subquery for IN in DDLLoadingDependencyVisitor. [#66395](https://github.com/ClickHouse/ClickHouse/pull/66395) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Update test_storage_rabbitmq/test.py. [#66396](https://github.com/ClickHouse/ClickHouse/pull/66396) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Add query elapsed time for non-default format in play UI. [#66398](https://github.com/ClickHouse/ClickHouse/pull/66398) ([Azat Khuzhin](https://github.com/azat)).
* Untangle setting headers. [#66404](https://github.com/ClickHouse/ClickHouse/pull/66404) ([Raúl Marín](https://github.com/Algunenano)).
* Remove noisy message. [#66406](https://github.com/ClickHouse/ClickHouse/pull/66406) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* If job detected as in-progress in PR workflow run - just remove it from todo list, as it not affected by the change. [#66407](https://github.com/ClickHouse/ClickHouse/pull/66407) ([Max K.](https://github.com/maxknv)).
* CI: CIBuddy to post from master only. [#66417](https://github.com/ClickHouse/ClickHouse/pull/66417) ([Max K.](https://github.com/maxknv)).
* Add a test for [#66333](https://github.com/ClickHouse/ClickHouse/issues/66333). [#66432](https://github.com/ClickHouse/ClickHouse/pull/66432) ([max-vostrikov](https://github.com/max-vostrikov)).
* Limit number of linker jobs on arm to avoid OOM during build. [#66435](https://github.com/ClickHouse/ClickHouse/pull/66435) ([Nikita Taranov](https://github.com/nickitat)).
* [RFC] Fix jemalloc assertion due to non-monotonic CLOCK_MONOTONIC_COARSE. [#66439](https://github.com/ClickHouse/ClickHouse/pull/66439) ([Azat Khuzhin](https://github.com/azat)).
* CI: Do not block CI on few number of test failures. [#66440](https://github.com/ClickHouse/ClickHouse/pull/66440) ([Max K.](https://github.com/maxknv)).
* Stateless tests: fix flaky tests 01037_polygon_dicts*. [#66445](https://github.com/ClickHouse/ClickHouse/pull/66445) ([Nikita Fomichev](https://github.com/fm4v)).
* Related to https://github.com/ClickHouse/ClickHouse/pull/62067 https://s3.amazonaws.com/clickhouse-test-reports/66410/5557dce188cabc7477bb4e874d47e3b80278ee66/stateless_tests__release_.html ``` 2024-07-12 16:04:29 +Queries for alter_table did not finish automatically after 250+ seconds 2024-07-12 16:04:29 +==================== QUERIES ==================== 2024-07-12 16:04:29 +Row 1: 2024-07-12 16:04:29 +────── 2024-07-12 16:04:29 +is_initial_query: 1 2024-07-12 16:04:29 +user: default 2024-07-12 16:04:29 +query_id: b43ffd7d-aee6-4161-aa82-bf9fff9d78c0 2024-07-12 16:04:29 +address: ::1 2024-07-12 16:04:29 +port: 58360 2024-07-12 16:04:29 +initial_user: default 2024-07-12 16:04:29 +initial_query_id: b43ffd7d-aee6-4161-aa82-bf9fff9d78c0 ... 2024-07-12 16:04:29 +query: OPTIMIZE TABLE alter_table0 FINAL ```. [#66460](https://github.com/ClickHouse/ClickHouse/pull/66460) ([Alexander Tokmakov](https://github.com/tavplubix)).
* OOM error was not visible since process is killed and status is not set Change sets ERROR status if job was killed. [#66463](https://github.com/ClickHouse/ClickHouse/pull/66463) ([Max K.](https://github.com/maxknv)).
* Add AST fuzzers jobs for CI caching so that they can be skipped in PRs not related to build or tests. [#66468](https://github.com/ClickHouse/ClickHouse/pull/66468) ([Max K.](https://github.com/maxknv)).
* If job with the same digest has been seen in master's CI it should be skipped in PR run. [#66471](https://github.com/ClickHouse/ClickHouse/pull/66471) ([Max K.](https://github.com/maxknv)).
* CI: Check job's exit status and report if killed. [#66477](https://github.com/ClickHouse/ClickHouse/pull/66477) ([Max K.](https://github.com/maxknv)).
* This closes [#37557](https://github.com/ClickHouse/ClickHouse/issues/37557). [#66482](https://github.com/ClickHouse/ClickHouse/pull/66482) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* CI: Add retry for GH set_status_comment call. [#66488](https://github.com/ClickHouse/ClickHouse/pull/66488) ([Max K.](https://github.com/maxknv)).
* OpenSSL: Minor follow-up to [#66064](https://github.com/ClickHouse/ClickHouse/issues/66064). [#66489](https://github.com/ClickHouse/ClickHouse/pull/66489) ([Robert Schulze](https://github.com/rschu1ze)).
* CI: Fix for job filtering in PRs. [#66490](https://github.com/ClickHouse/ClickHouse/pull/66490) ([Max K.](https://github.com/maxknv)).
* CI: Create release workflow updates. [#66498](https://github.com/ClickHouse/ClickHouse/pull/66498) ([Max K.](https://github.com/maxknv)).
* Add one more revision to ignore. [#66499](https://github.com/ClickHouse/ClickHouse/pull/66499) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Unit tests vomit a ton of garbage, see https://s3.amazonaws.com/clickhouse-test-reports/66457/0c82dc91f07b29ba503d7579c7d3ebecba532b73/unit_tests__tsan_/run.log - remove it. [#66501](https://github.com/ClickHouse/ClickHouse/pull/66501) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix double whitespace in EXPLAIN AST CREATE. [#66505](https://github.com/ClickHouse/ClickHouse/pull/66505) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix bad test `02530_dictionaries_update_field`. [#66507](https://github.com/ClickHouse/ClickHouse/pull/66507) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Https://play.clickhouse.com/play?user=play#u0vmrunuignozwnrx3n0yxj0x3rpbwusignozwnrx25hbwusihrlc3rfbmftzswgcmvwb3j0x3vybapguk9nignozwnrcwpxsevsrsbjagvja19zdgfydf90aw1lid49ig5vdygpic0gsu5urvjwquwgmjqwiehpvvikicagieforcbwdwxsx3jlcxvlc3rfbnvtymvyid0gmaogicagqu5eihrlc3rfc3rhdhvzice9icdts0lquevejwogicagqu5eihrlc3rfc3rhdhvziexjs0ugj0yljwogicagqu5eignozwnrx3n0yxr1cyahpsanc3vjy2vzcyckicagieforcbwb3npdglvbih0zxn0x25hbwusicdhcgfjagvfyxjyb3cnksa+idakt1jervigqlkgy2hly2tfc3rhcnrfdgltzq==. [#66508](https://github.com/ClickHouse/ClickHouse/pull/66508) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix MSan report in GRPC. [#66509](https://github.com/ClickHouse/ClickHouse/pull/66509) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* CI: Fix for skipping Builds_2 in PRs' CI. [#66512](https://github.com/ClickHouse/ClickHouse/pull/66512) ([Max K.](https://github.com/maxknv)).
* CI: Do not block Tests_3 unless MAX_FAILED_TESTS exceeded. [#66513](https://github.com/ClickHouse/ClickHouse/pull/66513) ([Max K.](https://github.com/maxknv)).
* Fix `02918_parallel_replicas_custom_key_unavailable_replica`. [#66516](https://github.com/ClickHouse/ClickHouse/pull/66516) ([Antonio Andelic](https://github.com/antonio2368)).
* Stateless tests: improvements related to OOM of test runs. [#66520](https://github.com/ClickHouse/ClickHouse/pull/66520) ([Nikita Fomichev](https://github.com/fm4v)).
* Tests: rename bad log names. [#66522](https://github.com/ClickHouse/ClickHouse/pull/66522) ([Nikita Fomichev](https://github.com/fm4v)).
* Add additional log masking in CI. [#66523](https://github.com/ClickHouse/ClickHouse/pull/66523) ([Raúl Marín](https://github.com/Algunenano)).
* CI: Multiple fixes for handling killed jobs. [#66524](https://github.com/ClickHouse/ClickHouse/pull/66524) ([Max K.](https://github.com/maxknv)).
* Allow GWP Asan allocations only when initialization is finished. [#66526](https://github.com/ClickHouse/ClickHouse/pull/66526) ([Alexey Katsman](https://github.com/alexkats)).
* Update 02443_detach_attach_partition.sh. [#66529](https://github.com/ClickHouse/ClickHouse/pull/66529) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Follow up [#66435](https://github.com/ClickHouse/ClickHouse/issues/66435). [#66530](https://github.com/ClickHouse/ClickHouse/pull/66530) ([Nikita Taranov](https://github.com/nickitat)).
* fix log in keeper tcp handler. [#66531](https://github.com/ClickHouse/ClickHouse/pull/66531) ([Han Fei](https://github.com/hanfei1991)).
* CI: Report job start and finish to CI DB. [#66533](https://github.com/ClickHouse/ClickHouse/pull/66533) ([Max K.](https://github.com/maxknv)).
* Update 01396_inactive_replica_cleanup_nodes_zookeeper.sh. [#66535](https://github.com/ClickHouse/ClickHouse/pull/66535) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Add dedicated runner to libfuzzer, update docker. [#66551](https://github.com/ClickHouse/ClickHouse/pull/66551) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* fix tidy build. [#66552](https://github.com/ClickHouse/ClickHouse/pull/66552) ([Sema Checherinda](https://github.com/CheSema)).
* No-op env change. [#66553](https://github.com/ClickHouse/ClickHouse/pull/66553) ([Raúl Marín](https://github.com/Algunenano)).
* Fix typo in new_delete.cpp. [#66554](https://github.com/ClickHouse/ClickHouse/pull/66554) ([alesapin](https://github.com/alesapin)).
* Fix something in Fast Test. [#66558](https://github.com/ClickHouse/ClickHouse/pull/66558) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* update trusted contributors. [#66561](https://github.com/ClickHouse/ClickHouse/pull/66561) ([Xu Jia](https://github.com/XuJia0210)).
* Delete bad test `02805_distributed_queries_timeouts`. [#66563](https://github.com/ClickHouse/ClickHouse/pull/66563) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* More clarity in the test `03001_consider_lwd_when_merge`. [#66564](https://github.com/ClickHouse/ClickHouse/pull/66564) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Stateless tests: temporary disable sequential tests in parallel. [#66585](https://github.com/ClickHouse/ClickHouse/pull/66585) ([Nikita Fomichev](https://github.com/fm4v)).
* Move view targets to separate AST class `ASTViewTargets` in order to allow extending it to support more kinds of view targets. [#66590](https://github.com/ClickHouse/ClickHouse/pull/66590) ([Vitaly Baranov](https://github.com/vitlibar)).
* Fix bsdtar for @nikitamikhaylov. [#66592](https://github.com/ClickHouse/ClickHouse/pull/66592) ([alesapin](https://github.com/alesapin)).
* CI: POC for Auto Releases. [#66593](https://github.com/ClickHouse/ClickHouse/pull/66593) ([Max K.](https://github.com/maxknv)).
* Fix clang tidy after [#66402](https://github.com/ClickHouse/ClickHouse/issues/66402). [#66597](https://github.com/ClickHouse/ClickHouse/pull/66597) ([vdimir](https://github.com/vdimir)).
* Adjust the runtime of some slow performance test. [#66619](https://github.com/ClickHouse/ClickHouse/pull/66619) ([Robert Schulze](https://github.com/rschu1ze)).
* CI: Scale down AutoScaling Groups from runners. [#66622](https://github.com/ClickHouse/ClickHouse/pull/66622) ([Max K.](https://github.com/maxknv)).
* Allow to run clang-tidy with clang-19. [#66625](https://github.com/ClickHouse/ClickHouse/pull/66625) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix terrible test @arthurpassos. [#66632](https://github.com/ClickHouse/ClickHouse/pull/66632) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix bad log message. [#66633](https://github.com/ClickHouse/ClickHouse/pull/66633) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Negative sign in prewhere optimization. [#66637](https://github.com/ClickHouse/ClickHouse/pull/66637) ([cangyin](https://github.com/cangyin)).
* Closes [#66639](https://github.com/ClickHouse/ClickHouse/issues/66639#event-13533944949). [#66640](https://github.com/ClickHouse/ClickHouse/pull/66640) ([Kruglov Pavel](https://github.com/Avogar)).
* Avoid generating named tuple for special keywords (null, true, false). [#66641](https://github.com/ClickHouse/ClickHouse/pull/66641) ([Amos Bird](https://github.com/amosbird)).
* rearrange heavy tests 03008_deduplication. [#66642](https://github.com/ClickHouse/ClickHouse/pull/66642) ([Sema Checherinda](https://github.com/CheSema)).
* Fix data race in S3::ClientCache. [#66644](https://github.com/ClickHouse/ClickHouse/pull/66644) ([Konstantin Morozov](https://github.com/k-morozov)).
* CI: Remove aws lambda packages from oss. [#66651](https://github.com/ClickHouse/ClickHouse/pull/66651) ([Max K.](https://github.com/maxknv)).
* Introduce separate DEBUG_OR_SANITIZER_BUILD macro. [#66652](https://github.com/ClickHouse/ClickHouse/pull/66652) ([Nikita Taranov](https://github.com/nickitat)).
* Increase backoff because with slow builds sometimes 100ms is not enough to recover. [#66653](https://github.com/ClickHouse/ClickHouse/pull/66653) ([alesapin](https://github.com/alesapin)).
* Fix wrong queries hung error because of 02044_url_glob_parallel_connection_refused. [#66657](https://github.com/ClickHouse/ClickHouse/pull/66657) ([Nikita Taranov](https://github.com/nickitat)).
* add log for splitBlockIntoParts. [#66658](https://github.com/ClickHouse/ClickHouse/pull/66658) ([Han Fei](https://github.com/hanfei1991)).
* Minor: Make `CaseSensitiveness` an enum class. [#66673](https://github.com/ClickHouse/ClickHouse/pull/66673) ([Robert Schulze](https://github.com/rschu1ze)).
* Fix bad test `02210_processors_profile_log`. [#66684](https://github.com/ClickHouse/ClickHouse/pull/66684) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix something around clang-tidy. [#66694](https://github.com/ClickHouse/ClickHouse/pull/66694) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* ci: dump dmesg in case of OOM. [#66705](https://github.com/ClickHouse/ClickHouse/pull/66705) ([Azat Khuzhin](https://github.com/azat)).
* fix clang tidy. [#66706](https://github.com/ClickHouse/ClickHouse/pull/66706) ([Han Fei](https://github.com/hanfei1991)).
* Https://s3.amazonaws.com/clickhouse-test-reports/61109/5cf2b53f146c1a4f24d8212f9f810d587c46bfc0/stateless_tests__release_.html. [#66724](https://github.com/ClickHouse/ClickHouse/pull/66724) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Support one more case in JOIN ON ... IS NULL. [#66725](https://github.com/ClickHouse/ClickHouse/pull/66725) ([vdimir](https://github.com/vdimir)).
* CI: Fix issue with a skipped Build report. [#66726](https://github.com/ClickHouse/ClickHouse/pull/66726) ([Max K.](https://github.com/maxknv)).
* relax condition in test, remove unused counters. [#66730](https://github.com/ClickHouse/ClickHouse/pull/66730) ([Sema Checherinda](https://github.com/CheSema)).
* Remove bad test `host_resolver_fail_count`. [#66731](https://github.com/ClickHouse/ClickHouse/pull/66731) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix bad test `03036_join_filter_push_down_equivalent_sets`. [#66736](https://github.com/ClickHouse/ClickHouse/pull/66736) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix bad tests `long_select_and_alter`. [#66737](https://github.com/ClickHouse/ClickHouse/pull/66737) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Add test test_storage_mysql/test.py::test_joins. [#66743](https://github.com/ClickHouse/ClickHouse/pull/66743) ([vdimir](https://github.com/vdimir)).
* Disallow build exclusion only by CI settings (ci_include_, ci_exclude_) to avoid running builds in auto sync prs. [#66744](https://github.com/ClickHouse/ClickHouse/pull/66744) ([Max K.](https://github.com/maxknv)).
* Use non-existent address to check connection error at table creation. [#66760](https://github.com/ClickHouse/ClickHouse/pull/66760) ([János Benjamin Antal](https://github.com/antaljanosbenjamin)).
* Backported in [#67063](https://github.com/ClickHouse/ClickHouse/issues/67063): Increase asio pool size in case the server is tiny. [#66761](https://github.com/ClickHouse/ClickHouse/pull/66761) ([alesapin](https://github.com/alesapin)).
* Fix flakiness of async insert tests due to adaptive timeout. [#66771](https://github.com/ClickHouse/ClickHouse/pull/66771) ([Raúl Marín](https://github.com/Algunenano)).
* Attempt to fix flakiness of 01194_http_query_id. [#66774](https://github.com/ClickHouse/ClickHouse/pull/66774) ([Raúl Marín](https://github.com/Algunenano)).
* Turn off randomization of harmful setting. [#66776](https://github.com/ClickHouse/ClickHouse/pull/66776) ([alesapin](https://github.com/alesapin)).
* The number of batches was reduced in https://github.com/ClickHouse/ClickHouse/pull/65186, but then the parallel execution was disabled in https://github.com/ClickHouse/ClickHouse/pull/66585. So now tasks fail with timeout sometimes: https://s3.amazonaws.com/clickhouse-test-reports/66724/36275fdacc34206931f69087fe77539e25bbbedd/stateless_tests__tsan__s3_storage__[2_3].html. [#66783](https://github.com/ClickHouse/ClickHouse/pull/66783) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Remove obsolete code from CMakeLists. [#66786](https://github.com/ClickHouse/ClickHouse/pull/66786) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Stateless tests: split parallel tests more evenly. [#66787](https://github.com/ClickHouse/ClickHouse/pull/66787) ([Nikita Fomichev](https://github.com/fm4v)).
* Fix test `02724_limit_num_mutations.sh`. [#66788](https://github.com/ClickHouse/ClickHouse/pull/66788) ([Anton Popov](https://github.com/CurtizJ)).
* Better diagnostics in `test_disk_configuration`. [#66802](https://github.com/ClickHouse/ClickHouse/pull/66802) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix bad test `02950_part_log_bytes_uncompressed`. [#66803](https://github.com/ClickHouse/ClickHouse/pull/66803) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Better diagnostics for test trace_events_stress. [#66804](https://github.com/ClickHouse/ClickHouse/pull/66804) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Make test `00997_set_index_array` lighter. [#66817](https://github.com/ClickHouse/ClickHouse/pull/66817) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Miscellaneous. [#66818](https://github.com/ClickHouse/ClickHouse/pull/66818) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix inconsistent formatting of lambda functions inside composite types. [#66819](https://github.com/ClickHouse/ClickHouse/pull/66819) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Small fix in realloc memory tracking. [#66820](https://github.com/ClickHouse/ClickHouse/pull/66820) ([Antonio Andelic](https://github.com/antonio2368)).
* Never await in CI on BuildReport - just redo (await can be longer) - Remove BuildReport if no build jobs in workflow (for instance: Docs change) - Do not fail CheckReadyForMerge job if the only non-green status is Cloud Sync. [#66822](https://github.com/ClickHouse/ClickHouse/pull/66822) ([Max K.](https://github.com/maxknv)).
* Remove bad tests @azat. [#66823](https://github.com/ClickHouse/ClickHouse/pull/66823) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* CI: New Release workflow updates and fixes. [#66830](https://github.com/ClickHouse/ClickHouse/pull/66830) ([Max K.](https://github.com/maxknv)).
* Fix signed integer overflow in function `age`. [#66831](https://github.com/ClickHouse/ClickHouse/pull/66831) ([Robert Schulze](https://github.com/rschu1ze)).
* Fix RocksDB bs. [#66838](https://github.com/ClickHouse/ClickHouse/pull/66838) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Split a test for index. [#66839](https://github.com/ClickHouse/ClickHouse/pull/66839) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix inconsistent formatting of `NOT ((SELECT ...))`. [#66840](https://github.com/ClickHouse/ClickHouse/pull/66840) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Make test `01592_long_window_functions1` lighter. [#66841](https://github.com/ClickHouse/ClickHouse/pull/66841) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* What if I will change the test for SSL authentication?. [#66844](https://github.com/ClickHouse/ClickHouse/pull/66844) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Functions `[a-g]*`: Iterate over `input_rows_count` where appropriate. [#66846](https://github.com/ClickHouse/ClickHouse/pull/66846) ([Robert Schulze](https://github.com/rschu1ze)).
* Revert libunwind patch. [#66850](https://github.com/ClickHouse/ClickHouse/pull/66850) ([Antonio Andelic](https://github.com/antonio2368)).
* Split test 03038_nested_dynamic_merges to avoid timeouts. [#66863](https://github.com/ClickHouse/ClickHouse/pull/66863) ([Kruglov Pavel](https://github.com/Avogar)).
* CI: Print instance info in runner's init script. [#66868](https://github.com/ClickHouse/ClickHouse/pull/66868) ([Max K.](https://github.com/maxknv)).
* Backported in [#67257](https://github.com/ClickHouse/ClickHouse/issues/67257): Followup [#66725](https://github.com/ClickHouse/ClickHouse/issues/66725). [#66869](https://github.com/ClickHouse/ClickHouse/pull/66869) ([vdimir](https://github.com/vdimir)).
* CI: CI Buddy to notify about fatal workflow failures. [#66890](https://github.com/ClickHouse/ClickHouse/pull/66890) ([Max K.](https://github.com/maxknv)).
* CI: Add ec2 instance lifecycle metadata to CIDB. [#66918](https://github.com/ClickHouse/ClickHouse/pull/66918) ([Max K.](https://github.com/maxknv)).
* CI: Remove ci runners scripts from oss. [#66920](https://github.com/ClickHouse/ClickHouse/pull/66920) ([Max K.](https://github.com/maxknv)).
* Backported in [#67209](https://github.com/ClickHouse/ClickHouse/issues/67209): Decrease rate limit in `01923_network_receive_time_metric_insert`. [#66924](https://github.com/ClickHouse/ClickHouse/pull/66924) ([János Benjamin Antal](https://github.com/antaljanosbenjamin)).
* Backported in [#67227](https://github.com/ClickHouse/ClickHouse/issues/67227): Grouparrayintersect: fix serialization bug. [#66928](https://github.com/ClickHouse/ClickHouse/pull/66928) ([Raúl Marín](https://github.com/Algunenano)).
* Backported in [#67207](https://github.com/ClickHouse/ClickHouse/issues/67207): Un-flake test_runtime_configurable_cache_size. [#66934](https://github.com/ClickHouse/ClickHouse/pull/66934) ([Robert Schulze](https://github.com/rschu1ze)).
* Backported in [#66975](https://github.com/ClickHouse/ClickHouse/issues/66975): CI: Fixes docker server build for release branches. [#66955](https://github.com/ClickHouse/ClickHouse/pull/66955) ([Max K.](https://github.com/maxknv)).
* Backported in [#67213](https://github.com/ClickHouse/ClickHouse/issues/67213): [CI Fest] Split dynamic tests and rewrite them from sh to sql to avoid timeouts. [#66981](https://github.com/ClickHouse/ClickHouse/pull/66981) ([Kruglov Pavel](https://github.com/Avogar)).
* Backported in [#67033](https://github.com/ClickHouse/ClickHouse/issues/67033): [CI Fest] Fix use-of-uninitialized-value in JSONExtract* numeric functions. [#66984](https://github.com/ClickHouse/ClickHouse/pull/66984) ([Kruglov Pavel](https://github.com/Avogar)).
* Backported in [#67051](https://github.com/ClickHouse/ClickHouse/issues/67051): CI: Fix for workflow results parsing. [#67000](https://github.com/ClickHouse/ClickHouse/pull/67000) ([Max K.](https://github.com/maxknv)).
* Backported in [#67116](https://github.com/ClickHouse/ClickHouse/issues/67116): Disable setting `optimize_functions_to_subcolumns`. [#67046](https://github.com/ClickHouse/ClickHouse/pull/67046) ([Anton Popov](https://github.com/CurtizJ)).
* Backported in [#67205](https://github.com/ClickHouse/ClickHouse/issues/67205): Increase max allocation size for sanitizers. [#67049](https://github.com/ClickHouse/ClickHouse/pull/67049) ([János Benjamin Antal](https://github.com/antaljanosbenjamin)).
* Backported in [#67124](https://github.com/ClickHouse/ClickHouse/issues/67124): Very sad failure: ``` 2024.07.24 13:28:45.517777 [ 10 ] {08745bf9-4bc1-4946-b9a8-c03d82ec55dc} <Debug> executeQuery: (from 172.16.11.1:55890) OPTIMIZE TABLE replicated_mt FINAL (stage: Complete) 2024.07.24 13:28:45.525945 [ 10 ] {08745bf9-4bc1-4946-b9a8-c03d82ec55dc} <Trace> default.replicated_mt (ReplicatedMergeTreeQueue): Waiting for 4 entries to be processed: queue-0000000004, queue-0000000002, queue-0000000001, queue-0000000000 2024.07.24 13:29:15.528024 [ 10 ] {08745bf9-4bc1-4946-b9a8-c03d82ec55dc} <Debug> default.replicated_mt (6581a6fb-8458-466d-8350-89951eb1ac8e) (MergerMutator): Selected 3 parts from all_0_0_0 to all_2_2_0 2024.07.24 13:29:15.530736 [ 10 ] {08745bf9-4bc1-4946-b9a8-c03d82ec55dc} <Trace> default.replicated_mt (6581a6fb-8458-466d-8350-89951eb1ac8e): Created log entry /clickhouse/tables/replicated_mt/log/log-0000000004 for merge all_0_2_1 2024.07.24 13:29:15.530873 [ 10 ] {08745bf9-4bc1-4946-b9a8-c03d82ec55dc} <Debug> default.replicated_mt (6581a6fb-8458-466d-8350-89951eb1ac8e): Waiting for node1 to process log entry 2024.07.24 13:29:15.530919 [ 10 ] {08745bf9-4bc1-4946-b9a8-c03d82ec55dc} <Debug> default.replicated_mt (6581a6fb-8458-466d-8350-89951eb1ac8e): Waiting for node1 to pull log-0000000004 to queue 2024.07.24 13:29:15.534286 [ 10 ] {08745bf9-4bc1-4946-b9a8-c03d82ec55dc} <Debug> default.replicated_mt (6581a6fb-8458-466d-8350-89951eb1ac8e): Looking for node corresponding to log-0000000004 in node1 queue 2024.07.24 13:29:15.534793 [ 10 ] {08745bf9-4bc1-4946-b9a8-c03d82ec55dc} <Debug> default.replicated_mt (6581a6fb-8458-466d-8350-89951eb1ac8e): Waiting for queue-0000000005 to disappear from node1 queue 2024.07.24 13:29:15.585533 [ 10 ] {08745bf9-4bc1-4946-b9a8-c03d82ec55dc} <Debug> TCPHandler: Processed in 30.067804125 sec. ```. [#67067](https://github.com/ClickHouse/ClickHouse/pull/67067) ([alesapin](https://github.com/alesapin)).
* Backported in [#67203](https://github.com/ClickHouse/ClickHouse/issues/67203): Fix flaky `test_seekable_formats_url` and `test_seekable_formats` S3 storage tests. [#67070](https://github.com/ClickHouse/ClickHouse/pull/67070) ([János Benjamin Antal](https://github.com/antaljanosbenjamin)).
* Backported in [#67222](https://github.com/ClickHouse/ClickHouse/issues/67222): Fix 2680 flasky. [#67078](https://github.com/ClickHouse/ClickHouse/pull/67078) ([jsc0218](https://github.com/jsc0218)).
* Backported in [#67190](https://github.com/ClickHouse/ClickHouse/issues/67190): Attempt to fix flakiness of some window view tests. [#67130](https://github.com/ClickHouse/ClickHouse/pull/67130) ([Robert Schulze](https://github.com/rschu1ze)).
* Backported in [#67272](https://github.com/ClickHouse/ClickHouse/issues/67272): Rename (unreleased) bad setting. [#67149](https://github.com/ClickHouse/ClickHouse/pull/67149) ([Raúl Marín](https://github.com/Algunenano)).
* Backported in [#67441](https://github.com/ClickHouse/ClickHouse/issues/67441): Try to fix 2572. [#67158](https://github.com/ClickHouse/ClickHouse/pull/67158) ([jsc0218](https://github.com/jsc0218)).
* Backported in [#67416](https://github.com/ClickHouse/ClickHouse/issues/67416): CI: Fix build results for release branches. [#67402](https://github.com/ClickHouse/ClickHouse/pull/67402) ([Max K.](https://github.com/maxknv)).

