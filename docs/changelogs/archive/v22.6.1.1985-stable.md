---
sidebar_position: 1
sidebar_label: 2022
---

# 2022 Changelog

### ClickHouse release v22.6.1.1985-stable (7000c4e0033) FIXME as compared to v22.5.1.2079-stable (df0cb062098)

#### Backward Incompatible Change
* Changes how settings using `seconds` as type are parsed to support floating point values (for example: `max_execution_time=0.5`). Infinity or NaN values will throw an exception. [#37187](https://github.com/ClickHouse/ClickHouse/pull/37187) ([Raúl Marín](https://github.com/Algunenano)).
* Changed format of binary serialization of columns of experimental type `Object`. New format is more convenient to implement by third-party clients. [#37482](https://github.com/ClickHouse/ClickHouse/pull/37482) ([Anton Popov](https://github.com/CurtizJ)).
* Turn on setting `output_format_json_named_tuples_as_objects` by default. It allows to serialize named tuples as JSON objects in JSON formats. [#37756](https://github.com/ClickHouse/ClickHouse/pull/37756) ([Anton Popov](https://github.com/CurtizJ)).

#### New Feature
* Added `SYSTEM UNFREEZE` query that deletes the whole backup regardless if the corresponding table is deleted or not. [#36424](https://github.com/ClickHouse/ClickHouse/pull/36424) ([Vadim Volodin](https://github.com/PolyProgrammist)).
* Adds H3 unidirectional edge functions. [#36843](https://github.com/ClickHouse/ClickHouse/pull/36843) ([Bharat Nallan](https://github.com/bharatnc)).
* Add merge_reason column to system.part_log table. [#36912](https://github.com/ClickHouse/ClickHouse/pull/36912) ([Sema Checherinda](https://github.com/CheSema)).
* This PR enables `POPULATE` for WindowView. [#36945](https://github.com/ClickHouse/ClickHouse/pull/36945) ([vxider](https://github.com/Vxider)).
* Add new columnar JSON formats: JSONColumns, JSONCompactColumns, JSONColumnsWithMetadata. Closes [#36338](https://github.com/ClickHouse/ClickHouse/issues/36338) Closes [#34509](https://github.com/ClickHouse/ClickHouse/issues/34509). [#36975](https://github.com/ClickHouse/ClickHouse/pull/36975) ([Kruglov Pavel](https://github.com/Avogar)).
* Add support for calculating [hashids](https://hashids.org/) from unsigned integers. [#37013](https://github.com/ClickHouse/ClickHouse/pull/37013) ([Michael Nutt](https://github.com/mnutt)).
* Add GROUPING function. Closes [#19426](https://github.com/ClickHouse/ClickHouse/issues/19426). [#37163](https://github.com/ClickHouse/ClickHouse/pull/37163) ([Dmitry Novik](https://github.com/novikd)).
* `ALTER TABLE ... MODIFY QUERY` support for WindowView. [#37188](https://github.com/ClickHouse/ClickHouse/pull/37188) ([vxider](https://github.com/Vxider)).
* This PR changes the behavior of the `ENGINE` syntax in WindowView, to make it like in MaterializedView. [#37214](https://github.com/ClickHouse/ClickHouse/pull/37214) ([vxider](https://github.com/Vxider)).
* SALT is allowed for CREATE USER <user> IDENTIFIED WITH sha256_hash. [#37377](https://github.com/ClickHouse/ClickHouse/pull/37377) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Implemented changing comment to a ReplicatedMergeTree table. [#37416](https://github.com/ClickHouse/ClickHouse/pull/37416) ([Vasily Nemkov](https://github.com/Enmk)).
* Add support for Maps and Records in Avro format. Add new setting `input_format_avro_null_as_default ` that allow to insert null as default in Avro format. Closes [#18925](https://github.com/ClickHouse/ClickHouse/issues/18925) Closes [#37378](https://github.com/ClickHouse/ClickHouse/issues/37378) Closes [#32899](https://github.com/ClickHouse/ClickHouse/issues/32899). [#37525](https://github.com/ClickHouse/ClickHouse/pull/37525) ([Kruglov Pavel](https://github.com/Avogar)).
* Add two new settings `input_format_csv_skip_first_lines/input_format_tsv_skip_first_lines` to allow skipping specified number of lines in the beginning of the file in CSV/TSV formats. [#37537](https://github.com/ClickHouse/ClickHouse/pull/37537) ([Kruglov Pavel](https://github.com/Avogar)).
* showCertificate() function shows current server's SSL certificate. [#37540](https://github.com/ClickHouse/ClickHouse/pull/37540) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Implementation of [FPC](https://userweb.cs.txstate.edu/~burtscher/papers/dcc07a.pdf) algorithm for floating point data compression. [#37553](https://github.com/ClickHouse/ClickHouse/pull/37553) ([Mikhail Guzov](https://github.com/koloshmet)).
* HTTP source for Data Dictionaries in Named Collections is supported. [#37581](https://github.com/ClickHouse/ClickHouse/pull/37581) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* This PR aims to resolve [#22130](https://github.com/ClickHouse/ClickHouse/issues/22130) which allows inserting into `system.zookeeper`. To simplify, this PR is designed as:. [#37596](https://github.com/ClickHouse/ClickHouse/pull/37596) ([Han Fei](https://github.com/hanfei1991)).
* Added a new window function `nonNegativeDerivative(metric_column, timestamp_column[, INTERVAL x SECOND])`. [#37628](https://github.com/ClickHouse/ClickHouse/pull/37628) ([Andrey Zvonov](https://github.com/zvonand)).
* Executable user defined functions support parameters. Example: `SELECT test_function(parameters)(arguments)`. Closes [#37578](https://github.com/ClickHouse/ClickHouse/issues/37578). [#37720](https://github.com/ClickHouse/ClickHouse/pull/37720) ([Maksim Kita](https://github.com/kitaisreal)).
* Added open telemetry traces visualizing tool based on d3js. [#37810](https://github.com/ClickHouse/ClickHouse/pull/37810) ([Sergei Trifonov](https://github.com/serxa)).

#### Performance Improvement
* Improve performance of insert into MergeTree if there are multiple columns in ORDER BY. [#35762](https://github.com/ClickHouse/ClickHouse/pull/35762) ([Maksim Kita](https://github.com/kitaisreal)).
* Apply read method 'threadpool' for StorageHive. [#36328](https://github.com/ClickHouse/ClickHouse/pull/36328) ([李扬](https://github.com/taiyang-li)).
* Now we split data parts into layers and distribute them among threads instead of whole parts to make the execution of queries with `FINAL` more data-parallel. [#36396](https://github.com/ClickHouse/ClickHouse/pull/36396) ([Nikita Taranov](https://github.com/nickitat)).
* Load marks for only necessary columns when reading wide parts. [#36879](https://github.com/ClickHouse/ClickHouse/pull/36879) ([Anton Kozlov](https://github.com/tonickkozlov)).
* When all the columns to read are partition keys, construct columns by the file's row number without real reading the hive file. [#37103](https://github.com/ClickHouse/ClickHouse/pull/37103) ([lgbo](https://github.com/lgbo-ustc)).
* Fix performance of `dictGetDescendants`, `dictGetChildren` functions, create temporary parent to children hierarchical index per query, not per function call during query. Allow to specify `BIDIRECTIONAL` for `HIERARHICAL` attributes, dictionary will maintain parent to children index in memory, that way functions `dictGetDescendants`, `dictGetChildren` will not create temporary index per query. Closes [#32481](https://github.com/ClickHouse/ClickHouse/issues/32481). [#37148](https://github.com/ClickHouse/ClickHouse/pull/37148) ([Maksim Kita](https://github.com/kitaisreal)).
* Improve performance and memory usage for select of subset of columns for formats Native, Protobuf, CapnProto, JSONEachRow, TSKV, all formats with suffixes WithNames/WithNamesAndTypes. Previously while selecting only subset of columns from files in these formats all columns were read and stored in memory. Now only required columns are read. This PR enables setting `input_format_skip_unknown_fields` by default, because otherwise in case of select of subset of columns exception will be thrown. [#37192](https://github.com/ClickHouse/ClickHouse/pull/37192) ([Kruglov Pavel](https://github.com/Avogar)).
* Improve sort performance by single column. [#37195](https://github.com/ClickHouse/ClickHouse/pull/37195) ([Maksim Kita](https://github.com/kitaisreal)).
* Support multi disks for caching hive files. [#37279](https://github.com/ClickHouse/ClickHouse/pull/37279) ([lgbo](https://github.com/lgbo-ustc)).
* Improved performance on array norm and distance functions 2x-4x times. [#37394](https://github.com/ClickHouse/ClickHouse/pull/37394) ([Alexander Gololobov](https://github.com/davenger)).
* Improve performance of number comparison functions using dynamic dispatch. [#37399](https://github.com/ClickHouse/ClickHouse/pull/37399) ([Maksim Kita](https://github.com/kitaisreal)).
* Improve performance of ORDER BY with LIMIT. [#37481](https://github.com/ClickHouse/ClickHouse/pull/37481) ([Maksim Kita](https://github.com/kitaisreal)).
* Improve performance of `hasAll` function using dynamic dispatch infrastructure. [#37484](https://github.com/ClickHouse/ClickHouse/pull/37484) ([Maksim Kita](https://github.com/kitaisreal)).
* Improve performance of `greatCircleAngle`, `greatCircleDistance`, `geoDistance` functions. [#37524](https://github.com/ClickHouse/ClickHouse/pull/37524) ([Maksim Kita](https://github.com/kitaisreal)).
* Optimized the internal caching of re2 patterns which occur e.g. in LIKE and MATCH functions. [#37544](https://github.com/ClickHouse/ClickHouse/pull/37544) ([Robert Schulze](https://github.com/rschu1ze)).
* Improve filter bitmask generator function all in one with avx512 instructions. [#37588](https://github.com/ClickHouse/ClickHouse/pull/37588) ([yaqi-zhao](https://github.com/yaqi-zhao)).
* Improved performance of aggregation in case, when sparse columns (can be enabled by experimental setting `ratio_of_defaults_for_sparse_serialization` in `MergeTree` tables) are used as arguments in aggregate functions. [#37617](https://github.com/ClickHouse/ClickHouse/pull/37617) ([Anton Popov](https://github.com/CurtizJ)).
* Optimize function `COALESCE` with two arguments. [#37666](https://github.com/ClickHouse/ClickHouse/pull/37666) ([Anton Popov](https://github.com/CurtizJ)).
* Replace `multiIf` to `if` in case when `multiIf` has only one condition, because function `if` is more performant. [#37695](https://github.com/ClickHouse/ClickHouse/pull/37695) ([Anton Popov](https://github.com/CurtizJ)).
* Aggregates state destruction now may be posted on a thread pool. For queries with LIMIT and big state it provides significant speedup, e.g. `select uniq(number) from numbers_mt(1e7) group by number limit 100` became around 2.5x faster. [#37855](https://github.com/ClickHouse/ClickHouse/pull/37855) ([Nikita Taranov](https://github.com/nickitat)).
* Improve performance of single column sorting using sorting queue specializations. [#37990](https://github.com/ClickHouse/ClickHouse/pull/37990) ([Maksim Kita](https://github.com/kitaisreal)).
* Fix excessive CPU usage in background when there are a lot of tables. [#38028](https://github.com/ClickHouse/ClickHouse/pull/38028) ([Maksim Kita](https://github.com/kitaisreal)).
* Improve performance of `not` function using dynamic dispatch. [#38058](https://github.com/ClickHouse/ClickHouse/pull/38058) ([Maksim Kita](https://github.com/kitaisreal)).
* Added numerous NEON accelerated paths for main libraries. [#38093](https://github.com/ClickHouse/ClickHouse/pull/38093) ([Daniel Kutenin](https://github.com/danlark1)).

#### Improvement
* Add separate CLUSTER grant (and `access_control_improvements.on_cluster_queries_require_cluster_grant` configuration directive, for backward compatibility, default to `false`). [#35767](https://github.com/ClickHouse/ClickHouse/pull/35767) ([Azat Khuzhin](https://github.com/azat)).
* Add self extracting executable [#34755](https://github.com/ClickHouse/ClickHouse/issues/34755). [#35775](https://github.com/ClickHouse/ClickHouse/pull/35775) ([Filatenkov Artur](https://github.com/FArthur-cmd)).
* Added support for schema inference for `hdfsCluster`. [#35812](https://github.com/ClickHouse/ClickHouse/pull/35812) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Add feature disks( ls - list files on disk, C - set config file, list-disks - list disks names, disk - set disk for work by name, help - produce help message copy - copy data on disk containing at `from_path` to `to_path` link - Create hardlink on disk from `from_path` to `to_path` list - List files on disk move - Move file or directory on disk from `from_path` to `to_path` read - read File on disk `from_path` to `to_path` or to stdout remove - Remove file or directory on disk with all children. write - Write File on disk`from_path` or stdin to `to_path`). [#36060](https://github.com/ClickHouse/ClickHouse/pull/36060) ([Artyom Yurkov](https://github.com/Varinara)).
* Implement `least_used` load balancing algorithm for disks inside volume (multi disk configuration). [#36686](https://github.com/ClickHouse/ClickHouse/pull/36686) ([Azat Khuzhin](https://github.com/azat)).
* - Modify the HTTP Endpoint to return the full stats under the `X-ClickHouse-Summary` header when `send_progress_in_http_headers=0` (before it would return all zeros). - Modify the HTTP Endpoint to return `X-ClickHouse-Exception-Code` header when progress has been sent before (`send_progress_in_http_headers=1`) - Modify the HTTP Endpoint to return `HTTP_REQUEST_TIMEOUT` (408) instead of `HTTP_INTERNAL_SERVER_ERROR` (500) on `TIMEOUT_EXCEEDED` errors. [#36884](https://github.com/ClickHouse/ClickHouse/pull/36884) ([Raúl Marín](https://github.com/Algunenano)).
* Allow a user to inspect grants from granted roles. [#36941](https://github.com/ClickHouse/ClickHouse/pull/36941) ([nvartolomei](https://github.com/nvartolomei)).
* Do not calculate an integral numerically but use CDF functions instead. This will speed up execution and will increase the precision. This fixes [#36714](https://github.com/ClickHouse/ClickHouse/issues/36714). [#36953](https://github.com/ClickHouse/ClickHouse/pull/36953) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* Add default implementation for Nothing in functions. Now most of the functions will return column with type Nothing in case one of it's arguments is Nothing. It also solves problem with functions like arrayMap/arrayFilter and similar when they have empty array as an argument. Previously queries like `select arrayMap(x -> 2 * x, []);` failed because function inside lambda cannot work with type `Nothing`, now such queries return empty array with type `Array(Nothing)`. Also add support for arrays of nullable types in functions like arrayFilter/arrayFill. Previously, queries like `select arrayFilter(x -> x % 2, [1, NULL])` failed, now they work (if the result of lambda is NULL, then this value won't be included in the result). Closes [#37000](https://github.com/ClickHouse/ClickHouse/issues/37000). [#37048](https://github.com/ClickHouse/ClickHouse/pull/37048) ([Kruglov Pavel](https://github.com/Avogar)).
* Now if a shard has local replica we create a local plan and a plan to read from all remote replicas. They have shared initiator which coordinates reading. [#37204](https://github.com/ClickHouse/ClickHouse/pull/37204) ([Nikita Mikhaylov](https://github.com/nikitamikhaylov)).
* In function: CompressedWriteBuffer::nextImpl(), there is an unnecessary write-copy step that would happen frequently during inserting data. Below shows the differentiation with this patch: - Before: 1. Compress "working_buffer" into "compressed_buffer" 2. write-copy into "out" - After: Directly Compress "working_buffer" into "out". [#37242](https://github.com/ClickHouse/ClickHouse/pull/37242) ([jasperzhu](https://github.com/jinjunzh)).
* Support non-constant SQL functions (NOT) (I)LIKE and MATCH. [#37251](https://github.com/ClickHouse/ClickHouse/pull/37251) ([Robert Schulze](https://github.com/rschu1ze)).
* Client will try every IP address returned by DNS resolution until successful connection. [#37273](https://github.com/ClickHouse/ClickHouse/pull/37273) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* - Do no longer abort server startup if configuration option "mark_cache_size" is not explicitly set. [#37326](https://github.com/ClickHouse/ClickHouse/pull/37326) ([Robert Schulze](https://github.com/rschu1ze)).
* Allow to use String type instead of Binary in Arrow/Parquet/ORC formats. This PR introduces 3 new settings for it: `output_format_arrow_string_as_string`, `output_format_parquet_string_as_string`, `output_format_orc_string_as_string`. Default value for all settings is `false`. [#37327](https://github.com/ClickHouse/ClickHouse/pull/37327) ([Kruglov Pavel](https://github.com/Avogar)).
* Apply setting `input_format_max_rows_to_read_for_schema_inference` for all read rows in total from all files in globs. Previously setting `input_format_max_rows_to_read_for_schema_inference` was applied for each file in glob separately and in case of huge number of nulls we could read first `input_format_max_rows_to_read_for_schema_inference` rows from each file and get nothing. Also increase default value for this setting to 25000. [#37332](https://github.com/ClickHouse/ClickHouse/pull/37332) ([Kruglov Pavel](https://github.com/Avogar)).
* allows providing `NULL`/`NOT NULL` right after type in column declaration. [#37337](https://github.com/ClickHouse/ClickHouse/pull/37337) ([Igor Nikonov](https://github.com/devcrafter)).
* optimize file segment PARTIALLY_DOWNLOADED get read buffer. [#37338](https://github.com/ClickHouse/ClickHouse/pull/37338) ([chen](https://github.com/xiedeyantu)).
* Allow to prune the list of files via virtual columns such as `_file` and `_path` when reading from S3. This is for [#37174](https://github.com/ClickHouse/ClickHouse/issues/37174) , [#23494](https://github.com/ClickHouse/ClickHouse/issues/23494). [#37356](https://github.com/ClickHouse/ClickHouse/pull/37356) ([Amos Bird](https://github.com/amosbird)).
* Try to improve short circuit functions processing to fix problems with stress tests. [#37384](https://github.com/ClickHouse/ClickHouse/pull/37384) ([Kruglov Pavel](https://github.com/Avogar)).
* Closes [#37395](https://github.com/ClickHouse/ClickHouse/issues/37395). [#37415](https://github.com/ClickHouse/ClickHouse/pull/37415) ([Memo](https://github.com/Joeywzr)).
* Fix extremely rare deadlock during part fetch in zero-copy replication. Fixes [#37423](https://github.com/ClickHouse/ClickHouse/issues/37423). [#37424](https://github.com/ClickHouse/ClickHouse/pull/37424) ([metahys](https://github.com/metahys)).
* Don't allow to create storage with unknown data format. [#37450](https://github.com/ClickHouse/ClickHouse/pull/37450) ([Kruglov Pavel](https://github.com/Avogar)).
* Set `global_memory_usage_overcommit_max_wait_microseconds` default value to 5 seconds. Add info about `OvercommitTracker` to OOM exception message. Add `MemoryOvercommitWaitTimeMicroseconds` profile event. [#37460](https://github.com/ClickHouse/ClickHouse/pull/37460) ([Dmitry Novik](https://github.com/novikd)).
* Play UI: Keep controls in place when the page is scrolled horizontally. This makes edits comfortable even if the table is wide and it was scrolled far to the right. The feature proposed by Maksym Tereshchenko from CaspianDB. [#37470](https://github.com/ClickHouse/ClickHouse/pull/37470) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Now more filters can be pushed down for join. [#37472](https://github.com/ClickHouse/ClickHouse/pull/37472) ([Amos Bird](https://github.com/amosbird)).
* Modify query div in play.html to be extendable beyond 20% height. In case of very long queries it is helpful to extend the textarea element, only today, since the div is fixed height, the extended textarea hides the data div underneath. With this fix, extending the textarea element will push the data div down/up such the extended textarea won't hide it. Also, keeps query box width 100% even when the user adjusting the size of the query textarea. [#37488](https://github.com/ClickHouse/ClickHouse/pull/37488) ([guyco87](https://github.com/guyco87)).
* Modify query div in play.html to be extendable beyond 20% height. In case of very long queries it is helpful to extend the textarea element, only today, since the div is fixed height, the extended textarea hides the data div underneath. With this fix, extending the textarea element will push the data div down/up such the extended textarea won't hide it. Also, keeps query box width 100% even when the user adjusting the size of the query textarea. [#37504](https://github.com/ClickHouse/ClickHouse/pull/37504) ([guyco87](https://github.com/guyco87)).
* Currently clickhouse directly downloads all remote files to the local cache (even if they are only read once), which will frequently cause IO of the local hard disk. In some scenarios, these IOs may not be necessary and may easily cause negative optimization. As shown in the figure below, when we run SSB Q1-Q4, the performance of the cache has caused negative optimization. [#37516](https://github.com/ClickHouse/ClickHouse/pull/37516) ([Han Shukai](https://github.com/KinderRiven)).
* Added `ProfileEvents` for introspection of type of written (inserted or merged) parts (`Inserted{Wide/Compact/InMemory}Parts`, `MergedInto{Wide/Compact/InMemory}Parts`. Added column `part_type` to `system.part_log`. Resolves [#37495](https://github.com/ClickHouse/ClickHouse/issues/37495). [#37536](https://github.com/ClickHouse/ClickHouse/pull/37536) ([Anton Popov](https://github.com/CurtizJ)).
* clickhouse-keeper improvement: move broken logs to a timestamped folder. [#37565](https://github.com/ClickHouse/ClickHouse/pull/37565) ([Antonio Andelic](https://github.com/antonio2368)).
* Do not write expired columns by TTL after subsequent merges (before only first merge/optimize of the part will not write expired by TTL columns, all other will do). [#37570](https://github.com/ClickHouse/ClickHouse/pull/37570) ([Azat Khuzhin](https://github.com/azat)).
* More precise result of the `dumpColumnStructure` miscellaneous function in presence of LowCardinality or Sparse columns. In previous versions, these functions were converting the argument to a full column before returning the result. This is needed to provide an answer in [#6935](https://github.com/ClickHouse/ClickHouse/issues/6935). [#37633](https://github.com/ClickHouse/ClickHouse/pull/37633) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* keeper: store only unique session IDs for watches. [#37641](https://github.com/ClickHouse/ClickHouse/pull/37641) ([Azat Khuzhin](https://github.com/azat)).
* Fix possible "Cannot write to finalized buffer". [#37645](https://github.com/ClickHouse/ClickHouse/pull/37645) ([Azat Khuzhin](https://github.com/azat)).
* Add setting `support_batch_delete` for `DiskS3` to disable multiobject delete calls, which Google Cloud Storage doesn't support. [#37659](https://github.com/ClickHouse/ClickHouse/pull/37659) ([Fred Wulff](https://github.com/frew)).
* Support types with non-standard defaults in ROLLUP, CUBE, GROUPING SETS. Closes [#37360](https://github.com/ClickHouse/ClickHouse/issues/37360). [#37667](https://github.com/ClickHouse/ClickHouse/pull/37667) ([Dmitry Novik](https://github.com/novikd)).
* Add an option to disable connection pooling in ODBC bridge. [#37705](https://github.com/ClickHouse/ClickHouse/pull/37705) ([Anton Kozlov](https://github.com/tonickkozlov)).
* ... LIKE patterns with trailing escape symbol ('\\') are now disallowed (as mandated by the SQL standard). [#37764](https://github.com/ClickHouse/ClickHouse/pull/37764) ([Robert Schulze](https://github.com/rschu1ze)).
* Fix stacktraces collection on ARM. Closes [#37044](https://github.com/ClickHouse/ClickHouse/issues/37044). Closes [#15638](https://github.com/ClickHouse/ClickHouse/issues/15638). [#37797](https://github.com/ClickHouse/ClickHouse/pull/37797) ([Maksim Kita](https://github.com/kitaisreal)).
* Functions `dictGetHierarchy`, `dictIsIn`, `dictGetChildren`, `dictGetDescendants` added support nullable `HIERARCHICAL` attribute in dictionaries. Closes [#35521](https://github.com/ClickHouse/ClickHouse/issues/35521). [#37805](https://github.com/ClickHouse/ClickHouse/pull/37805) ([Maksim Kita](https://github.com/kitaisreal)).
* Expose BoringSSL version related info in the `system.build_options` table. [#37850](https://github.com/ClickHouse/ClickHouse/pull/37850) ([Bharat Nallan](https://github.com/bharatnc)).
* **Description** Limiting the maximum cache usage per query can effectively prevent cache pool contamination. [Related Issues](https://github.com/ClickHouse/ClickHouse/issues/28961). [#37859](https://github.com/ClickHouse/ClickHouse/pull/37859) ([Han Shukai](https://github.com/KinderRiven)).
* Now clickhouse-server removes `delete_tmp` directories on server start. Fixes [#26503](https://github.com/ClickHouse/ClickHouse/issues/26503). [#37906](https://github.com/ClickHouse/ClickHouse/pull/37906) ([alesapin](https://github.com/alesapin)).
* Clean up broken detached parts after timeout. Closes [#25195](https://github.com/ClickHouse/ClickHouse/issues/25195). [#37975](https://github.com/ClickHouse/ClickHouse/pull/37975) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Now in MergeTree table engines family failed-to-move parts will be removed instantly. [#37994](https://github.com/ClickHouse/ClickHouse/pull/37994) ([alesapin](https://github.com/alesapin)).
* Now if setting `always_fetch_merged_part` is enabled for ReplicatedMergeTree merges will try to find parts on other replicas rarely with smaller load for [Zoo]Keeper. [#37995](https://github.com/ClickHouse/ClickHouse/pull/37995) ([alesapin](https://github.com/alesapin)).
* Add implicit grants with grant option too. For example `GRANT CREATE TABLE ON test.* TO A WITH GRANT OPTION` now allows `A` to execute `GRANT CREATE VIEW ON test.* TO B`. [#38017](https://github.com/ClickHouse/ClickHouse/pull/38017) ([Vitaly Baranov](https://github.com/vitlibar)).
* Do not display `-0.0` CPU time in clickhouse-client. It can appear due to rounding errors. This closes [#38003](https://github.com/ClickHouse/ClickHouse/issues/38003). This closes [#38038](https://github.com/ClickHouse/ClickHouse/issues/38038). [#38064](https://github.com/ClickHouse/ClickHouse/pull/38064) ([Alexey Milovidov](https://github.com/alexey-milovidov)).

#### Build/Testing/Packaging Improvement
* Use clang-14 and LLVM infrastructure version 14 for builds. This closes [#34681](https://github.com/ClickHouse/ClickHouse/issues/34681). [#34754](https://github.com/ClickHouse/ClickHouse/pull/34754) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Allow to drop privileges at startup. This simplifies Docker images. Closes [#36293](https://github.com/ClickHouse/ClickHouse/issues/36293). [#36341](https://github.com/ClickHouse/ClickHouse/pull/36341) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Remove recursive submodules, because we don't need them and they can be confusing. Add style check to prevent recursive submodules. This closes [#32821](https://github.com/ClickHouse/ClickHouse/issues/32821). [#37616](https://github.com/ClickHouse/ClickHouse/pull/37616) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Add docs spellcheck to CI. [#37790](https://github.com/ClickHouse/ClickHouse/pull/37790) ([Vladimir C](https://github.com/vdimir)).
* Fix overly aggressive stripping which removed the embedded hash required for checking the consistency of the executable. [#37993](https://github.com/ClickHouse/ClickHouse/pull/37993) ([Robert Schulze](https://github.com/rschu1ze)).
* fix MacOS build compressor faild. [#38007](https://github.com/ClickHouse/ClickHouse/pull/38007) ([chen](https://github.com/xiedeyantu)).

#### Bug Fix (user-visible misbehavior in official stable release)

* Fix `GROUP BY` `AggregateFunction` (i.e. you `GROUP BY` by the column that has `AggregateFunction` type). [#37093](https://github.com/ClickHouse/ClickHouse/pull/37093) ([Azat Khuzhin](https://github.com/azat)).
* Fix possible heap-use-after-free error when reading system.projection_parts and system.projection_parts_columns . This fixes [#37184](https://github.com/ClickHouse/ClickHouse/issues/37184). [#37185](https://github.com/ClickHouse/ClickHouse/pull/37185) ([Amos Bird](https://github.com/amosbird)).
* Fix `addDependency` in WindowView. This bug can be reproduced like [#37237](https://github.com/ClickHouse/ClickHouse/issues/37237). [#37224](https://github.com/ClickHouse/ClickHouse/pull/37224) ([vxider](https://github.com/Vxider)).
* This PR moving `addDependency` from constructor to `startup()` to avoid adding dependency to a **dropped** table, fix [#37237](https://github.com/ClickHouse/ClickHouse/issues/37237). [#37243](https://github.com/ClickHouse/ClickHouse/pull/37243) ([vxider](https://github.com/Vxider)).
* Fix inserting defaults for missing values in columnar formats. Previously missing columns were filled with defaults for types, not for columns. [#37253](https://github.com/ClickHouse/ClickHouse/pull/37253) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix some cases of insertion nested arrays to columns of type `Object`. [#37305](https://github.com/ClickHouse/ClickHouse/pull/37305) ([Anton Popov](https://github.com/CurtizJ)).
* Fix unexpected errors with a clash of constant strings in aggregate function, prewhere and join. Close [#36891](https://github.com/ClickHouse/ClickHouse/issues/36891). [#37336](https://github.com/ClickHouse/ClickHouse/pull/37336) ([Vladimir C](https://github.com/vdimir)).
* Fix projections with GROUP/ORDER BY in query and optimize_aggregation_in_order (before the result was incorrect since only finish sorting was performed). [#37342](https://github.com/ClickHouse/ClickHouse/pull/37342) ([Azat Khuzhin](https://github.com/azat)).
* Fixed error with symbols in key name in S3. Fixes [#33009](https://github.com/ClickHouse/ClickHouse/issues/33009). [#37344](https://github.com/ClickHouse/ClickHouse/pull/37344) ([Vladimir Chebotarev](https://github.com/excitoon)).
* Throw an exception when GROUPING SETS used with ROLLUP or CUBE. [#37367](https://github.com/ClickHouse/ClickHouse/pull/37367) ([Dmitry Novik](https://github.com/novikd)).
* Fix LOGICAL_ERROR in getMaxSourcePartsSizeForMerge during merges (in case of non standard, greater, values of `background_pool_size`/`background_merges_mutations_concurrency_ratio` has been specified in `config.xml` (new way) not in `users.xml` (deprecated way)). [#37413](https://github.com/ClickHouse/ClickHouse/pull/37413) ([Azat Khuzhin](https://github.com/azat)).
* ``` Stop removing UTF-8 BOM in RowBinary format. [#37428](https://github.com/ClickHouse/ClickHouse/pull/37428) ([Paul Loyd](https://github.com/loyd)). ```. [#37428](https://github.com/ClickHouse/ClickHouse/pull/37428) ([Paul Loyd](https://github.com/loyd)).
* clickhouse-keeper bugfix: fix force recovery for single node cluster. [#37440](https://github.com/ClickHouse/ClickHouse/pull/37440) ([Antonio Andelic](https://github.com/antonio2368)).
* Fix logical error in normalizeUTF8 functions. Closes [#37298](https://github.com/ClickHouse/ClickHouse/issues/37298). [#37443](https://github.com/ClickHouse/ClickHouse/pull/37443) ([Maksim Kita](https://github.com/kitaisreal)).
* * Fix cast lowcard of nullable in JoinSwitcher, close [#37385](https://github.com/ClickHouse/ClickHouse/issues/37385). [#37453](https://github.com/ClickHouse/ClickHouse/pull/37453) ([Vladimir C](https://github.com/vdimir)).
* Fix named tuples output in ORC/Arrow/Parquet formats. [#37458](https://github.com/ClickHouse/ClickHouse/pull/37458) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix optimization of monotonous functions in ORDER BY clause in presence of GROUPING SETS. Fixes [#37401](https://github.com/ClickHouse/ClickHouse/issues/37401). [#37493](https://github.com/ClickHouse/ClickHouse/pull/37493) ([Dmitry Novik](https://github.com/novikd)).
* Fix error on joining with dictionary on some conditions. Close [#37386](https://github.com/ClickHouse/ClickHouse/issues/37386). [#37530](https://github.com/ClickHouse/ClickHouse/pull/37530) ([Vladimir C](https://github.com/vdimir)).
* Prohibit `optimize_aggregation_in_order` with `GROUPING SETS` (fixes `LOGICAL_ERROR`). [#37542](https://github.com/ClickHouse/ClickHouse/pull/37542) ([Azat Khuzhin](https://github.com/azat)).
* Fix wrong dump information of ActionsDAG. [#37587](https://github.com/ClickHouse/ClickHouse/pull/37587) ([zhanglistar](https://github.com/zhanglistar)).
* Fix converting types for UNION queries (may produce LOGICAL_ERROR). [#37593](https://github.com/ClickHouse/ClickHouse/pull/37593) ([Azat Khuzhin](https://github.com/azat)).
* Fix `WITH FILL` modifier with negative intervals in `STEP` clause. Fixes [#37514](https://github.com/ClickHouse/ClickHouse/issues/37514). [#37600](https://github.com/ClickHouse/ClickHouse/pull/37600) ([Anton Popov](https://github.com/CurtizJ)).
* Fix illegal joinGet array usage when ` join_use_nulls = 1`. This fixes [#37562](https://github.com/ClickHouse/ClickHouse/issues/37562) . [#37650](https://github.com/ClickHouse/ClickHouse/pull/37650) ([Amos Bird](https://github.com/amosbird)).
* Fix columns number mismatch in cross join, close [#37561](https://github.com/ClickHouse/ClickHouse/issues/37561). [#37653](https://github.com/ClickHouse/ClickHouse/pull/37653) ([Vladimir C](https://github.com/vdimir)).
* Fix segmentation fault in `show create table` from mysql database when it is configured with named collections. Closes [#37683](https://github.com/ClickHouse/ClickHouse/issues/37683). [#37690](https://github.com/ClickHouse/ClickHouse/pull/37690) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fix RabbitMQ Storage not being able to startup on server restart if storage was create without SETTINGS clause. Closes [#37463](https://github.com/ClickHouse/ClickHouse/issues/37463). [#37691](https://github.com/ClickHouse/ClickHouse/pull/37691) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fixed DateTime64 fractional seconds behavior prior to Unix epoch. [#37697](https://github.com/ClickHouse/ClickHouse/pull/37697) ([Andrey Zvonov](https://github.com/zvonand)).
* SQL user defined functions disable CREATE/DROP in readonly mode. Closes [#37280](https://github.com/ClickHouse/ClickHouse/issues/37280). [#37699](https://github.com/ClickHouse/ClickHouse/pull/37699) ([Maksim Kita](https://github.com/kitaisreal)).
* Fix formatting of Nullable arguments for executable user defined functions. Closes [#35897](https://github.com/ClickHouse/ClickHouse/issues/35897). [#37711](https://github.com/ClickHouse/ClickHouse/pull/37711) ([Maksim Kita](https://github.com/kitaisreal)).
* Fix optimization enabled by setting `optimize_monotonous_functions_in_order_by` in distributed queries. Fixes [#36037](https://github.com/ClickHouse/ClickHouse/issues/36037). [#37724](https://github.com/ClickHouse/ClickHouse/pull/37724) ([Anton Popov](https://github.com/CurtizJ)).
* Fix `SELECT ... INTERSECT` and `EXCEPT SELECT` statements with constant string types. [#37738](https://github.com/ClickHouse/ClickHouse/pull/37738) ([Antonio Andelic](https://github.com/antonio2368)).
* Fix crash of FunctionHashID, closes [#37735](https://github.com/ClickHouse/ClickHouse/issues/37735). [#37742](https://github.com/ClickHouse/ClickHouse/pull/37742) ([flynn](https://github.com/ucasfl)).
* Fix possible logical error: `Invalid Field get from type UInt64 to type Float64` in `values` table function. Closes [#37602](https://github.com/ClickHouse/ClickHouse/issues/37602). [#37754](https://github.com/ClickHouse/ClickHouse/pull/37754) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix possible segfault in schema inference in case of exception in SchemaReader constructor. Closes [#37680](https://github.com/ClickHouse/ClickHouse/issues/37680). [#37760](https://github.com/ClickHouse/ClickHouse/pull/37760) ([Kruglov Pavel](https://github.com/Avogar)).
* Fix setting cast_ipv4_ipv6_default_on_conversion_error for internal cast function. Closes [#35156](https://github.com/ClickHouse/ClickHouse/issues/35156). [#37761](https://github.com/ClickHouse/ClickHouse/pull/37761) ([Maksim Kita](https://github.com/kitaisreal)).
* Octal literals are not supported. [#37765](https://github.com/ClickHouse/ClickHouse/pull/37765) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* fix toString error on DatatypeDate32. [#37775](https://github.com/ClickHouse/ClickHouse/pull/37775) ([LiuNeng](https://github.com/liuneng1994)).
* The clickhouse-keeper setting `dead_session_check_period_ms` was transformed into microseconds (multiplied by 1000), which lead to dead sessions only being cleaned up after several minutes (instead of 500ms). [#37824](https://github.com/ClickHouse/ClickHouse/pull/37824) ([Michael Lex](https://github.com/mlex)).
* Fix possible "No more packets are available" for distributed queries (in case of `async_socket_for_remote`/`use_hedged_requests` is disabled). [#37826](https://github.com/ClickHouse/ClickHouse/pull/37826) ([Azat Khuzhin](https://github.com/azat)).
* Do not drop the inner target table when executing `ALTER TABLE ... MODIFY QUERY` in WindowView. [#37879](https://github.com/ClickHouse/ClickHouse/pull/37879) ([vxider](https://github.com/Vxider)).
* Fix directory ownership of coordination dir in clickhouse-keeper Docker image. Fixes [#37914](https://github.com/ClickHouse/ClickHouse/issues/37914). [#37915](https://github.com/ClickHouse/ClickHouse/pull/37915) ([James Maidment](https://github.com/jamesmaidment)).
* Dictionaries fix custom query with update field and `{condition}`. Closes [#33746](https://github.com/ClickHouse/ClickHouse/issues/33746). [#37947](https://github.com/ClickHouse/ClickHouse/pull/37947) ([Maksim Kita](https://github.com/kitaisreal)).
* Fix possible incorrect result of `SELECT ... WITH FILL` in the case when `ORDER BY` should be applied after `WITH FILL` result (e.g. for outer query). Incorrect result was caused by optimization for `ORDER BY` expressions ([#35623](https://github.com/ClickHouse/ClickHouse/issues/35623)). Closes [#37904](https://github.com/ClickHouse/ClickHouse/issues/37904). [#37959](https://github.com/ClickHouse/ClickHouse/pull/37959) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Add missing default columns when pushing to the target table in WindowView, fix [#37815](https://github.com/ClickHouse/ClickHouse/issues/37815). [#37965](https://github.com/ClickHouse/ClickHouse/pull/37965) ([vxider](https://github.com/Vxider)).
* Fixed a stack overflow issue that would cause compilation to fail. [#37996](https://github.com/ClickHouse/ClickHouse/pull/37996) ([Han Shukai](https://github.com/KinderRiven)).
* when open enable_filesystem_query_cache_limit, throw Reserved cache size exceeds the remaining cache size. [#38004](https://github.com/ClickHouse/ClickHouse/pull/38004) ([chen](https://github.com/xiedeyantu)).
* Query, containing ORDER BY ... WITH FILL, can generate extra rows when multiple WITH FILL columns are present. [#38074](https://github.com/ClickHouse/ClickHouse/pull/38074) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).

#### Bug Fix (user-visible misbehaviour in official stable release)

* Fix converting types for UNION queries (may produce LOGICAL_ERROR). [#34775](https://github.com/ClickHouse/ClickHouse/pull/34775) ([Azat Khuzhin](https://github.com/azat)).
* TTL merge may not be scheduled again if BackgroundExecutor is busy. --merges_with_ttl_counter is increased in selectPartsToMerge() --merge task will be ignored if BackgroundExecutor is busy --merges_with_ttl_counter will not be decrease. [#36387](https://github.com/ClickHouse/ClickHouse/pull/36387) ([lthaooo](https://github.com/lthaooo)).
* Fix overrided settings value of `normalize_function_names`. [#36937](https://github.com/ClickHouse/ClickHouse/pull/36937) ([李扬](https://github.com/taiyang-li)).
* Fix for exponential time decaying window functions. Now respecting boundaries of the window. [#36944](https://github.com/ClickHouse/ClickHouse/pull/36944) ([Vladimir Chebotarev](https://github.com/excitoon)).
* Fix bug datetime64 parse from string '1969-12-31 23:59:59.123'. Close [#36994](https://github.com/ClickHouse/ClickHouse/issues/36994). [#37039](https://github.com/ClickHouse/ClickHouse/pull/37039) ([李扬](https://github.com/taiyang-li)).

#### NO CL ENTRY

* NO CL ENTRY:  'Revert "Fix mutations in tables with columns of type `Object`"'. [#37355](https://github.com/ClickHouse/ClickHouse/pull/37355) ([Alexander Tokmakov](https://github.com/tavplubix)).
* NO CL ENTRY:  'Revert "Remove height restrictions from the query div in play web tool, and m..."'. [#37501](https://github.com/ClickHouse/ClickHouse/pull/37501) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* NO CL ENTRY:  'Revert "Add support for preprocessing ZooKeeper operations in `clickhouse-keeper`"'. [#37534](https://github.com/ClickHouse/ClickHouse/pull/37534) ([Antonio Andelic](https://github.com/antonio2368)).
* NO CL ENTRY:  'Revert "(only with zero-copy replication, non-production experimental feature not recommended to use) fix possible deadlock during fetching part"'. [#37545](https://github.com/ClickHouse/ClickHouse/pull/37545) ([Alexander Tokmakov](https://github.com/tavplubix)).
* NO CL ENTRY:  'Revert "RFC: Fix converting types for UNION queries (may produce LOGICAL_ERROR)"'. [#37582](https://github.com/ClickHouse/ClickHouse/pull/37582) ([Dmitry Novik](https://github.com/novikd)).
* NO CL ENTRY:  'Revert "Revert "(only with zero-copy replication, non-production experimental feature not recommended to use) fix possible deadlock during fetching part""'. [#37598](https://github.com/ClickHouse/ClickHouse/pull/37598) ([alesapin](https://github.com/alesapin)).
* NO CL ENTRY:  'Revert "Implemented changing comment to a ReplicatedMergeTree table"'. [#37627](https://github.com/ClickHouse/ClickHouse/pull/37627) ([Alexander Tokmakov](https://github.com/tavplubix)).
* NO CL ENTRY:  'Revert "Remove resursive submodules"'. [#37774](https://github.com/ClickHouse/ClickHouse/pull/37774) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* NO CL ENTRY:  'Revert "Fix possible segfault in schema inference"'. [#37785](https://github.com/ClickHouse/ClickHouse/pull/37785) ([Alexander Tokmakov](https://github.com/tavplubix)).
* NO CL ENTRY:  'Revert "Revert "Fix possible segfault in schema inference""'. [#37787](https://github.com/ClickHouse/ClickHouse/pull/37787) ([Kruglov Pavel](https://github.com/Avogar)).
* NO CL ENTRY:  'Add more Rust client libraries to documentation'. [#37880](https://github.com/ClickHouse/ClickHouse/pull/37880) ([Paul Loyd](https://github.com/loyd)).
* NO CL ENTRY:  'Revert "Fix errors of CheckTriviallyCopyableMove type"'. [#37902](https://github.com/ClickHouse/ClickHouse/pull/37902) ([Anton Popov](https://github.com/CurtizJ)).
* NO CL ENTRY:  'Revert "Don't try to kill empty list of containers in `integration/runner`"'. [#38001](https://github.com/ClickHouse/ClickHouse/pull/38001) ([Alexander Tokmakov](https://github.com/tavplubix)).
* NO CL ENTRY:  'Revert "add d3js based trace visualizer as gantt chart"'. [#38043](https://github.com/ClickHouse/ClickHouse/pull/38043) ([Alexander Tokmakov](https://github.com/tavplubix)).
* NO CL ENTRY:  'Revert "Add backoff to merges in replicated queue if `always_fetch_merged_part` is enabled"'. [#38082](https://github.com/ClickHouse/ClickHouse/pull/38082) ([Alexander Tokmakov](https://github.com/tavplubix)).
* NO CL ENTRY:  'Revert "More parallel execution for queries with `FINAL`"'. [#38094](https://github.com/ClickHouse/ClickHouse/pull/38094) ([Alexander Tokmakov](https://github.com/tavplubix)).
* NO CL ENTRY:  'Revert "Revert "add d3js based trace visualizer as gantt chart""'. [#38129](https://github.com/ClickHouse/ClickHouse/pull/38129) ([Alexey Milovidov](https://github.com/alexey-milovidov)).

#### NOT FOR CHANGELOG / INSIGNIFICANT

* Fix collision of S3 operation log revision [#34915](https://github.com/ClickHouse/ClickHouse/pull/34915) ([ianton-ru](https://github.com/ianton-ru)).
* Change timezone in stateless tests [#35231](https://github.com/ClickHouse/ClickHouse/pull/35231) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Remove useless code in ReplicatedMergeTreeRestartingThread [#36113](https://github.com/ClickHouse/ClickHouse/pull/36113) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Merge release and performance builds [#36538](https://github.com/ClickHouse/ClickHouse/pull/36538) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Add support for preprocessing ZooKeeper operations in `clickhouse-keeper` [#37036](https://github.com/ClickHouse/ClickHouse/pull/37036) ([Antonio Andelic](https://github.com/antonio2368)).
* Fix jemalloc compatibility with LLVM libunwind [#37078](https://github.com/ClickHouse/ClickHouse/pull/37078) ([Yuriy Chernyshov](https://github.com/georgthegreat)).
* Separate object storage operations from disks [#37139](https://github.com/ClickHouse/ClickHouse/pull/37139) ([alesapin](https://github.com/alesapin)).
* Add test for WATCH LIMIT query in WindowView [#37219](https://github.com/ClickHouse/ClickHouse/pull/37219) ([vxider](https://github.com/Vxider)).
* Improve changelog script [#37249](https://github.com/ClickHouse/ClickHouse/pull/37249) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Rework AccessControl's notifications. [#37269](https://github.com/ClickHouse/ClickHouse/pull/37269) ([Vitaly Baranov](https://github.com/vitlibar)).
* Add ability to pass QueryKind via clickhouse-client/local (useful for debugging) [#37290](https://github.com/ClickHouse/ClickHouse/pull/37290) ([Azat Khuzhin](https://github.com/azat)).
* Various cmake cleanups [#37300](https://github.com/ClickHouse/ClickHouse/pull/37300) ([Robert Schulze](https://github.com/rschu1ze)).
* Try to fix some trash [#37303](https://github.com/ClickHouse/ClickHouse/pull/37303) ([Alexander Tokmakov](https://github.com/tavplubix)).
* [bug-fix] root_dir is not set in copyThroughBuffers [#37319](https://github.com/ClickHouse/ClickHouse/pull/37319) ([lingo-xp](https://github.com/lingo-xp)).
* Speed up test 00157_cache_dictionary [#37320](https://github.com/ClickHouse/ClickHouse/pull/37320) ([Kruglov Pavel](https://github.com/Avogar)).
* Add changelog for 22.5 [#37339](https://github.com/ClickHouse/ClickHouse/pull/37339) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Update version after release [#37346](https://github.com/ClickHouse/ClickHouse/pull/37346) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Disable clang-tidy readability-identifier-length [#37347](https://github.com/ClickHouse/ClickHouse/pull/37347) ([Robert Schulze](https://github.com/rschu1ze)).
* Tags and release [#37348](https://github.com/ClickHouse/ClickHouse/pull/37348) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* New stable tag [#37349](https://github.com/ClickHouse/ClickHouse/pull/37349) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Fixes for eigen library build [#37369](https://github.com/ClickHouse/ClickHouse/pull/37369) ([Alexander Gololobov](https://github.com/davenger)).
* Do not fail CI if events clickhouse is down [#37371](https://github.com/ClickHouse/ClickHouse/pull/37371) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Ugly hacks for performance.tgz artifacts [#37373](https://github.com/ClickHouse/ClickHouse/pull/37373) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Fix docker server images building [#37374](https://github.com/ClickHouse/ClickHouse/pull/37374) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Fix debug symbols in packages [#37379](https://github.com/ClickHouse/ClickHouse/pull/37379) ([Azat Khuzhin](https://github.com/azat)).
* Enable -Wc++98-compat-extra-semi [#37382](https://github.com/ClickHouse/ClickHouse/pull/37382) ([Robert Schulze](https://github.com/rschu1ze)).
* Mark all `operator bool()` as explicit [#37388](https://github.com/ClickHouse/ClickHouse/pull/37388) ([Anton Popov](https://github.com/CurtizJ)).
* Fixes for transactions [#37398](https://github.com/ClickHouse/ClickHouse/pull/37398) ([Alexander Tokmakov](https://github.com/tavplubix)).
* WindowTransform::moveRowNumber fix [#37400](https://github.com/ClickHouse/ClickHouse/pull/37400) ([Nikolay](https://github.com/ndchikin)).
* Fix possible memory leaks in system.certificates implementation [#37407](https://github.com/ClickHouse/ClickHouse/pull/37407) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Remove unused code in WindowView [#37426](https://github.com/ClickHouse/ClickHouse/pull/37426) ([vxider](https://github.com/Vxider)).
* Add join_algorithm='parallel_hash' to stress test [#37436](https://github.com/ClickHouse/ClickHouse/pull/37436) ([Vladimir C](https://github.com/vdimir)).
* Perf test for Norm and Distance functions for arrays and tuples [#37437](https://github.com/ClickHouse/ClickHouse/pull/37437) ([Alexander Gololobov](https://github.com/davenger)).
* FunctionBinaryRepresentation style fixes [#37438](https://github.com/ClickHouse/ClickHouse/pull/37438) ([Maksim Kita](https://github.com/kitaisreal)).
* CompressedWriteBuffer added comment [#37442](https://github.com/ClickHouse/ClickHouse/pull/37442) ([Maksim Kita](https://github.com/kitaisreal)).
* BinaryFunctionVectorized remove macro [#37447](https://github.com/ClickHouse/ClickHouse/pull/37447) ([Maksim Kita](https://github.com/kitaisreal)).
* RangeHashedDictionary added test [#37449](https://github.com/ClickHouse/ClickHouse/pull/37449) ([Maksim Kita](https://github.com/kitaisreal)).
* Fix failed thread stateless tests [#37455](https://github.com/ClickHouse/ClickHouse/pull/37455) ([Kruglov Pavel](https://github.com/Avogar)).
* tests: fix table in 01710_projection_aggregation_in_order [#37468](https://github.com/ClickHouse/ClickHouse/pull/37468) ([Azat Khuzhin](https://github.com/azat)).
* Sync workflows paths for PR and DocsCheck [#37477](https://github.com/ClickHouse/ClickHouse/pull/37477) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Set compiler flags for ASM same as for C/CXX [#37479](https://github.com/ClickHouse/ClickHouse/pull/37479) ([Azat Khuzhin](https://github.com/azat)).
* Dynamic dispatch infrastructure style fixes [#37480](https://github.com/ClickHouse/ClickHouse/pull/37480) ([Maksim Kita](https://github.com/kitaisreal)).
* Refactorings of LIKE/MATCH code [#37491](https://github.com/ClickHouse/ClickHouse/pull/37491) ([Robert Schulze](https://github.com/rschu1ze)).
* Improve changelog.py script, get changelogs for 2021 [#37496](https://github.com/ClickHouse/ClickHouse/pull/37496) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Try to run aarch64 performance tests [#37497](https://github.com/ClickHouse/ClickHouse/pull/37497) ([Ilya Yatsishin](https://github.com/qoega)).
* Renamed arrayXXNorm/arrayXXDistance functions to XXNorm/XXDistance and fixed some overflow cases [#37502](https://github.com/ClickHouse/ClickHouse/pull/37502) ([Alexander Gololobov](https://github.com/davenger)).
* Bump cctz to 2022-05-15 [#37518](https://github.com/ClickHouse/ClickHouse/pull/37518) ([Robert Schulze](https://github.com/rschu1ze)).
* Add failed builds to the build report [#37527](https://github.com/ClickHouse/ClickHouse/pull/37527) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Functions normalizeUTF8 unstable performance tests fix [#37528](https://github.com/ClickHouse/ClickHouse/pull/37528) ([Maksim Kita](https://github.com/kitaisreal)).
* Use a separate mutex for query_factories_info in Context. [#37532](https://github.com/ClickHouse/ClickHouse/pull/37532) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Update SECURITY.md [#37541](https://github.com/ClickHouse/ClickHouse/pull/37541) ([Ivan Blinkov](https://github.com/blinkov)).
* Refactor read metrics and callbacks [#37543](https://github.com/ClickHouse/ClickHouse/pull/37543) ([Nikolai Kochetov](https://github.com/KochetovNicolai)).
* Follow-up to [#37398](https://github.com/ClickHouse/ClickHouse/issues/37398) [#37547](https://github.com/ClickHouse/ClickHouse/pull/37547) ([Alexander Tokmakov](https://github.com/tavplubix)).
* fix root CMakeLists.txt search [#37552](https://github.com/ClickHouse/ClickHouse/pull/37552) ([Sergei Trifonov](https://github.com/serxa)).
* Cleanup StorageHDFS (unused variables prevent build with clang 12) [#37554](https://github.com/ClickHouse/ClickHouse/pull/37554) ([Michail Safronov](https://github.com/msaf1980)).
* Fix failed assertion in cache [#37566](https://github.com/ClickHouse/ClickHouse/pull/37566) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Slightly better jepsen tests [#37568](https://github.com/ClickHouse/ClickHouse/pull/37568) ([alesapin](https://github.com/alesapin)).
* Remove unused MergeTreeDataMergerMutator::chooseMergeAlgorithm() [#37574](https://github.com/ClickHouse/ClickHouse/pull/37574) ([Azat Khuzhin](https://github.com/azat)).
* Remove useless files [#37579](https://github.com/ClickHouse/ClickHouse/pull/37579) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Flush and shutdown temporary table before drop [#37585](https://github.com/ClickHouse/ClickHouse/pull/37585) ([vxider](https://github.com/Vxider)).
* tests: fix 01317_no_password_in_command_line flakiness (and make it race free) [#37597](https://github.com/ClickHouse/ClickHouse/pull/37597) ([Azat Khuzhin](https://github.com/azat)).
* Use Jepsen worklow directly in PR workflow [#37599](https://github.com/ClickHouse/ClickHouse/pull/37599) ([Antonio Andelic](https://github.com/antonio2368)).
* Added LpNorm and LpDistance functions for arrays [#37601](https://github.com/ClickHouse/ClickHouse/pull/37601) ([Alexander Gololobov](https://github.com/davenger)).
* Turn on s3 tests to red mode [#37604](https://github.com/ClickHouse/ClickHouse/pull/37604) ([alesapin](https://github.com/alesapin)).
* Update FileCache.cpp [#37606](https://github.com/ClickHouse/ClickHouse/pull/37606) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fix Clang-Tidy:  remove std::move() from trivially-copyable object [#37609](https://github.com/ClickHouse/ClickHouse/pull/37609) ([Heena Bansal](https://github.com/HeenaBansal2009)).
* Fix clang-tidy-14, part 1 [#37612](https://github.com/ClickHouse/ClickHouse/pull/37612) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Remove margin in test reports and change the font [#37614](https://github.com/ClickHouse/ClickHouse/pull/37614) ([Alexey Milovidov](https://github.com/alexey-milovidov)).
* Fix bugs in WindowView when using table identifier [#37619](https://github.com/ClickHouse/ClickHouse/pull/37619) ([vxider](https://github.com/Vxider)).
* Try fix tests [#37622](https://github.com/ClickHouse/ClickHouse/pull/37622) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Fix refactoring issue [#37629](https://github.com/ClickHouse/ClickHouse/pull/37629) ([alesapin](https://github.com/alesapin)).
* fix typo [#37637](https://github.com/ClickHouse/ClickHouse/pull/37637) ([flynn](https://github.com/ucasfl)).
* Fix excessive LIST requests to coordinator for transactions [#37640](https://github.com/ClickHouse/ClickHouse/pull/37640) ([Azat Khuzhin](https://github.com/azat)).
* Less flaky jbod rebalancer test [#37642](https://github.com/ClickHouse/ClickHouse/pull/37642) ([Amos Bird](https://github.com/amosbird)).
* Avoid useless context copy when building query interpreters [#37643](https://github.com/ClickHouse/ClickHouse/pull/37643) ([Amos Bird](https://github.com/amosbird)).
* Disable amqp-cpp and cassandra build if libuv is disabled [#37644](https://github.com/ClickHouse/ClickHouse/pull/37644) ([Robert Schulze](https://github.com/rschu1ze)).
* Fix errors of CheckTriviallyCopyableMove type [#37647](https://github.com/ClickHouse/ClickHouse/pull/37647) ([Heena Bansal](https://github.com/HeenaBansal2009)).
* Fix hung check [#37655](https://github.com/ClickHouse/ClickHouse/pull/37655) ([Kseniia Sumarokova](https://github.com/kssenii)).
* Fix flaky test test_row_policy [#37658](https://github.com/ClickHouse/ClickHouse/pull/37658) ([Vitaly Baranov](https://github.com/vitlibar)).
* Trying to remove prewhere_info.alias_actions as they seem to always be included in prewhere_actions [#37661](https://github.com/ClickHouse/ClickHouse/pull/37661) ([Alexander Gololobov](https://github.com/davenger)).
* test for [#36995](https://github.com/ClickHouse/ClickHouse/issues/36995) [#37668](https://github.com/ClickHouse/ClickHouse/pull/37668) ([Denny Crane](https://github.com/den-crane)).
* Cleanup unused file [#37674](https://github.com/ClickHouse/ClickHouse/pull/37674) ([hongbin](https://github.com/xlwh)).
* more verbose sanity checks [#37676](https://github.com/ClickHouse/ClickHouse/pull/37676) ([Sergei Trifonov](https://github.com/serxa)).
* Fix `test_keeper_force_recovery*` tests [#37679](https://github.com/ClickHouse/ClickHouse/pull/37679) ([Antonio Andelic](https://github.com/antonio2368)).
* Display entires for failed tests at the top of report by default [#37704](https://github.com/ClickHouse/ClickHouse/pull/37704) ([Vladimir C](https://github.com/vdimir)).
* Make GROUPING function skip constant folding [#37710](https://github.com/ClickHouse/ClickHouse/pull/37710) ([Dmitry Novik](https://github.com/novikd)).
* Get rid of duplicate download_previous_release [#37712](https://github.com/ClickHouse/ClickHouse/pull/37712) ([Vladimir C](https://github.com/vdimir)).
* Fix possible flaky 00814_replicated_minimalistic_part_header_zookeeper [#37714](https://github.com/ClickHouse/ClickHouse/pull/37714) ([Vladimir C](https://github.com/vdimir)).
* Fix build with -DENABLE_LIBRARIES=0 [#37719](https://github.com/ClickHouse/ClickHouse/pull/37719) ([Robert Schulze](https://github.com/rschu1ze)).
* Update grpc submodule to PR 9 [#37721](https://github.com/ClickHouse/ClickHouse/pull/37721) ([Harry Lee](https://github.com/HarryLeeIBM)).
* Initialize `ParallelReadBuffer` after construction [#37726](https://github.com/ClickHouse/ClickHouse/pull/37726) ([Antonio Andelic](https://github.com/antonio2368)).
* Some fixes for tests [#37740](https://github.com/ClickHouse/ClickHouse/pull/37740) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Add test 02315_pmj_union_ubsan_35857 [#37744](https://github.com/ClickHouse/ClickHouse/pull/37744) ([Vladimir C](https://github.com/vdimir)).
* Upgrade libxml2 to 2.9.14 [#37745](https://github.com/ClickHouse/ClickHouse/pull/37745) ([Suzy Wang](https://github.com/SuzyWangIBMer)).
* Implement new submodule sync to fix nightly scan [#37750](https://github.com/ClickHouse/ClickHouse/pull/37750) ([Boris Kuschel](https://github.com/bkuschel)).
* Return back [#37266](https://github.com/ClickHouse/ClickHouse/issues/37266) [#37755](https://github.com/ClickHouse/ClickHouse/pull/37755) ([Anton Popov](https://github.com/CurtizJ)).
* Fix flaky 01154_move_partition_long [#37763](https://github.com/ClickHouse/ClickHouse/pull/37763) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Moved `ClientConfigurationPerRequest` to ClickHouse [#37767](https://github.com/ClickHouse/ClickHouse/pull/37767) ([Vladimir Chebotarev](https://github.com/excitoon)).
* Got rid of `S3AuthSigner` [#37769](https://github.com/ClickHouse/ClickHouse/pull/37769) ([Vladimir Chebotarev](https://github.com/excitoon)).
* Fix empty target table id in WindowView [#37770](https://github.com/ClickHouse/ClickHouse/pull/37770) ([vxider](https://github.com/Vxider)).
* Typos [#37773](https://github.com/ClickHouse/ClickHouse/pull/37773) ([Vladimir Chebotarev](https://github.com/excitoon)).
* Use multitarget framework for numZerosInTail implementation [#37777](https://github.com/ClickHouse/ClickHouse/pull/37777) ([Alexander Gololobov](https://github.com/davenger)).
* Dictionaries improve exception messages [#37778](https://github.com/ClickHouse/ClickHouse/pull/37778) ([Maksim Kita](https://github.com/kitaisreal)).
* Ignore PART_IS_TEMPORARILY_LOCKED error in BC check [#37788](https://github.com/ClickHouse/ClickHouse/pull/37788) ([Kruglov Pavel](https://github.com/Avogar)).
* Use clang compiler by default and cleanup cmake preload [#37796](https://github.com/ClickHouse/ClickHouse/pull/37796) ([Azat Khuzhin](https://github.com/azat)).
* Fix reading of empty S3 files [#37801](https://github.com/ClickHouse/ClickHouse/pull/37801) ([Azat Khuzhin](https://github.com/azat)).
* Metadata storage abstraction for Disks [#37804](https://github.com/ClickHouse/ClickHouse/pull/37804) ([alesapin](https://github.com/alesapin)).
* Minor follow-up to cache table: std::{vector-->array} [#37806](https://github.com/ClickHouse/ClickHouse/pull/37806) ([Robert Schulze](https://github.com/rschu1ze)).
* Delay schedule of the cleaning task in WindowView [#37807](https://github.com/ClickHouse/ClickHouse/pull/37807) ([vxider](https://github.com/Vxider)).
* Fix submodule changed label condition [#37825](https://github.com/ClickHouse/ClickHouse/pull/37825) ([Vladimir C](https://github.com/vdimir)).
* Fix build (packager) [#37829](https://github.com/ClickHouse/ClickHouse/pull/37829) ([Nikita Taranov](https://github.com/nickitat)).
* Executable task forward job callback [#37830](https://github.com/ClickHouse/ClickHouse/pull/37830) ([Maksim Kita](https://github.com/kitaisreal)).
* Fix docs check (remove git clean for yarn.lock) [#37836](https://github.com/ClickHouse/ClickHouse/pull/37836) ([Vladimir C](https://github.com/vdimir)).
* Opentracing minimal changes for processors [#37837](https://github.com/ClickHouse/ClickHouse/pull/37837) ([Ilya Yatsishin](https://github.com/qoega)).
* Do not count covered unexpected parts on sanity check [#37839](https://github.com/ClickHouse/ClickHouse/pull/37839) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Print query in one line on fatal errors [#37840](https://github.com/ClickHouse/ClickHouse/pull/37840) ([Azat Khuzhin](https://github.com/azat)).
* Hotfix for minio in functional tests [#37845](https://github.com/ClickHouse/ClickHouse/pull/37845) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Hotfix for minio in functional tests 2 [#37846](https://github.com/ClickHouse/ClickHouse/pull/37846) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Fire and clean windows in WindowView only when data is inserted [#37853](https://github.com/ClickHouse/ClickHouse/pull/37853) ([vxider](https://github.com/Vxider)).
* Don't try to kill empty list of containers in `integration/runner` [#37854](https://github.com/ClickHouse/ClickHouse/pull/37854) ([Vladimir Chebotarev](https://github.com/excitoon)).
* Minor cleanup [#37866](https://github.com/ClickHouse/ClickHouse/pull/37866) ([Igor Nikonov](https://github.com/devcrafter)).
* Update protobuf files for kafka and rabbitmq [fix integration tests] [#37884](https://github.com/ClickHouse/ClickHouse/pull/37884) ([Nikita Taranov](https://github.com/nickitat)).
* Fix keeper converter test [#37890](https://github.com/ClickHouse/ClickHouse/pull/37890) ([alesapin](https://github.com/alesapin)).
* Fix stress hung because of attached MV with sleep() [#37892](https://github.com/ClickHouse/ClickHouse/pull/37892) ([Azat Khuzhin](https://github.com/azat)).
* Fix build [#37903](https://github.com/ClickHouse/ClickHouse/pull/37903) ([Nikita Taranov](https://github.com/nickitat)).
* Try fix `test_grpc_protocol/test.py::test_progress` [#37908](https://github.com/ClickHouse/ClickHouse/pull/37908) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Disable flaky tests with MaterializedPostgreSQL [#37924](https://github.com/ClickHouse/ClickHouse/pull/37924) ([Alexander Tokmakov](https://github.com/tavplubix)).
* ProcfsMetricsProvider fix typo [#37927](https://github.com/ClickHouse/ClickHouse/pull/37927) ([Maksim Kita](https://github.com/kitaisreal)).
* Function dictGet check arguments size [#37930](https://github.com/ClickHouse/ClickHouse/pull/37930) ([Maksim Kita](https://github.com/kitaisreal)).
* Hierarchical dictionaries performance test fix [#37953](https://github.com/ClickHouse/ClickHouse/pull/37953) ([Maksim Kita](https://github.com/kitaisreal)).
* Normalize UTF8 performance test fix [#37954](https://github.com/ClickHouse/ClickHouse/pull/37954) ([Maksim Kita](https://github.com/kitaisreal)).
* Fix flaky `test_parts_delete_zookeeper` [#37957](https://github.com/ClickHouse/ClickHouse/pull/37957) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Better parsing of `versionId` in `S3::URI::URI` [#37964](https://github.com/ClickHouse/ClickHouse/pull/37964) ([Vladimir Chebotarev](https://github.com/excitoon)).
* More consistent use of platform macros [#37969](https://github.com/ClickHouse/ClickHouse/pull/37969) ([Robert Schulze](https://github.com/rschu1ze)).
* Always disable --color-diagnostics for LLVM [#37970](https://github.com/ClickHouse/ClickHouse/pull/37970) ([Robert Schulze](https://github.com/rschu1ze)).
* Try fix `test_consistent_parts_after_clone_replica` [#37976](https://github.com/ClickHouse/ClickHouse/pull/37976) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Generalize setup_minio.sh [#37979](https://github.com/ClickHouse/ClickHouse/pull/37979) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Add checks for numChildren in Keeper [#37980](https://github.com/ClickHouse/ClickHouse/pull/37980) ([alesapin](https://github.com/alesapin)).
* PartialSortingTransform refactoring [#37992](https://github.com/ClickHouse/ClickHouse/pull/37992) ([Maksim Kita](https://github.com/kitaisreal)).
* Follow up on self-extracting-executable [#38011](https://github.com/ClickHouse/ClickHouse/pull/38011) ([Yakov Olkhovskiy](https://github.com/yakov-olkhovskiy)).
* Update docker run client command in docs [#38015](https://github.com/ClickHouse/ClickHouse/pull/38015) ([Vladimir C](https://github.com/vdimir)).
* Add another useful `docker exec` option [#38016](https://github.com/ClickHouse/ClickHouse/pull/38016) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Add release commit hash to image labels [#38020](https://github.com/ClickHouse/ClickHouse/pull/38020) ([Pavel Novitskiy](https://github.com/pnovitskiy)).
* Use correct version in `DiskObjectStorageMetadata` [#38021](https://github.com/ClickHouse/ClickHouse/pull/38021) ([Antonio Andelic](https://github.com/antonio2368)).
* Examples coroutines build fix [#38023](https://github.com/ClickHouse/ClickHouse/pull/38023) ([Maksim Kita](https://github.com/kitaisreal)).
* Use pdqsort instead of standard sort [#38025](https://github.com/ClickHouse/ClickHouse/pull/38025) ([Maksim Kita](https://github.com/kitaisreal)).
* Decimal: noexcept move constructor/assignment operator [#38026](https://github.com/ClickHouse/ClickHouse/pull/38026) ([Igor Nikonov](https://github.com/devcrafter)).
* tests: avoid "_csv.Error: field larger than field limit (131072)" error [#38030](https://github.com/ClickHouse/ClickHouse/pull/38030) ([Azat Khuzhin](https://github.com/azat)).
* More consts for disks [#38033](https://github.com/ClickHouse/ClickHouse/pull/38033) ([alesapin](https://github.com/alesapin)).
* Remove link to past event [#38045](https://github.com/ClickHouse/ClickHouse/pull/38045) ([Ivan Blinkov](https://github.com/blinkov)).
* Use workflow URL as "link" ref [#38057](https://github.com/ClickHouse/ClickHouse/pull/38057) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Fix a compile errors in self-extracting-executable (de)compressor [#38087](https://github.com/ClickHouse/ClickHouse/pull/38087) ([Robert Schulze](https://github.com/rschu1ze)).
* Small follow-up for FPC codec [#38089](https://github.com/ClickHouse/ClickHouse/pull/38089) ([Robert Schulze](https://github.com/rschu1ze)).
* Add dispatch to CherryPick action [#38095](https://github.com/ClickHouse/ClickHouse/pull/38095) ([Mikhail f. Shiryaev](https://github.com/Felixoid)).
* Fix: build error [#38098](https://github.com/ClickHouse/ClickHouse/pull/38098) ([Igor Nikonov](https://github.com/devcrafter)).
* Try random container name in integration tests runner [#38107](https://github.com/ClickHouse/ClickHouse/pull/38107) ([Alexander Tokmakov](https://github.com/tavplubix)).
* Try to fix fpc codec [#38117](https://github.com/ClickHouse/ClickHouse/pull/38117) ([Anton Popov](https://github.com/CurtizJ)).
* Remove unused class member [#38120](https://github.com/ClickHouse/ClickHouse/pull/38120) ([Daohui Wang](https://github.com/wangdh15)).
