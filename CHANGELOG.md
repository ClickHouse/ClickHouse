# ClickHouse release 1.1.54343, 2018-02-05

* Added macros support for defining cluster names in distributed DDL queries and constructors of Distributed tables: `CREATE TABLE distr ON CLUSTER '{cluster}' (...) ENGINE = Distributed('{cluster}', 'db', 'table')`.
* Now the table index is used for conditions like `expr IN (subquery)`.
* Improved processing of duplicates when inserting to Replicated tables, so they no longer slow down execution of the replication queue.

# ClickHouse release 1.1.54342, 2018-01-22

This release contains bug fixes for the previous release 1.1.54342:
* Fixed a regression in 1.1.54337: if the default user has readonly access, then the server refuses to start up with the message `Cannot create database in readonly mode`.
* Fixed a regression in 1.1.54337: on systems with `systemd`, logs are always written to syslog regardless of the configuration; the watchdog script still uses `init.d`.
* Fixed a regression in 1.1.54337: wrong default configuration in the Docker image.
* Fixed nondeterministic behaviour of GraphiteMergeTree (you can notice it in log messages `Data after merge is not byte-identical to data on another replicas`).
* Fixed a bug that may lead to inconsistent merges after OPTIMIZE query to Replicated tables (you may notice it in log messages `Part ... intersects previous part`).
* Buffer tables now work correctly when MATERIALIZED columns are present in the destination table (by zhang2014).
* Fixed a bug in implementation of NULL.

# ClickHouse release 1.1.54337, 2018-01-18

## New features:

* Added support for storage of multidimensional arrays and tuples (`Tuple` data type) in tables.
* Added support for table functions in `DESCRIBE` and `INSERT` queries. Added support for subqueries in `DESCRIBE`. Examples: `DESC TABLE remote('host', default.hits)`; `DESC TABLE (SELECT 1)`; `INSERT INTO TABLE FUNCTION remote('host', default.hits)`. Support for `INSERT INTO TABLE` syntax in addition to `INSERT INTO`.
* Improved support for timezones. The `DateTime` data type can be annotated with the timezone that is used for parsing and formatting in text formats. Example: `DateTime('Europe/Moscow')`. When timezones are specified in functions for DateTime arguments, the return type will track the timezone, and the value will be displayed as expected.
* Added the functions `toTimeZone`, `timeDiff`, `toQuarter`, `toRelativeQuarterNum`. The `toRelativeHour`/`Minute`/`Second` functions can take a value of type `Date` as an argument. The name of the `now` function has been made case-insensitive.
* Added the `toStartOfFifteenMinutes` function (Kirill Shvakov).
* Added the `clickhouse format` tool for formatting queries.
* Added the `format_schema_path` configuration parameter (Marek Vavruša). It is used for specifying a schema in `Cap'n'Proto` format. Schema files can be located only in the specified directory.
* Added support for config substitutions (`incl` and `conf.d`) for configuration of external dictionaries and models (Pavel Yakunin).
* Added a column with documentation for the `system.settings` table (Kirill Shvakov).
* Added the `system.parts_columns` table with information about column sizes in each data part of `MergeTree` tables.
* Added the `system.models` table with information about loaded `CatBoost` machine learning models.
* Added the `mysql` and `odbc` table functions along with the corresponding `MySQL` and `ODBC` table engines for working with foreign databases. This feature is in the beta stage.
* Added the possibility to pass an argument of type `AggregateFunction` for the `groupArray` aggregate function (so you can create an array of states of some aggregate function).
* Removed restrictions on various combinations of aggregate function combinators. For example, you can use `avgForEachIf` as well as `avgIfForEach` aggregate functions, which have different behaviors.
* The `-ForEach` aggregate function combinator is extended for the case of aggregate functions of multiple arguments.
* Added support for aggregate functions of `Nullable` arguments even for cases when the function returns a non-`Nullable` result (added with the contribution of Silviu Caragea). Examples: `groupArray`, `groupUniqArray`, `topK`.
* Added the `max_client_network_bandwidth` command line parameter for `clickhouse-client` (Kirill Shvakov).
* Users with the `readonly = 2` setting are allowed to work with TEMPORARY tables (CREATE, DROP, INSERT...) (Kirill Shvakov).
* Added support for using multiple consumers with the `Kafka` engine. Extended configuration options for `Kafka` (Marek Vavruša).
* Added the `intExp2` and `intExp10` functions.
* Added the `sumKahan` aggregate function (computationally stable summation of floating point numbers).
* Added to*Number*OrNull functions, where *Number* is a numeric type.
* Added support for the `WITH` clause for an `INSERT SELECT` query (by zhang2014).
* Added the settings `http_connection_timeout`, `http_send_timeout`, and `http_receive_timeout`. In particular, these settings are used for downloading data parts for replication. Changing these settings allows for faster failover if the network is overloaded.
* Added support for the `ALTER` query for tables of type `Null` (Anastasiya Tsarkova). Tables of type `Null` are often used with materialized views.
* The `reinterpretAsString` function is extended for all data types that are stored contiguously in memory.
* Added the `--silent` option for the `clickhouse-local` tool. It suppresses printing query execution info in stderr.
* Added support for reading values of type `Date` from text in a format where the month and/or day of the month is specified using a single digit instead of two digits (Amos Bird).

## Performance optimizations:

* Improved performance of `min`, `max`, `any`, `anyLast`, `anyHeavy`, `argMin`, `argMax` aggregate functions for String arguments.
* Improved performance of `isInfinite`, `isFinite`, `isNaN`, `roundToExp2` functions.
* Improved performance of parsing and formatting values of type `Date` and `DateTime` in text formats.
* Improved performance and precision of parsing floating point numbers.
* Lowered memory usage for `JOIN` in the case when the left and right parts have columns with identical names that are not contained in `USING`.
* Improved performance of `varSamp`, `varPop`, `stddevSamp`, `stddevPop`, `covarSamp`, `covarPop`, and `corr` aggregate functions by reducing computational stability. The old functions are available under the names: `varSampStable`, `varPopStable`, `stddevSampStable`, `stddevPopStable`, `covarSampStable`, `covarPopStable`, `corrStable`.

## Bug fixes:

* Fixed data deduplication after running a `DROP PARTITION` query. In the previous version, dropping a partition and INSERTing the same data again was not working because INSERTed blocks were considered duplicates.
* Fixed a bug that could lead to incorrect interpretation of the `WHERE` clause for `CREATE MATERIALIZED VIEW` queries with `POPULATE`.
* Fixed a bug in using the `root_path` parameter in the `zookeeper_servers` configuration.
* Fixed unexpected results of passing the `Date` argument to `toStartOfDay`.
* Fixed the `addMonths` and `subtractMonths` functions and the arithmetic for `INTERVAL n MONTH` in cases when the result has the previous year.
* Added missing support for the `UUID` data type for `DISTINCT`, `JOIN`, and `uniq` aggregate functions and external dictionaries (Evgeniy Ivanov). Support for `UUID` is still incomplete.
* Fixed `SummingMergeTree` behavior in cases when the rows summed to zero.
* Various fixes for the `Kafka` engine (Marek Vavruša).
* Fixed incorrect behavior of the `Join` table engine (Amos Bird).
* Fixed incorrect allocator behavior under FreeBSD and OS X.
* The `extractAll` function now supports empty matches.
* Fixed an error that blocked usage of `libressl` instead of `openssl`.
* Fixed the `CREATE TABLE AS SELECT` query from temporary tables.
* Fixed non-atomicity of updating the replication queue. This could lead to replicas being out of sync until the server restarts.
* Fixed possible overflow in `gcd`, `lcm` and `modulo` (`%` operator) (Maks Skorokhod).
* `-preprocessed` files are now created after changing `umask` (`umask` can be changed in the config).
* Fixed a bug in the background check of parts (`MergeTreePartChecker`) when using a custom partition key.
* Fixed parsing of tuples (values of the `Tuple` data type) in text formats.
* Improved error messages about incompatible types passed to `multiIf`, `array` and some other functions.
* Support for `Nullable` types is completely reworked. Fixed bugs that may lead to a server crash. Fixed almost all other bugs related to NULL support: incorrect type conversions in INSERT SELECT, insufficient support for Nullable in HAVING and PREWHERE, `join_use_nulls` mode, Nullable types as arguments of OR operator, etc.
* Fixed various bugs related to internal semantics of data types. Examples: unnecessary summing of `Enum` type fields in `SummingMergeTree`; alignment of `Enum` types in Pretty formats, etc.
* Stricter checks for allowed combinations of composite columns. Fixed several bugs that could lead to a server crash.
* Fixed the overflow when specifying a very large parameter for the `FixedString` data type.
* Fixed a bug in the `topK` aggregate function in a generic case.
* Added the missing check for equality of array sizes in arguments of n-ary variants of aggregate functions with an `-Array` combinator.
* Fixed the `--pager` option for `clickhouse-client` (by ks1322).
* Fixed the precision of the `exp10` function.
* Fixed the behavior of the `visitParamExtract` function for better compliance with documentation.
* Fixed the crash when incorrect data types are specified.
* Fixed the behavior of `DISTINCT` in the case when all columns are constants.
* Fixed query formatting in the case of using the `tupleElement` function with a complex constant expression as the tuple element index.
* Fixed the `Dictionary` table engine for dictionaries of type `range_hashed`.
* Fixed a bug that leads to excessive rows in the result of `FULL` and `RIGHT JOIN` (Amos Bird).
* Fixed a server crash when creating and removing temporary files in `config.d` directories during config reload.
* Fixed the `SYSTEM DROP DNS CACHE` query: the cache was flushed but addresses of cluster nodes were not updated.
* Fixed the behavior of `MATERIALIZED VIEW` after executing `DETACH TABLE` for the table under the view (Marek Vavruša).

## Build improvements:

* Builds use `pbuilder`. The build process is almost completely independent of the build host environment.
* A single build is used for different OS versions. Packages and binaries have been made compatible with a wide range of Linux systems.
* Added the `clickhouse-test` package. It can be used to run functional tests.
* The source tarball can now be published to the repository. It can be used to reproduce the build without using GitHub.
* Added limited integration with Travis CI. Due to limits on build time in Travis, only the debug build is tested and a limited subset of tests are run.
* Added support for `Cap'n'Proto` in the default build.
* Changed the format of documentation sources from `Restructured Text` to `Markdown`.
* Added support for `systemd` (Vladimir Smirnov). It is disabled by default due to incompatibility with some OS images and can be enabled manually.
* For dynamic code generation, `clang` and `lld` are embedded into the `clickhouse` binary. They can also be invoked as `clickhouse clang` and `clickhouse lld`.
* Removed usage of GNU extensions from the code. Enabled the `-Wextra` option. When building with `clang`, `libc++` is used instead of `libstdc++`.
* Extracted `clickhouse_parsers` and `clickhouse_common_io` libraries to speed up builds of various tools.

## Backward incompatible changes:

* The format for marks in `Log` type tables that contain `Nullable` columns was changed in a backward incompatible way. If you have these tables, you should convert them to the `TinyLog` type before starting up the new server version. To do this, replace `ENGINE = Log` with `ENGINE = TinyLog` in the corresponding `.sql` file in the `metadata` directory. If your table doesn't have `Nullable` columns or if the type of your table is not `Log`, then you don't need to do anything.
* Removed the `experimental_allow_extended_storage_definition_syntax` setting. Now this feature is enabled by default.
* To avoid confusion, the `runningIncome` function has been renamed to `runningDifferenceStartingWithFirstValue`.
* Removed the `FROM ARRAY JOIN arr` syntax when ARRAY JOIN is specified directly after FROM with no table (Amos Bird).
* Removed the `BlockTabSeparated` format that was used solely for demonstration purposes.
* Changed the serialization format of intermediate states of the aggregate functions `varSamp`, `varPop`, `stddevSamp`, `stddevPop`, `covarSamp`, `covarPop`, and `corr`. If you have stored states of these aggregate functions in tables (using the AggregateFunction data type or materialized views with corresponding states), please write to clickhouse-feedback@yandex-team.com.
* In previous server versions there was an undocumented feature: if an aggregate function depends on parameters, you can still specify it without parameters in the AggregateFunction data type. Example: `AggregateFunction(quantiles, UInt64)` instead of `AggregateFunction(quantiles(0.5, 0.9), UInt64)`. This feature was lost. Although it was undocumented, we plan to support it again in future releases.
* Enum data types cannot be used in min/max aggregate functions. The possibility will be returned back in future release.

## Please note when upgrading:
* When doing a rolling update on a cluster, at the point when some of the replicas are running the old version of ClickHouse and some are running the new version, replication is temporarily stopped and the message `unknown parameter 'shard'` appears in the log. Replication will continue after all replicas of the cluster are updated.
* If you have different ClickHouse versions on the cluster, you can get incorrect results for distributed queries with the aggregate functions `varSamp`, `varPop`, `stddevSamp`, `stddevPop`, `covarSamp`, `covarPop`, and `corr`. You should update all cluster nodes.

# ClickHouse release 1.1.54327, 2017-12-21

This release contains bug fixes for the previous release 1.1.54318:
* Fixed bug with possible race condition in replication that could lead to data loss. This issue affects versions 1.1.54310 and 1.1.54318. If you use one of these versions with Replicated tables, the update is strongly recommended. This issue shows in logs in Warning messages like `Part ... from own log doesn't exist.` The issue is relevant even if you don't see these messages in logs.

# ClickHouse release 1.1.54318, 2017-11-30

This release contains bug fixes for the previous release 1.1.54310:
* Fixed incorrect row deletions during merges in the SummingMergeTree engine
* Fixed a memory leak in unreplicated MergeTree engines
* Fixed performance degradation with frequent inserts in MergeTree engines
* Fixed an issue that was causing the replication queue to stop running
* Fixed rotation and archiving of server logs

# ClickHouse release 1.1.54310, 2017-11-01

## New features:
* Custom partitioning key for the MergeTree family of table engines.
* [Kafka](https://clickhouse.yandex/docs/en/single/index.html#document-table_engines/kafka) table engine.
* Added support for loading [CatBoost](https://catboost.yandex/) models and applying them to data stored in ClickHouse.
* Added support for time zones with non-integer offsets from UTC.
* Added support for arithmetic operations with time intervals.
* The range of values for the Date and DateTime types is extended to the year 2105.
* Added the `CREATE MATERIALIZED VIEW x TO y` query (specifies an existing table for storing the data of a materialized view).
* Added the `ATTACH TABLE` query without arguments.
* The processing logic for Nested columns with names ending in -Map in a SummingMergeTree table was extracted to the sumMap aggregate function. You can now specify such columns explicitly.
* Max size of the IP trie dictionary is increased to 128M entries.
* Added the `getSizeOfEnumType` function.
* Added the `sumWithOverflow` aggregate function.
* Added support for the Cap'n Proto input format.
* You can now customize compression level when using the zstd algorithm.

## Backward incompatible changes:
* Creation of temporary tables with an engine other than Memory is forbidden.
* Explicit creation of tables with the View or MaterializedView engine is forbidden.
* During table creation, a new check verifies that the sampling key expression is included in the primary key.

## Bug fixes:
* Fixed hangups when synchronously inserting into a Distributed table.
* Fixed nonatomic adding and removing of parts in Replicated tables.
* Data inserted into a materialized view is not subjected to unnecessary deduplication.
* Executing a query to a Distributed table for which the local replica is lagging and remote replicas are unavailable does not result in an error anymore.
* Users don't need access permissions to the `default` database to create temporary tables anymore.
* Fixed crashing when specifying the Array type without arguments.
* Fixed hangups when the disk volume containing server logs is full.
* Fixed an overflow in the `toRelativeWeekNum` function for the first week of the Unix epoch.

## Build improvements:
* Several third-party libraries (notably Poco) were updated and converted to git submodules.

# ClickHouse release 1.1.54304, 2017-10-19

## New features:
* TLS support in the native protocol (to enable, set `tcp_ssl_port` in `config.xml`)

## Bug fixes:
* `ALTER` for replicated tables now tries to start running as soon as possible
* Fixed crashing when reading data with the setting `preferred_block_size_bytes=0`
* Fixed crashes of `clickhouse-client` when `Page Down` is pressed
* Correct interpretation of certain complex queries with `GLOBAL IN` and `UNION ALL`
* `FREEZE PARTITION` always works atomically now
* Empty POST requests now return a response with code 411
* Fixed interpretation errors for expressions like `CAST(1 AS Nullable(UInt8))`
* Fixed an error when reading columns like `Array(Nullable(String))` from `MergeTree` tables
* Fixed crashing when parsing queries like `SELECT dummy AS dummy, dummy AS b`
* Users are updated correctly when `users.xml` is invalid
* Correct handling when an executable dictionary returns a non-zero response code

# ClickHouse release 1.1.54292, 2017-09-20

## New features:
* Added the `pointInPolygon` function for working with coordinates on a coordinate plane.
* Added the `sumMap` aggregate function for calculating the sum of arrays, similar to `SummingMergeTree`.
* Added the `trunc` function. Improved performance of the rounding functions (`round`, `floor`, `ceil`, `roundToExp2`) and corrected the logic of how they work. Changed the logic of the `roundToExp2` function for fractions and negative numbers.
* The ClickHouse executable file is now less dependent on the libc version. The same ClickHouse executable file can run on a wide variety of Linux systems. Note: There is still a dependency when using compiled queries (with the setting `compile = 1`, which is not used by default).
* Reduced the time needed for dynamic compilation of queries.

## Bug fixes:
* Fixed an error that sometimes produced `part ... intersects previous part` messages and weakened replica consistency.
* Fixed an error that caused the server to lock up if ZooKeeper was unavailable during shutdown.
* Removed excessive logging when restoring replicas.
* Fixed an error in the UNION ALL implementation.
* Fixed an error in the concat function that occurred if the first column in a block has the Array type.
* Progress is now displayed correctly in the system.merges table.

# ClickHouse release 1.1.54289, 2017-09-13

## New features:
* `SYSTEM` queries for server administration: `SYSTEM RELOAD DICTIONARY`, `SYSTEM RELOAD DICTIONARIES`, `SYSTEM DROP DNS CACHE`, `SYSTEM SHUTDOWN`, `SYSTEM KILL`.
* Added functions for working with arrays: `concat`, `arraySlice`, `arrayPushBack`, `arrayPushFront`, `arrayPopBack`, `arrayPopFront`.
* Added the `root` and `identity` parameters for the ZooKeeper configuration. This allows you to isolate individual users on the same ZooKeeper cluster.
* Added the aggregate functions `groupBitAnd`, `groupBitOr`, and `groupBitXor` (for compatibility, they can also be accessed with the names `BIT_AND`, `BIT_OR`, and `BIT_XOR`).
* External dictionaries can be loaded from MySQL by specifying a socket in the filesystem.
* External dictionaries can be loaded from MySQL over SSL (the `ssl_cert`, `ssl_key`, and `ssl_ca` parameters).
* Added the `max_network_bandwidth_for_user` setting to restrict the overall bandwidth use for queries per user.
* Support for `DROP TABLE` for temporary tables.
* Support for reading `DateTime` values in Unix timestamp format from the `CSV` and `JSONEachRow` formats.
* Lagging replicas in distributed queries are now excluded by default (the default threshold is 5 minutes).
* FIFO locking is used during ALTER: an ALTER query isn't blocked indefinitely for continuously running queries.
* Option to set `umask` in the config file.
* Improved performance for queries with `DISTINCT`.

## Bug fixes:
* Improved the process for deleting old nodes in ZooKeeper. Previously, old nodes sometimes didn't get deleted if there were very frequent inserts, which caused the server to be slow to shut down, among other things.
* Fixed randomization when choosing hosts for the connection to ZooKeeper.
* Fixed the exclusion of lagging replicas in distributed queries if the replica is localhost.
* Fixed an error where a data part in a `ReplicatedMergeTree` table could be broken after running `ALTER MODIFY` on an element in a `Nested` structure.
* Fixed an error that could cause SELECT queries to "hang".
* Improvements to distributed DDL queries.
* Fixed the query `CREATE TABLE ... AS <materialized view>`.
* Resolved the deadlock in the `ALTER ... CLEAR COLUMN IN PARTITION` query for `Buffer` tables.
* Fixed the invalid default value for `Enum`s (0 instead of the minimum) when using the `JSONEachRow` and `TSKV` formats.
* Resolved the appearance of zombie processes when using a dictionary with an `executable` source.
* Fixed segfault for the HEAD query.

## Improvements to development workflow and ClickHouse build:
* You can use `pbuilder` to build ClickHouse.
* You can use `libc++` instead of `libstdc++` for builds on Linux.
* Added instructions for using static code analysis tools: `Coverity`, `clang-tidy`, and `cppcheck`.

## Please note when upgrading:
* There is now a higher default value for the MergeTree setting `max_bytes_to_merge_at_max_space_in_pool` (the maximum total size of data parts to merge, in bytes): it has increased from 100 GiB to 150 GiB. This might result in large merges running after the server upgrade, which could cause an increased load on the disk subsystem. If the free space available on the server is less than twice the total amount of the merges that are running, this will cause all other merges to stop running, including merges of small data parts. As a result, INSERT requests will fail with the message "Merges are processing significantly slower than inserts." Use the `SELECT * FROM system.merges` request to monitor the situation. You can also check the `DiskSpaceReservedForMerge` metric in the `system.metrics` table, or in Graphite. You don't need to do anything to fix this, since the issue will resolve itself once the large merges finish. If you find this unacceptable, you can restore the previous value for the `max_bytes_to_merge_at_max_space_in_pool` setting (to do this, go to the `<merge_tree>` section in config.xml, set `<max_bytes_to_merge_at_max_space_in_pool>107374182400</max_bytes_to_merge_at_max_space_in_pool>` and restart the server).

# ClickHouse release 1.1.54284, 2017-08-29

* This is bugfix release for previous 1.1.54282 release. It fixes ZooKeeper nodes leak in `parts/` directory.

# ClickHouse release 1.1.54282, 2017-08-23

This is a bugfix release. The following bugs were fixed:
* `DB::Exception: Assertion violation: !_path.empty()` error when inserting into a Distributed table.
* Error when parsing inserted data in RowBinary format if the data begins with ';' character.
* Errors during runtime compilation of certain aggregate functions (e.g. `groupArray()`).

# ClickHouse release 1.1.54276, 2017-08-16

## New features:

* You can use an optional WITH clause in a SELECT query. Example query: `WITH 1+1 AS a SELECT a, a*a`
* INSERT can be performed synchronously in a Distributed table: OK is returned only after all the data is saved on all the shards. This is activated by the setting insert_distributed_sync=1.
* Added the UUID data type for working with 16-byte identifiers.
* Added aliases of CHAR, FLOAT and other types for compatibility with the Tableau.
* Added the functions toYYYYMM, toYYYYMMDD, and toYYYYMMDDhhmmss for converting time into numbers.
* You can use IP addresses (together with the hostname) to identify servers for clustered DDL queries.
* Added support for non-constant arguments and negative offsets in the function `substring(str, pos, len).`
* Added the max_size parameter for the `groupArray(max_size)(column)` aggregate function, and optimized its performance.

## Major changes:

* Improved security: all server files are created with 0640 permissions (can be changed via <umask> config parameter).
* Improved error messages for queries with invalid syntax.
* Significantly reduced memory consumption and improved performance when merging large sections of MergeTree data.
* Significantly increased the performance of data merges for the ReplacingMergeTree engine.
* Improved performance for asynchronous inserts from a Distributed table by batching multiple source inserts. To enable this functionality, use the setting distributed_directory_monitor_batch_inserts=1.

## Backward incompatible changes:

* Changed the binary format of aggregate states of `groupArray(array_column)` functions for arrays.

## Complete list of changes:

* Added the `output_format_json_quote_denormals` setting, which enables outputting nan and inf values in JSON format.
* Optimized thread allocation when reading from a Distributed table.
* Settings can be modified in readonly mode if the value doesn't change.
* Added the ability to read fractional granules of the MergeTree engine in order to meet restrictions on the block size specified in the preferred_block_size_bytes setting. The purpose is to reduce the consumption of RAM and increase cache locality when processing queries from tables with large columns.
* Efficient use of indexes that contain expressions like `toStartOfHour(x)` for conditions like `toStartOfHour(x) op сonstexpr.`
* Added new settings for MergeTree engines (the merge_tree section in config.xml):
   - replicated_deduplication_window_seconds sets the size of deduplication window in seconds for Replicated tables.
   - cleanup_delay_period sets how often to start cleanup to remove outdated data.
   - replicated_can_become_leader can prevent a replica from becoming the leader (and assigning merges).
* Accelerated cleanup to remove outdated data from ZooKeeper.
* Multiple improvements and fixes for clustered DDL queries. Of particular interest is the new setting distributed_ddl_task_timeout, which limits the time to wait for a response from the servers in the cluster.
* Improved display of stack traces in the server logs.
* Added the "none" value for the compression method.
* You can use multiple dictionaries_config sections in config.xml.
* It is possible to connect to MySQL through a socket in the file system.
* The `system.parts` table has a new column with information about the size of marks, in bytes.

## Bug fixes:

* Distributed tables using a Merge table now work correctly for a SELECT query with a condition on the  _table field.
* Fixed a rare race condition in ReplicatedMergeTree when checking data parts.
* Fixed possible freezing on "leader election" when starting a server.
* The max_replica_delay_for_distributed_queries setting was ignored when using a local replica of the data source. This has been fixed.
* Fixed incorrect behavior of `ALTER TABLE CLEAR COLUMN IN PARTITION` when attempting to clean a non-existing column.
* Fixed an exception in the multiIf function when using empty arrays or strings.
* Fixed excessive memory allocations when deserializing Native format.
* Fixed incorrect auto-update of Trie dictionaries.
* Fixed an exception when running queries with a GROUP BY clause from a Merge table when using SAMPLE.
* Fixed a crash of GROUP BY when using distributed_aggregation_memory_efficient=1.
* Now you can specify the database.table in the right side of IN and JOIN.
* Too many threads were used for parallel aggregation. This has been fixed.
* Fixed how the "if" function works with FixedString arguments.
* SELECT worked incorrectly from a Distributed table for shards with a weight of 0. This has been fixed.
* Crashes no longer occur when running `CREATE VIEW IF EXISTS.`
* Fixed incorrect behavior when input_format_skip_unknown_fields=1 is set and there are negative numbers.
* Fixed an infinite loop in the `dictGetHierarchy()` function if there is some invalid data in the dictionary.
* Fixed `Syntax error: unexpected (...)` errors  when running distributed queries with subqueries in an IN or JOIN clause and Merge tables.
* Fixed the incorrect interpretation of a SELECT query from Dictionary tables.
* Fixed the "Cannot mremap" error when using arrays in IN and JOIN clauses with more than 2 billion elements.
* Fixed the failover for dictionaries with MySQL as the source.

## Improved workflow for developing and assembling ClickHouse:

* Builds can be assembled in Arcadia.
* You can use gcc 7 to compile ClickHouse.
* Parallel builds using ccache+distcc are faster now.

# ClickHouse release 1.1.54245, 2017-07-04

## New features:

* Distributed DDL (for example, `CREATE TABLE ON CLUSTER`).
* The replicated request `ALTER TABLE CLEAR COLUMN IN PARTITION.`
* The engine for Dictionary tables (access to dictionary data in the form of a table).
* Dictionary database engine (this type of database automatically has Dictionary tables available for all the connected external dictionaries).
* You can check for updates to the dictionary by sending a request to the source.
* Qualified column names
* Quoting identifiers using double quotation marks.
* Sessions in the HTTP interface.
* The OPTIMIZE query for a Replicated table can can run not only on the leader.

## Backward incompatible changes:

* Removed SET GLOBAL.

## Minor changes:

* If an alert is triggered, the full stack trace is printed into the log.
* Relaxed the verification of the number of damaged or extra data parts at startup (there were too many false positives).

## Bug fixes:

* Fixed a bad connection "sticking" when inserting into a Distributed table.
* GLOBAL IN now works for a query from a Merge table that looks at a Distributed table.
* The incorrect number of cores was detected on a Google Compute Engine virtual machine. This has been fixed.
* Changes in how an executable source of cached external dictionaries works.
* Fixed the comparison of strings containing null characters.
* Fixed the comparison of Float32 primary key fields with constants.
* Previously, an incorrect estimate of the size of a field could lead to overly large allocations. This has been fixed.
* Fixed a crash when querying a Nullable column added to a table using ALTER.
* Fixed a crash when sorting by a Nullable column, if the number of rows is less than LIMIT.
* Fixed an ORDER BY subquery consisting of only constant values.
* Previously, a Replicated table could remain in the invalid state after a failed DROP TABLE.
* Aliases for scalar subqueries with empty results are no longer lost.
* Now a query that used compilation does not fail with an error if the .so file gets damaged.
