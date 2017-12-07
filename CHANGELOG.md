# ClickHouse release 1.1.54318, 2017-11-30
* This is bugfix release for previous 1.1.54310 release.

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

## Backwards incompatible changes:
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
