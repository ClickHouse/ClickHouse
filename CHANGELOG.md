## ClickHouse release 18.5.1, 2018-07-31

### New features:

* Added the hash function `murmurHash2_32` [#2756](https://github.com/yandex/ClickHouse/pull/2756).

### Improvements:

* Now you can use the `from_env` attribute to set values in config files from environment variables [#2741](https://github.com/yandex/ClickHouse/pull/2741).
* Added case-insensitive versions of the `coalesce`, `ifNull`, and `nullIf functions` [#2752](https://github.com/yandex/ClickHouse/pull/2752).

### Bug fixes:

* Fixed a possible bug when starting a replica [#2759](https://github.com/yandex/ClickHouse/pull/2759).

## ClickHouse release 18.4.0, 2018-07-28

### New features:

* Added system tables: `formats`, `data_type_families`, `aggregate_function_combinators`, `table_functions`, `table_engines`, `collations` [#2721](https://github.com/yandex/ClickHouse/pull/2721).
* Added the ability to use a table function instead of a table as an argument of a `remote` or `cluster` table function [#2708](https://github.com/yandex/ClickHouse/pull/2708).
* Support for `HTTP Basic` authentication in the replication protocol [#2727](https://github.com/yandex/ClickHouse/pull/2727).
* The `has` function now allows searching for a numeric value in an array of `Enum` values [Maxim Khrisanfov](https://github.com/yandex/ClickHouse/pull/2699).
* Support for adding arbitrary message separators when reading from `Kafka` [Amos Bird](https://github.com/yandex/ClickHouse/pull/2701).

### Improvements:

* The `ALTER TABLE t DELETE WHERE` query does not rewrite data chunks that were not affected by the WHERE condition [#2694](https://github.com/yandex/ClickHouse/pull/2694).
* The `use_minimalistic_checksums_in_zookeeper` option for `ReplicatedMergeTree` tables is enabled by default. This setting was added in version 1.1.54378, 2018-04-16. Versions that are older than 1.1.54378 can no longer be installed.
* Support for running `KILL` and `OPTIMIZE` queries that specify `ON CLUSTER` [Winter Zhang](https://github.com/yandex/ClickHouse/pull/2689).

### Bug fixes:

* Fixed the error `Column ... is not under an aggregate function and not in GROUP BY` for aggregation with an IN expression. This bug appeared in version 18.1.0. ([bbdd780b](https://github.com/yandex/ClickHouse/commit/bbdd780be0be06a0f336775941cdd536878dd2c2))
* Fixed a bug in the `windowFunnel` aggregate function [Winter Zhang](https://github.com/yandex/ClickHouse/pull/2735).
* Fixed a bug in the `anyHeavy`  aggregate function ([a2101df2](https://github.com/yandex/ClickHouse/commit/a2101df25a6a0fba99aa71f8793d762af2b801ee))
* Fixed server crash when using the `countArray()` aggregate function.

## ClickHouse release 18.1.0, 2018-07-23

### New features:

* Support for the `ALTER TABLE t DELETE WHERE` query for non-replicated MergeTree tables ([#2634](https://github.com/yandex/ClickHouse/pull/2634)).
* Support for arbitrary types for the `uniq*` family of aggregate functions ([#2010](https://github.com/yandex/ClickHouse/issues/2010)).
* Support for arbitrary types in comparison operators ([#2026](https://github.com/yandex/ClickHouse/issues/2026)).
* The `users.xml` file allows setting a subnet mask in the format `10.0.0.1/255.255.255.0`. This is necessary for using masks for IPv6 networks with zeros in the middle ([#2637](https://github.com/yandex/ClickHouse/pull/2637)).
* Added the `arrayDistinct` function ([#2670](https://github.com/yandex/ClickHouse/pull/2670)).
* The SummingMergeTree engine can now work with AggregateFunction type columns ([Constantin S. Pan](https://github.com/yandex/ClickHouse/pull/2566)).

### Improvements:

* Changed the numbering scheme for release versions. Now the first part contains the year of release (A.D., Moscow timezone, minus 2000), the second part contains the number for major changes (increases for most releases), and the third part is the patch version. Releases are still backwards compatible, unless otherwise stated in the changelog.
* Faster conversions of floating-point numbers to a string ([Amos Bird](https://github.com/yandex/ClickHouse/pull/2664)).
* If some rows were skipped during an insert due to parsing errors (this is possible with the `input_allow_errors_num` and `input_allow_errors_ratio` settings enabled), the number of skipped rows is now written to the server log ([Leonardo Cecchi](https://github.com/yandex/ClickHouse/pull/2669)).

### Bug fixes:

* Fixed the TRUNCATE command for temporary tables ([Amos Bird](https://github.com/yandex/ClickHouse/pull/2624)).
* Fixed a rare deadlock in the ZooKeeper client library that occurred when there was a network error while reading the response ([c315200](https://github.com/yandex/ClickHouse/commit/c315200e64b87e44bdf740707fc857d1fdf7e947)).
* Fixed an error during a CAST to Nullable types ([#1322](https://github.com/yandex/ClickHouse/issues/1322)).
* Fixed the incorrect result of the `maxIntersection()` function when the boundaries of intervals coincided ([Michael Furmur](https://github.com/yandex/ClickHouse/pull/2657)).
* Fixed incorrect transformation of the OR expression chain in a function argument ([chenxing-xc](https://github.com/yandex/ClickHouse/pull/2663)).
* Fixed performance degradation for queries containing `IN (subquery)` expressions inside another subquery ([#2571](https://github.com/yandex/ClickHouse/issues/2571)).
* Fixed incompatibility between servers with different versions in distributed queries that use a `CAST` function that isn't in uppercase letters ([fe8c4d6](https://github.com/yandex/ClickHouse/commit/fe8c4d64e434cacd4ceef34faa9005129f2190a5)).
* Added missing quoting of identifiers for queries to an external DBMS ([#2635](https://github.com/yandex/ClickHouse/issues/2635)).

### Backward incompatible changes:

* Converting a string containing the number zero to DateTime does not work. Example: `SELECT toDateTime('0')`. This is also the reason that `DateTime DEFAULT '0'` does not work in tables, as well as `<null_value>0</null_value>` in dictionaries. Solution: replace `0` with `0000-00-00 00:00:00`.


## ClickHouse release 1.1.54394, 2018-07-12

### New features:

* Added the `histogram` aggregate function ([Mikhail Surin](https://github.com/yandex/ClickHouse/pull/2521)).
* Now `OPTIMIZE TABLE ... FINAL` can be used without specifying partitions for `ReplicatedMergeTree` ([Amos Bird](https://github.com/yandex/ClickHouse/pull/2600)).

### Bug fixes:

* Fixed a problem with a very small timeout for sockets (one second) for reading and writing when sending and downloading replicated data, which made it impossible to download larger parts if there is a load on the network or disk (it resulted in cyclical attempts to download parts). This error occurred in version 1.1.54388.
* Fixed issues when using chroot in ZooKeeper if you inserted duplicate data blocks in the table.
* The `has` function now works correctly for an array with Nullable elements ([#2115](https://github.com/yandex/ClickHouse/issues/2115)).
* The `system.tables` table now works correctly when used in distributed queries. The `metadata_modification_time` and `engine_full` columns are now non-virtual. Fixed an error that occurred if only these columns were requested from the table.
* Fixed how an empty `TinyLog` table works after inserting an empty data block ([#2563](https://github.com/yandex/ClickHouse/issues/2563)).
* The `system.zookeeper` table works if the value of the node in ZooKeeper is NULL.

## ClickHouse release 1.1.54390, 2018-07-06

### New features:

* Queries can be sent in `multipart/form-data` format (in the `query` field), which is useful if external data is also sent for query processing ([Olga Hvostikova](https://github.com/yandex/ClickHouse/pull/2490)).
* Added the ability to enable or disable processing single or double quotes when reading data in CSV format. You can configure this in the `format_csv_allow_single_quotes` and `format_csv_allow_double_quotes` settings ([Amos Bird](https://github.com/yandex/ClickHouse/pull/2574)).
* Now `OPTIMIZE TABLE ... FINAL` can be used without specifying the partition for non-replicated variants of `MergeTree` ([Amos Bird](https://github.com/yandex/ClickHouse/pull/2599)).

### Improvements:

* Improved performance, reduced memory consumption, and correct tracking of memory consumption with use of the IN operator when a table index could be used ([#2584](https://github.com/yandex/ClickHouse/pull/2584)).
* Removed redundant checking of checksums when adding a data part. This is important when there are a large number of replicas, because in these cases the total number of checks was equal to N^2.
* Added support for `Array(Tuple(...))`  arguments for the `arrayEnumerateUniq` function ([#2573](https://github.com/yandex/ClickHouse/pull/2573)).
* Added `Nullable` support for the `runningDifference` function ([#2594](https://github.com/yandex/ClickHouse/pull/2594)).
* Improved query analysis performance when there is a very large number of expressions ([#2572](https://github.com/yandex/ClickHouse/pull/2572)).
* Faster selection of data parts for merging in `ReplicatedMergeTree` tables. Faster recovery of the ZooKeeper session ([#2597](https://github.com/yandex/ClickHouse/pull/2597)).
* The `format_version.txt` file for `MergeTree` tables is re-created if it is missing, which makes sense if ClickHouse is launched after copying the directory structure without files ([Ciprian Hacman](https://github.com/yandex/ClickHouse/pull/2593)).

### Bug fixes:

* Fixed a bug when working with ZooKeeper that could make it impossible to recover the session and readonly states of tables before restarting the server.
* Fixed a bug when working with ZooKeeper that could result in old nodes not being deleted if the session is interrupted.
* Fixed an error in the `quantileTDigest` function for Float arguments (this bug was introduced in version 1.1.54388) ([Mikhail Surin](https://github.com/yandex/ClickHouse/pull/2553)).
* Fixed a bug in the index for MergeTree tables if the primary key column is located inside the function for converting types between signed and unsigned integers of the same size ([#2603](https://github.com/yandex/ClickHouse/pull/2603)).
* Fixed segfault if `macros` are used but they aren't in the config file ([#2570](https://github.com/yandex/ClickHouse/pull/2570)).
* Fixed switching to the default database when reconnecting the client ([#2583](https://github.com/yandex/ClickHouse/pull/2583)).
* Fixed a bug that occurred when the `use_index_for_in_with_subqueries` setting was disabled.

### Security fix:

* Sending files is no longer possible when connected to MySQL (`LOAD DATA LOCAL INFILE`).

## ClickHouse release 1.1.54388, 2018-06-28

### New features:

* Support for the `ALTER TABLE t DELETE WHERE` query for replicated tables. Added the `system.mutations` table to track progress of this type of queries.
* Support for the `ALTER TABLE t [REPLACE|ATTACH] PARTITION` query for MergeTree tables.
* Support for the `TRUNCATE TABLE` query ([Winter Zhang](https://github.com/yandex/ClickHouse/pull/2260)).
* Several new `SYSTEM` queries for replicated tables (`RESTART REPLICAS`, `SYNC REPLICA`, `[STOP|START] [MERGES|FETCHES|SENDS REPLICATED|REPLICATION QUEUES]`).
* Added the ability to write to a table with the MySQL engine and the corresponding table function ([sundy-li](https://github.com/yandex/ClickHouse/pull/2294)).
* Added the `url()` table function and the `URL` table engine ([Alexander Sapin](https://github.com/yandex/ClickHouse/pull/2501)).
* Added the `windowFunnel` aggregate function ([sundy-li](https://github.com/yandex/ClickHouse/pull/2352)).
* New `startsWith` and `endsWith` functions for strings ([Vadim Plakhtinsky](https://github.com/yandex/ClickHouse/pull/2429)).
* The `numbers()` table function now allows you to specify the offset ([Winter Zhang](https://github.com/yandex/ClickHouse/pull/2535)).
* The password to `clickhouse-client` can be entered interactively.
* Server logs can now be sent to syslog ([Alexander Krasheninnikov](https://github.com/yandex/ClickHouse/pull/2459)).
* Support for logging in dictionaries with a shared library source ([Alexander Sapin](https://github.com/yandex/ClickHouse/pull/2472)).
* Support for custom CSV delimiters ([Ivan Zhukov](https://github.com/yandex/ClickHouse/pull/2263)).
* Added the `date_time_input_format` setting. If you switch this setting to `'best_effort'`, DateTime values will be read in a wide range of formats.
* Added the `clickhouse-obfuscator` utility for data obfuscation. Usage example: publishing data used in performance tests.

### Experimental features:

* Added the ability to calculate `and` arguments only where they are needed ([Anastasia Tsarkova](https://github.com/yandex/ClickHouse/pull/2272)).
* JIT compilation to native code is now available for some expressions ([pyos](https://github.com/yandex/ClickHouse/pull/2277)).

### Bug fixes:

* Duplicates no longer appear for a query with `DISTINCT` and `ORDER BY`.
* Queries with `ARRAY JOIN` and `arrayFilter` no longer return an incorrect result.
* Fixed an error when reading an array column from a Nested structure ([#2066](https://github.com/yandex/ClickHouse/issues/2066)).
* Fixed an error when analyzing queries with a HAVING section like `HAVING tuple IN (...)`.
* Fixed an error when analyzing queries with recursive aliases.
* Fixed an error when reading from ReplacingMergeTree with a condition in PREWHERE that filters all rows ([#2525](https://github.com/yandex/ClickHouse/issues/2525)).
* User profile settings were not applied when using sessions in the HTTP interface.
* Fixed how settings are applied from the command line parameters in `clickhouse-local`.
* The ZooKeeper client library now uses the session timeout received from the server.
* Fixed a bug in the ZooKeeper client library when the client waited for the server response longer than the timeout.
* Fixed pruning of parts for queries with conditions on partition key columns ([#2342](https://github.com/yandex/ClickHouse/issues/2342)).
* Merges are now possible after `CLEAR COLUMN IN PARTITION` ([#2315](https://github.com/yandex/ClickHouse/issues/2315)).
* Type mapping in the ODBC table function has been fixed ([sundy-li](https://github.com/yandex/ClickHouse/pull/2268)).
* Type comparisons have been fixed for `DateTime` with and without the time zone ([Alexander Bocharov](https://github.com/yandex/ClickHouse/pull/2400)).
* Fixed syntactic parsing and formatting of the `CAST` operator.
* Fixed insertion into a materialized view for the Distributed table engine ([Babacar Diassé](https://github.com/yandex/ClickHouse/pull/2411)).
* Fixed a race condition when writing data from the `Kafka` engine to materialized views ([Yangkuan Liu](https://github.com/yandex/ClickHouse/pull/2448)).
* Fixed SSRF in the `remote()` table function.
* Fixed exit behavior of `clickhouse-client` in multiline mode ([#2510](https://github.com/yandex/ClickHouse/issues/2510)).

### Improvements:

* Background tasks in replicated tables are now performed in a thread pool instead of in separate threads ([Silviu Caragea](https://github.com/yandex/ClickHouse/pull/1722)).
* Improved LZ4 compression performance.
* Faster analysis for queries with a large number of JOINs and sub-queries.
* The DNS cache is now updated automatically when there are too many network errors.
* Table inserts no longer occur if the insert into one of the materialized views is not possible because it has too many parts.
* Corrected the discrepancy in the event counters `Query`, `SelectQuery`, and `InsertQuery`.
* Expressions like `tuple IN (SELECT tuple)` are allowed if the tuple types match.
* A server with replicated tables can start even if you haven't configured ZooKeeper.
* When calculating the number of available CPU cores, limits on cgroups are now taken into account ([Atri Sharma](https://github.com/yandex/ClickHouse/pull/2325)).
* Added chown for config directories in the systemd config file ([Mikhail Shiryaev](https://github.com/yandex/ClickHouse/pull/2421)).

### Build changes:

* The gcc8 compiler can be used for builds.
* Added the ability to build llvm from a submodule.
* The version of the librdkafka library has been updated to v0.11.4.
* Added the ability to use the system libcpuid library. The library version has been updated to 0.4.0.
* Fixed the build using the vectorclass library ([Babacar Diassé](https://github.com/yandex/ClickHouse/pull/2274)).
* Cmake now generates files for ninja by default (like when using `-G Ninja`).
* Added the ability to use the libtinfo library instead of libtermcap ([Georgy Kondratiev](https://github.com/yandex/ClickHouse/pull/2519)).
* Fixed a header file conflict in Fedora Rawhide ([#2520](https://github.com/yandex/ClickHouse/issues/2520)).

### Backward incompatible changes:

* Removed escaping in `Vertical` and `Pretty*` formats and deleted the `VerticalRaw` format.
* If servers with version 1.1.54388 (or newer) and servers with older version are used simultaneously in distributed query and the query has `cast(x, 'Type')` expression in the form without `AS` keyword and with `cast` not in uppercase, then the exception with message like `Not found column cast(0, 'UInt8') in block` will be thrown. Solution: update server on all cluster nodes.

## ClickHouse release 1.1.54385, 2018-06-01

### Bug fixes:
* Fixed an error that in some cases caused ZooKeeper operations to block.

## ClickHouse release 1.1.54383, 2018-05-22

### Bug fixes:
* Fixed a slowdown of replication queue if a table has many replicas.

## ClickHouse release 1.1.54381, 2018-05-14

### Bug fixes:
* Fixed a nodes leak in ZooKeeper when ClickHouse loses connection to ZooKeeper server.

## ClickHouse release 1.1.54380, 2018-04-21

### New features:
* Added table function `file(path, format, structure)`. An example reading bytes from `/dev/urandom`: `ln -s /dev/urandom /var/lib/clickhouse/user_files/random` `clickhouse-client -q "SELECT * FROM file('random', 'RowBinary', 'd UInt8') LIMIT 10"`.

### Improvements:
* Subqueries could be wrapped by `()` braces (to enhance queries readability). For example, `(SELECT 1) UNION ALL (SELECT 1)`.
* Simple `SELECT` queries  from table `system.processes` are not counted in `max_concurrent_queries` limit.

### Bug fixes:
* Fixed incorrect behaviour of `IN` operator when select from `MATERIALIZED VIEW`.
* Fixed incorrect filtering by partition index in expressions like `WHERE partition_key_column IN (...)`
* Fixed inability to execute `OPTIMIZE` query on non-leader replica if the table was `REANAME`d.
* Fixed authorization error when execute `OPTIMIZE` or `ALTER` queries on a non-leader replica.
* Fixed freezing of `KILL QUERY` queries.
* Fixed an error in ZooKeeper client library which led to watches loses, freezing of distributed DDL queue and slowing replication queue if non-empty `chroot` prefix is used in ZooKeeper configuration.

### Backward incompatible changes:
* Removed support of expressions like `(a, b) IN (SELECT (a, b))` (instead of them you can use their equivalent `(a, b) IN (SELECT a, b)`). In previous releases, these expressions led to undetermined data filtering or caused errors.

## ClickHouse release 1.1.54378, 2018-04-16
### New features:

* Logging level can be changed without restarting the server.
* Added the `SHOW CREATE DATABASE` query.
* The `query_id` can be passed to `clickhouse-client` (elBroom).
* New setting: `max_network_bandwidth_for_all_users`.
* Added support for `ALTER TABLE ... PARTITION ... ` for `MATERIALIZED VIEW`.
* Added information about the size of data parts in uncompressed form in the system table.
* Server-to-server encryption support for distributed tables (`<secure>1</secure>` in the replica config in `<remote_servers>`).
* Configuration of the table level for the `ReplicatedMergeTree` family in order to minimize the amount of data stored in zookeeper: `use_minimalistic_checksums_in_zookeeper = 1`
* Configuration of the `clickhouse-client` prompt. By default, server names are now output to the prompt. The server's display name can be changed; it's also sent in the `X-ClickHouse-Display-Name` HTTP header (Kirill Shvakov).
* Multiple comma-separated `topics` can be specified for the `Kafka` engine (Tobias Adamson).
* When a query is stopped by `KILL QUERY` or `replace_running_query`, the client receives the `Query was cancelled` exception instead of an incomplete response.

### Improvements:

* `ALTER TABLE ... DROP/DETACH PARTITION` queries are run at the front of the replication queue.
* `SELECT ... FINAL` and `OPTIMIZE ... FINAL` can be used even when the table has a single data part.
* A `query_log` table is recreated on the fly if it was deleted manually (Kirill Shvakov).
* The `lengthUTF8` function runs faster (zhang2014).
* Improved performance of synchronous inserts in `Distributed` tables (`insert_distributed_sync = 1`) when there is a very large number of shards.
* The server accepts the `send_timeout` and `receive_timeout` settings from the client and applies them when connecting to the client (they are applied in reverse order: the server socket's `send_timeout` is set to the `receive_timeout` value received from the client, and vice versa).
* More robust crash recovery for asynchronous insertion into `Distributed` tables.
* The return type of the `countEqual` function changed from `UInt32` to `UInt64` (谢磊).

### Bug fixes:

* Fixed an error with `IN` when the left side of the expression is `Nullable`.
* Correct results are now returned when using tuples with `IN` when some of the tuple components are in the table index.
* The `max_execution_time` limit now works correctly with distributed queries.
* Fixed errors when calculating the size of composite columns in the `system.columns` table.
* Fixed an error when creating a temporary table `CREATE TEMPORARY TABLE IF NOT EXISTS`.
* Fixed errors in `StorageKafka` (#2075)
* Fixed server crashes from invalid arguments of certain aggregate functions.
* Fixed the error that prevented the `DETACH DATABASE` query from stopping background tasks for `ReplicatedMergeTree` tables.
* `Too many parts` state is less likely to happen when inserting into aggregated materialized views (#2084).
* Corrected recursive handling of substitutions in the config if a substitution must be followed by another substitution on the same level.
* Corrected the syntax in the metadata file when creating a `VIEW` that uses a query with `UNION ALL`.
* `SummingMergeTree` now works correctly for summation of nested data structures with a composite key.
* Fixed the possibility of a race condition when choosing the leader for `ReplicatedMergeTree` tables.

### Build changes:

* The build supports `ninja` instead of `make` and uses it by default for building releases.
* Renamed packages: `clickhouse-server-base` is now `clickhouse-common-static`; `clickhouse-server-common` is now `clickhouse-server`; `clickhouse-common-dbg` is now `clickhouse-common-static-dbg`. To install, use `clickhouse-server clickhouse-client`. Packages with the old names will still load in the repositories for backward compatibility.

### Backward-incompatible changes:

* Removed the special interpretation of an IN expression if an array is specified on the left side. Previously, the expression `arr IN (set)` was interpreted as "at least one `arr` element belongs to the `set`". To get the same behavior in the new version, write `arrayExists(x -> x IN (set), arr)`.
* Disabled the incorrect use of the socket option `SO_REUSEPORT`, which was incorrectly enabled by default in the Poco library. Note that on Linux there is no longer any reason to simultaneously specify the addresses `::` and `0.0.0.0` for listen – use just `::`, which allows listening to the connection both over IPv4 and IPv6 (with the default kernel config settings). You can also revert to the behavior from previous versions by specifying `<listen_reuse_port>1</listen_reuse_port>` in the config.


## ClickHouse release 1.1.54370, 2018-03-16

### New features:

* Added the `system.macros` table and auto updating of macros when the config file is changed.
* Added the `SYSTEM RELOAD CONFIG` query.
* Added the `maxIntersections(left_col, right_col)` aggregate function, which returns the maximum number of simultaneously intersecting intervals `[left; right]`. The `maxIntersectionsPosition(left, right)` function returns the beginning of the "maximum" interval. ([Michael Furmur](https://github.com/yandex/ClickHouse/pull/2012)).

### Improvements:

* When inserting data in a `Replicated` table, fewer requests are made to `ZooKeeper` (and most of the user-level errors have disappeared from the `ZooKeeper` log).
* Added the ability to create aliases for sets. Example: `WITH (1, 2, 3) AS set SELECT number IN set FROM system.numbers LIMIT 10`.

### Bug fixes:

* Fixed the `Illegal PREWHERE` error when reading from `Merge` tables over `Distributed` tables.
* Added fixes that allow you to run `clickhouse-server` in IPv4-only Docker containers.
* Fixed a race condition when reading from system `system.parts_columns` tables.
* Removed double buffering during a synchronous insert to a `Distributed` table, which could have caused the connection to timeout.
* Fixed a bug that caused excessively long waits for an unavailable replica before beginning a `SELECT` query.
* Fixed incorrect dates in the `system.parts` table.
* Fixed a bug that made it impossible to insert data in a `Replicated` table if `chroot` was non-empty in the configuration of the `ZooKeeper` cluster.
* Fixed the vertical merging algorithm for an empty `ORDER BY` table.
* Restored the ability to use dictionaries in queries to remote tables, even if these dictionaries are not present on the requestor server. This functionality was lost in release 1.1.54362.
* Restored the behavior for queries like `SELECT * FROM remote('server2', default.table) WHERE col IN (SELECT col2 FROM default.table)` when the right side argument of the `IN` should use a remote `default.table` instead of a local one. This behavior was broken in version 1.1.54358.
* Removed extraneous error-level logging of `Not found column ... in block`.

## ClickHouse release 1.1.54356, 2018-03-06

### New features:

* Aggregation without `GROUP BY` for an empty set (such as `SELECT count(*) FROM table WHERE 0`) now returns a result with one row with null values for aggregate functions, in compliance with the SQL standard. To restore the old behavior (return an empty result), set `empty_result_for_aggregation_by_empty_set` to 1.
* Added type conversion for `UNION ALL`. Different alias names are allowed in `SELECT` positions in `UNION ALL`, in compliance with the SQL standard.
* Arbitrary expressions are supported in `LIMIT BY` sections. Previously, it was only possible to use columns resulting from `SELECT`.
* An index of `MergeTree` tables is used when `IN` is applied to a tuple of expressions from the columns of the primary key. Example: `WHERE (UserID, EventDate) IN ((123, '2000-01-01'), ...)` (Anastasiya Tsarkova).
* Added the `clickhouse-copier` tool for copying between clusters and resharding data (beta).
* Added consistent hashing functions: `yandexConsistentHash`, `jumpConsistentHash`, `sumburConsistentHash`. They can be used as a sharding key in order to reduce the amount of network traffic during subsequent reshardings.
* Added functions: `arrayAny`, `arrayAll`, `hasAny`, `hasAll`, `arrayIntersect`, `arrayResize`.
* Added the `arrayCumSum` function (Javi Santana).
* Added the `parseDateTimeBestEffort`, `parseDateTimeBestEffortOrZero`, and `parseDateTimeBestEffortOrNull`functions to read the DateTime from a string containing text in a wide variety of possible formats.
* It is now possible to change the logging settings without restarting the server.
* Added the `cluster` table function. Example: `cluster(cluster_name, db, table)`. The `remote` table function can accept the cluster name as the first argument, if it is specified as an identifier.
* Added the `create_table_query` and `engine_full` virtual columns to the `system.tables`table . The `metadata_modification_time` column is virtual.
* Added the `data_path` and `metadata_path` columns to `system.tables` and` system.databases` tables, and added the `path` column to the `system.parts` and `system.parts_columns` tables.
* Added additional information about merges in the `system.part_log` table.
* An arbitrary partitioning key can be used for the `system.query_log` table (Kirill Shvakov).
* The `SHOW TABLES` query now also shows temporary tables. Added temporary tables and the `is_temporary` column to `system.tables` (zhang2014).
* Added the `DROP TEMPORARY TABLE` query (zhang2014).
* Support for `SHOW CREATE TABLE` for temporary tables (zhang2014).
* Added the `system_profile` configuration parameter for the settings used by internal processes.
* Support for loading `object_id` as an attribute in `MongoDB` dictionaries (Pavel Litvinenko).
* Reading `null` as the default value when loading data for an external dictionary with the `MongoDB` source (Pavel Litvinenko).
* Reading `DateTime` values in the `Values` format from a Unix timestamp without single quotes.
* Failover is supported in `remote` table functions for cases when some of the replicas are missing the requested table.
* Configuration settings can be overridden in the command line when you run `clickhouse-server`. Example: `clickhouse-server -- --logger.level=information`.
* Implemented the `empty` function from a `FixedString` argument: the function returns 1 if the string consists entirely of null bytes (zhang2014).
* Added the `listen_try`configuration parameter for listening to at least one of the listen addresses without quitting, if some of the addresses can't be listened to (useful for systems with disabled support for IPv4 or IPv6).
* Added the `VersionedCollapsingMergeTree` table engine.
* Support for rows and arbitrary numeric types for the `library` dictionary source.
* `MergeTree` tables can be used without a primary key (you need to specify `ORDER BY tuple()`).
* A `Nullable` type can be `CAST` to a non-`Nullable` type if the argument is not `NULL`.
* `RENAME TABLE` can be performed for `VIEW`.
* Added the `odbc_default_field_size` option, which allows you to extend the maximum size of the value loaded from an ODBC source (by default, it is 1024).

### Improvements:

* Limits and quotas on the result are no longer applied to intermediate data for `INSERT SELECT` queries or for `SELECT` subqueries.
* Fewer false triggers of `force_restore_data` when checking the status of `Replicated` tables when the server starts.
* Added the `allow_distributed_ddl` option.
* Nondeterministic functions are not allowed in expressions for `MergeTree` table keys.
* Files with substitutions from `config.d` directories are loaded in alphabetical order.
* Improved performance of the `arrayElement` function in the case of a constant multidimensional array with an empty array as one of the elements. Example: `[[1], []][x]`.
* The server starts faster now when using configuration files with very large substitutions (for instance, very large lists of IP networks).
* When running a query, table valued functions run once. Previously, `remote` and `mysql`  table valued functions performed the same query twice to retrieve the table structure from a remote server.
* The `MkDocs` documentation generator is used.
* When you try to delete a table column that `DEFAULT`/`MATERIALIZED` expressions of other columns depend on, an exception is thrown (zhang2014).
* Added the ability to parse an empty line in text formats as the number 0 for `Float` data types. This feature was previously available but was lost in release 1.1.54342.
* `Enum` values can be used in `min`, `max`, `sum` and some other functions. In these cases, it uses the corresponding numeric values. This feature was previously available but was lost in the release 1.1.54337.
* Added `max_expanded_ast_elements` to restrict the size of the AST after recursively expanding aliases.

### Bug fixes:

* Fixed cases when unnecessary columns were removed from subqueries in error, or not removed from subqueries containing `UNION ALL`.
* Fixed a bug in merges for `ReplacingMergeTree` tables.
* Fixed synchronous insertions in `Distributed` tables (`insert_distributed_sync = 1`).
* Fixed segfault for certain uses of `FULL` and `RIGHT JOIN` with duplicate columns in subqueries.
* Fixed the order of the `source` and `last_exception` columns in the `system.dictionaries` table.
* Fixed a bug when the `DROP DATABASE` query did not delete the file with metadata.
* Fixed the `DROP DATABASE` query for `Dictionary` databases.
* Fixed the low precision of `uniqHLL12` and `uniqCombined` functions for cardinalities greater than 100 million items (Alex Bocharov).
* Fixed the calculation of implicit default values when necessary to simultaneously calculate default explicit expressions in `INSERT` queries (zhang2014).
* Fixed a rare case when a query to a `MergeTree` table couldn't finish (chenxing-xc).
* Fixed a crash that occurred when running a `CHECK` query for `Distributed` tables if all shards are local (chenxing.xc).
* Fixed a slight performance regression with functions that use regular expressions.
* Fixed a performance regression when creating multidimensional arrays from complex expressions.
* Fixed a bug that could cause an extra `FORMAT` section to appear in an `.sql` file with metadata.
* Fixed a bug that caused the `max_table_size_to_drop` limit to apply when trying to delete a `MATERIALIZED VIEW` looking at an explicitly specified table.
* Fixed incompatibility with old clients (old clients were sometimes sent data with the `DateTime('timezone')` type, which they do not understand).
* Fixed a bug when reading `Nested` column elements of structures that were added using `ALTER` but that are empty for the old partitions, when the conditions for these columns moved to `PREWHERE`.
* Fixed a bug when filtering tables by virtual `_table` columns in queries to `Merge` tables.
* Fixed a bug when using `ALIAS` columns in `Distributed` tables.
* Fixed a bug that made dynamic compilation impossible for queries with aggregate functions from the `quantile` family.
* Fixed a race condition in the query execution pipeline that occurred in very rare cases when using `Merge` tables with a large number of tables, and when using `GLOBAL` subqueries.
* Fixed a crash when passing arrays of different sizes to an `arrayReduce` function when using aggregate functions from multiple arguments.
* Prohibited the use of queries with `UNION ALL` in a `MATERIALIZED VIEW`.

### Backward incompatible changes:

* Removed the `distributed_ddl_allow_replicated_alter` option. This behavior is enabled by default.
* Removed the `UnsortedMergeTree` engine.

## ClickHouse release 1.1.54343, 2018-02-05

* Added macros support for defining cluster names in distributed DDL queries and constructors of Distributed tables: `CREATE TABLE distr ON CLUSTER '{cluster}' (...) ENGINE = Distributed('{cluster}', 'db', 'table')`.
* Now the table index is used for conditions like `expr IN (subquery)`.
* Improved processing of duplicates when inserting to Replicated tables, so they no longer slow down execution of the replication queue.

## ClickHouse release 1.1.54342, 2018-01-22

This release contains bug fixes for the previous release 1.1.54337:
* Fixed a regression in 1.1.54337: if the default user has readonly access, then the server refuses to start up with the message `Cannot create database in readonly mode`.
* Fixed a regression in 1.1.54337: on systems with `systemd`, logs are always written to syslog regardless of the configuration; the watchdog script still uses `init.d`.
* Fixed a regression in 1.1.54337: wrong default configuration in the Docker image.
* Fixed nondeterministic behaviour of GraphiteMergeTree (you can notice it in log messages `Data after merge is not byte-identical to data on another replicas`).
* Fixed a bug that may lead to inconsistent merges after OPTIMIZE query to Replicated tables (you may notice it in log messages `Part ... intersects previous part`).
* Buffer tables now work correctly when MATERIALIZED columns are present in the destination table (by zhang2014).
* Fixed a bug in implementation of NULL.

## ClickHouse release 1.1.54337, 2018-01-18

### New features:

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

### Performance optimizations:

* Improved performance of `min`, `max`, `any`, `anyLast`, `anyHeavy`, `argMin`, `argMax` aggregate functions for String arguments.
* Improved performance of `isInfinite`, `isFinite`, `isNaN`, `roundToExp2` functions.
* Improved performance of parsing and formatting values of type `Date` and `DateTime` in text formats.
* Improved performance and precision of parsing floating point numbers.
* Lowered memory usage for `JOIN` in the case when the left and right parts have columns with identical names that are not contained in `USING`.
* Improved performance of `varSamp`, `varPop`, `stddevSamp`, `stddevPop`, `covarSamp`, `covarPop`, and `corr` aggregate functions by reducing computational stability. The old functions are available under the names: `varSampStable`, `varPopStable`, `stddevSampStable`, `stddevPopStable`, `covarSampStable`, `covarPopStable`, `corrStable`.

### Bug fixes:

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

### Build improvements:

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

### Backward incompatible changes:

* The format for marks in `Log` type tables that contain `Nullable` columns was changed in a backward incompatible way. If you have these tables, you should convert them to the `TinyLog` type before starting up the new server version. To do this, replace `ENGINE = Log` with `ENGINE = TinyLog` in the corresponding `.sql` file in the `metadata` directory. If your table doesn't have `Nullable` columns or if the type of your table is not `Log`, then you don't need to do anything.
* Removed the `experimental_allow_extended_storage_definition_syntax` setting. Now this feature is enabled by default.
* To avoid confusion, the `runningIncome` function has been renamed to `runningDifferenceStartingWithFirstValue`.
* Removed the `FROM ARRAY JOIN arr` syntax when ARRAY JOIN is specified directly after FROM with no table (Amos Bird).
* Removed the `BlockTabSeparated` format that was used solely for demonstration purposes.
* Changed the serialization format of intermediate states of the aggregate functions `varSamp`, `varPop`, `stddevSamp`, `stddevPop`, `covarSamp`, `covarPop`, and `corr`. If you have stored states of these aggregate functions in tables (using the AggregateFunction data type or materialized views with corresponding states), please write to clickhouse-feedback@yandex-team.com.
* In previous server versions there was an undocumented feature: if an aggregate function depends on parameters, you can still specify it without parameters in the AggregateFunction data type. Example: `AggregateFunction(quantiles, UInt64)` instead of `AggregateFunction(quantiles(0.5, 0.9), UInt64)`. This feature was lost. Although it was undocumented, we plan to support it again in future releases.
* Enum data types cannot be used in min/max aggregate functions. The possibility will be returned back in future release.

### Please note when upgrading:
* When doing a rolling update on a cluster, at the point when some of the replicas are running the old version of ClickHouse and some are running the new version, replication is temporarily stopped and the message `unknown parameter 'shard'` appears in the log. Replication will continue after all replicas of the cluster are updated.
* If you have different ClickHouse versions on the cluster, you can get incorrect results for distributed queries with the aggregate functions `varSamp`, `varPop`, `stddevSamp`, `stddevPop`, `covarSamp`, `covarPop`, and `corr`. You should update all cluster nodes.

## ClickHouse release 1.1.54327, 2017-12-21

This release contains bug fixes for the previous release 1.1.54318:
* Fixed bug with possible race condition in replication that could lead to data loss. This issue affects versions 1.1.54310 and 1.1.54318. If you use one of these versions with Replicated tables, the update is strongly recommended. This issue shows in logs in Warning messages like `Part ... from own log doesn't exist.` The issue is relevant even if you don't see these messages in logs.

## ClickHouse release 1.1.54318, 2017-11-30

This release contains bug fixes for the previous release 1.1.54310:
* Fixed incorrect row deletions during merges in the SummingMergeTree engine
* Fixed a memory leak in unreplicated MergeTree engines
* Fixed performance degradation with frequent inserts in MergeTree engines
* Fixed an issue that was causing the replication queue to stop running
* Fixed rotation and archiving of server logs

## ClickHouse release 1.1.54310, 2017-11-01

### New features:
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

### Backward incompatible changes:
* Creation of temporary tables with an engine other than Memory is forbidden.
* Explicit creation of tables with the View or MaterializedView engine is forbidden.
* During table creation, a new check verifies that the sampling key expression is included in the primary key.

### Bug fixes:
* Fixed hangups when synchronously inserting into a Distributed table.
* Fixed nonatomic adding and removing of parts in Replicated tables.
* Data inserted into a materialized view is not subjected to unnecessary deduplication.
* Executing a query to a Distributed table for which the local replica is lagging and remote replicas are unavailable does not result in an error anymore.
* Users don't need access permissions to the `default` database to create temporary tables anymore.
* Fixed crashing when specifying the Array type without arguments.
* Fixed hangups when the disk volume containing server logs is full.
* Fixed an overflow in the `toRelativeWeekNum` function for the first week of the Unix epoch.

### Build improvements:
* Several third-party libraries (notably Poco) were updated and converted to git submodules.

## ClickHouse release 1.1.54304, 2017-10-19

### New features:
* TLS support in the native protocol (to enable, set `tcp_ssl_port` in `config.xml`)

### Bug fixes:
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

## ClickHouse release 1.1.54292, 2017-09-20

### New features:
* Added the `pointInPolygon` function for working with coordinates on a coordinate plane.
* Added the `sumMap` aggregate function for calculating the sum of arrays, similar to `SummingMergeTree`.
* Added the `trunc` function. Improved performance of the rounding functions (`round`, `floor`, `ceil`, `roundToExp2`) and corrected the logic of how they work. Changed the logic of the `roundToExp2` function for fractions and negative numbers.
* The ClickHouse executable file is now less dependent on the libc version. The same ClickHouse executable file can run on a wide variety of Linux systems. Note: There is still a dependency when using compiled queries (with the setting `compile = 1`, which is not used by default).
* Reduced the time needed for dynamic compilation of queries.

### Bug fixes:
* Fixed an error that sometimes produced `part ... intersects previous part` messages and weakened replica consistency.
* Fixed an error that caused the server to lock up if ZooKeeper was unavailable during shutdown.
* Removed excessive logging when restoring replicas.
* Fixed an error in the UNION ALL implementation.
* Fixed an error in the concat function that occurred if the first column in a block has the Array type.
* Progress is now displayed correctly in the system.merges table.

## ClickHouse release 1.1.54289, 2017-09-13

### New features:
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

### Bug fixes:
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

### Improvements to development workflow and ClickHouse build:
* You can use `pbuilder` to build ClickHouse.
* You can use `libc++` instead of `libstdc++` for builds on Linux.
* Added instructions for using static code analysis tools: `Coverity`, `clang-tidy`, and `cppcheck`.

### Please note when upgrading:
* There is now a higher default value for the MergeTree setting `max_bytes_to_merge_at_max_space_in_pool` (the maximum total size of data parts to merge, in bytes): it has increased from 100 GiB to 150 GiB. This might result in large merges running after the server upgrade, which could cause an increased load on the disk subsystem. If the free space available on the server is less than twice the total amount of the merges that are running, this will cause all other merges to stop running, including merges of small data parts. As a result, INSERT requests will fail with the message "Merges are processing significantly slower than inserts." Use the `SELECT * FROM system.merges` request to monitor the situation. You can also check the `DiskSpaceReservedForMerge` metric in the `system.metrics` table, or in Graphite. You don't need to do anything to fix this, since the issue will resolve itself once the large merges finish. If you find this unacceptable, you can restore the previous value for the `max_bytes_to_merge_at_max_space_in_pool` setting (to do this, go to the `<merge_tree>` section in config.xml, set `<max_bytes_to_merge_at_max_space_in_pool>107374182400</max_bytes_to_merge_at_max_space_in_pool>` and restart the server).

## ClickHouse release 1.1.54284, 2017-08-29

* This is bugfix release for previous 1.1.54282 release. It fixes ZooKeeper nodes leak in `parts/` directory.

## ClickHouse release 1.1.54282, 2017-08-23

This is a bugfix release. The following bugs were fixed:
* `DB::Exception: Assertion violation: !_path.empty()` error when inserting into a Distributed table.
* Error when parsing inserted data in RowBinary format if the data begins with ';' character.
* Errors during runtime compilation of certain aggregate functions (e.g. `groupArray()`).

## ClickHouse release 1.1.54276, 2017-08-16

### New features:

* You can use an optional WITH clause in a SELECT query. Example query: `WITH 1+1 AS a SELECT a, a*a`
* INSERT can be performed synchronously in a Distributed table: OK is returned only after all the data is saved on all the shards. This is activated by the setting insert_distributed_sync=1.
* Added the UUID data type for working with 16-byte identifiers.
* Added aliases of CHAR, FLOAT and other types for compatibility with the Tableau.
* Added the functions toYYYYMM, toYYYYMMDD, and toYYYYMMDDhhmmss for converting time into numbers.
* You can use IP addresses (together with the hostname) to identify servers for clustered DDL queries.
* Added support for non-constant arguments and negative offsets in the function `substring(str, pos, len).`
* Added the max_size parameter for the `groupArray(max_size)(column)` aggregate function, and optimized its performance.

### Major changes:

* Improved security: all server files are created with 0640 permissions (can be changed via <umask> config parameter).
* Improved error messages for queries with invalid syntax.
* Significantly reduced memory consumption and improved performance when merging large sections of MergeTree data.
* Significantly increased the performance of data merges for the ReplacingMergeTree engine.
* Improved performance for asynchronous inserts from a Distributed table by batching multiple source inserts. To enable this functionality, use the setting distributed_directory_monitor_batch_inserts=1.

### Backward incompatible changes:

* Changed the binary format of aggregate states of `groupArray(array_column)` functions for arrays.

### Complete list of changes:

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

### Bug fixes:

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

### Improved workflow for developing and assembling ClickHouse:

* Builds can be assembled in Arcadia.
* You can use gcc 7 to compile ClickHouse.
* Parallel builds using ccache+distcc are faster now.

## ClickHouse release 1.1.54245, 2017-07-04

### New features:

* Distributed DDL (for example, `CREATE TABLE ON CLUSTER`).
* The replicated request `ALTER TABLE CLEAR COLUMN IN PARTITION.`
* The engine for Dictionary tables (access to dictionary data in the form of a table).
* Dictionary database engine (this type of database automatically has Dictionary tables available for all the connected external dictionaries).
* You can check for updates to the dictionary by sending a request to the source.
* Qualified column names
* Quoting identifiers using double quotation marks.
* Sessions in the HTTP interface.
* The OPTIMIZE query for a Replicated table can can run not only on the leader.

### Backward incompatible changes:

* Removed SET GLOBAL.

### Minor changes:

* If an alert is triggered, the full stack trace is printed into the log.
* Relaxed the verification of the number of damaged or extra data parts at startup (there were too many false positives).

### Bug fixes:

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
