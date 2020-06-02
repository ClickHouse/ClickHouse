---
toc_priority: 52
toc_title: System Tables
---

# System Tables {#system-tables}

## Introduction {#system-tables-introduction}

System tables provide information about:

- Server states, processes, and environment.
- Server's internal processes.

System tables:

- Located in the `system` database.
- Available only for reading data.
- Can't be dropped or altered, but can be detached.

Most of system tables store their data in RAM. ClickHouse server creates such system tables at the start.

The [metric_log](#system_tables-metric_log), [query_log](#system_tables-query_log), [query_thread_log](#system_tables-query_thread_log), [trace_log](#system_tables-trace_log) system tables store data in a storage filesystem. You can alter them or remove from a disk manually. If you remove one of that tables from a disk, the ClickHouse server creates the table again at the time of the next recording. A storage period for these tables is not limited, and ClickHouse server doesn't delete their data automatically. You need to organize removing of outdated logs by yourself. For example, you can use [TTL](../sql-reference/statements/alter.md#manipulations-with-table-ttl) settings for removing outdated log records. 


### Sources of System Metrics {#system-tables-sources-of-system-metrics}

For collecting system metrics ClickHouse server uses:

- `CAP_NET_ADMIN` capability.
- [procfs](https://en.wikipedia.org/wiki/Procfs) (only in Linux).

**procfs**

If ClickHouse server doesn't have `CAP_NET_ADMIN` capability, it tries to fall back to `ProcfsMetricsProvider`. `ProcfsMetricsProvider` allows collecting per-query system metrics (for CPU and I/O).

If procfs is supported and enabled on the system, ClickHouse server collects these metrics:

- `OSCPUVirtualTimeMicroseconds`
- `OSCPUWaitMicroseconds`
- `OSIOWaitMicroseconds`
- `OSReadChars`
- `OSWriteChars`
- `OSReadBytes`
- `OSWriteBytes`

## system.asynchronous\_metrics {#system_tables-asynchronous_metrics}

Contains metrics that are calculated periodically in the background. For example, the amount of RAM in use.

Columns:

-   `metric` ([String](../sql-reference/data-types/string.md)) — Metric name.
-   `value` ([Float64](../sql-reference/data-types/float.md)) — Metric value.

**Example**

``` sql
SELECT * FROM system.asynchronous_metrics LIMIT 10
```

``` text
┌─metric──────────────────────────────────┬──────value─┐
│ jemalloc.background_thread.run_interval │          0 │
│ jemalloc.background_thread.num_runs     │          0 │
│ jemalloc.background_thread.num_threads  │          0 │
│ jemalloc.retained                       │  422551552 │
│ jemalloc.mapped                         │ 1682989056 │
│ jemalloc.resident                       │ 1656446976 │
│ jemalloc.metadata_thp                   │          0 │
│ jemalloc.metadata                       │   10226856 │
│ UncompressedCacheCells                  │          0 │
│ MarkCacheFiles                          │          0 │
└─────────────────────────────────────────┴────────────┘
```

**See Also**

-   [Monitoring](monitoring.md) — Base concepts of ClickHouse monitoring.
-   [system.metrics](#system_tables-metrics) — Contains instantly calculated metrics.
-   [system.events](#system_tables-events) — Contains a number of events that have occurred.
-   [system.metric\_log](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.

## system.clusters {#system-clusters}

Contains information about clusters available in the config file and the servers in them.

Columns:

-   `cluster` (String) — The cluster name.
-   `shard_num` (UInt32) — The shard number in the cluster, starting from 1.
-   `shard_weight` (UInt32) — The relative weight of the shard when writing data.
-   `replica_num` (UInt32) — The replica number in the shard, starting from 1.
-   `host_name` (String) — The host name, as specified in the config.
-   `host_address` (String) — The host IP address obtained from DNS.
-   `port` (UInt16) — The port to use for connecting to the server.
-   `user` (String) — The name of the user for connecting to the server.
-   `errors_count` (UInt32) - number of times this host failed to reach replica.
-   `estimated_recovery_time` (UInt32) - seconds left until replica error count is zeroed and it is considered to be back to normal.

Please note that `errors_count` is updated once per query to the cluster, but `estimated_recovery_time` is recalculated on-demand. So there could be a case of non-zero `errors_count` and zero `estimated_recovery_time`, that next query will zero `errors_count` and try to use replica as if it has no errors.

**See also**

-   [Table engine Distributed](../engines/table-engines/special/distributed.md)
-   [distributed\_replica\_error\_cap setting](settings/settings.md#settings-distributed_replica_error_cap)
-   [distributed\_replica\_error\_half\_life setting](settings/settings.md#settings-distributed_replica_error_half_life)

## system.columns {#system-columns}

Contains information about columns in all the tables.

You can use this table to get information similar to the [DESCRIBE TABLE](../sql-reference/statements/misc.md#misc-describe-table) query, but for multiple tables at once.

The `system.columns` table contains the following columns (the column type is shown in brackets):

-   `database` (String) — Database name.
-   `table` (String) — Table name.
-   `name` (String) — Column name.
-   `type` (String) — Column type.
-   `default_kind` (String) — Expression type (`DEFAULT`, `MATERIALIZED`, `ALIAS`) for the default value, or an empty string if it is not defined.
-   `default_expression` (String) — Expression for the default value, or an empty string if it is not defined.
-   `data_compressed_bytes` (UInt64) — The size of compressed data, in bytes.
-   `data_uncompressed_bytes` (UInt64) — The size of decompressed data, in bytes.
-   `marks_bytes` (UInt64) — The size of marks, in bytes.
-   `comment` (String) — Comment on the column, or an empty string if it is not defined.
-   `is_in_partition_key` (UInt8) — Flag that indicates whether the column is in the partition expression.
-   `is_in_sorting_key` (UInt8) — Flag that indicates whether the column is in the sorting key expression.
-   `is_in_primary_key` (UInt8) — Flag that indicates whether the column is in the primary key expression.
-   `is_in_sampling_key` (UInt8) — Flag that indicates whether the column is in the sampling key expression.

## system.contributors {#system-contributors}

Contains information about contributors. All constributors in random order. The order is random at query execution time.

Columns:

-   `name` (String) — Contributor (author) name from git log.

**Example**

``` sql
SELECT * FROM system.contributors LIMIT 10
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
│ Max Vetrov       │
│ LiuYangkuan      │
│ svladykin        │
│ zamulla          │
│ Šimon Podlipský  │
│ BayoNet          │
│ Ilya Khomutov    │
│ Amy Krishnevsky  │
│ Loud_Scream      │
└──────────────────┘
```

To find out yourself in the table, use a query:

``` sql
SELECT * FROM system.contributors WHERE name='Olga Khvostikova'
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
└──────────────────┘
```

## system.databases {#system-databases}

This table contains a single String column called ‘name’ – the name of a database.
Each database that the server knows about has a corresponding entry in the table.
This system table is used for implementing the `SHOW DATABASES` query.

## system.detached\_parts {#system_tables-detached_parts}

Contains information about detached parts of [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) tables. The `reason` column specifies why the part was detached. For user-detached parts, the reason is empty. Such parts can be attached with [ALTER TABLE ATTACH PARTITION\|PART](../sql-reference/statements/alter.md#alter_attach-partition) command. For the description of other columns, see [system.parts](#system_tables-parts). If part name is invalid, values of some columns may be `NULL`. Such parts can be deleted with [ALTER TABLE DROP DETACHED PART](../sql-reference/statements/alter.md#alter_drop-detached).

## system.dictionaries {#system_tables-dictionaries}

Contains information about [external dictionaries](../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

Columns:

-   `database` ([String](../sql-reference/data-types/string.md)) — Name of the database containing the dictionary created by DDL query. Empty string for other dictionaries.
-   `name` ([String](../sql-reference/data-types/string.md)) — [Dictionary name](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict.md).
-   `status` ([Enum8](../sql-reference/data-types/enum.md)) — Dictionary status. Possible values:
    -   `NOT_LOADED` — Dictionary was not loaded because it was not used.
    -   `LOADED` — Dictionary loaded successfully.
    -   `FAILED` — Unable to load the dictionary as a result of an error.
    -   `LOADING` — Dictionary is loading now.
    -   `LOADED_AND_RELOADING` — Dictionary is loaded successfully, and is being reloaded right now (frequent reasons: [SYSTEM RELOAD DICTIONARY](../sql-reference/statements/system.md#query_language-system-reload-dictionary) query, timeout, dictionary config has changed).
    -   `FAILED_AND_RELOADING` — Could not load the dictionary as a result of an error and is loading now.
-   `origin` ([String](../sql-reference/data-types/string.md)) — Path to the configuration file that describes the dictionary.
-   `type` ([String](../sql-reference/data-types/string.md)) — Type of a dictionary allocation. [Storing Dictionaries in Memory](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md).
-   `key` — [Key type](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-key): Numeric Key ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) or Сomposite key ([String](../sql-reference/data-types/string.md)) — form “(type 1, type 2, …, type n)”.
-   `attribute.names` ([Array](../sql-reference/data-types/array.md)([String](../sql-reference/data-types/string.md))) — Array of [attribute names](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) provided by the dictionary.
-   `attribute.types` ([Array](../sql-reference/data-types/array.md)([String](../sql-reference/data-types/string.md))) — Corresponding array of [attribute types](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) that are provided by the dictionary.
-   `bytes_allocated` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Amount of RAM allocated for the dictionary.
-   `query_count` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of queries since the dictionary was loaded or since the last successful reboot.
-   `hit_rate` ([Float64](../sql-reference/data-types/float.md)) — For cache dictionaries, the percentage of uses for which the value was in the cache.
-   `element_count` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of items stored in the dictionary.
-   `load_factor` ([Float64](../sql-reference/data-types/float.md)) — Percentage filled in the dictionary (for a hashed dictionary, the percentage filled in the hash table).
-   `source` ([String](../sql-reference/data-types/string.md)) — Text describing the [data source](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md) for the dictionary.
-   `lifetime_min` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Minimum [lifetime](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) of the dictionary in memory, after which ClickHouse tries to reload the dictionary (if `invalidate_query` is set, then only if it has changed). Set in seconds.
-   `lifetime_max` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Maximum [lifetime](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) of the dictionary in memory, after which ClickHouse tries to reload the dictionary (if `invalidate_query` is set, then only if it has changed). Set in seconds.
-   `loading_start_time` ([DateTime](../sql-reference/data-types/datetime.md)) — Start time for loading the dictionary.
-   `last_successful_update_time` ([DateTime](../sql-reference/data-types/datetime.md)) — End time for loading or updating the dictionary. Helps to monitor some troubles with external sources and investigate causes.
-   `loading_duration` ([Float32](../sql-reference/data-types/float.md)) — Duration of a dictionary loading.
-   `last_exception` ([String](../sql-reference/data-types/string.md)) — Text of the error that occurs when creating or reloading the dictionary if the dictionary couldn’t be created.

**Example**

Configure the dictionary.

``` sql
CREATE DICTIONARY dictdb.dict
(
    `key` Int64 DEFAULT -1,
    `value_default` String DEFAULT 'world',
    `value_expression` String DEFAULT 'xxx' EXPRESSION 'toString(127 * 172)'
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'dicttbl' DB 'dictdb'))
LIFETIME(MIN 0 MAX 1)
LAYOUT(FLAT())
```

Make sure that the dictionary is loaded.

``` sql
SELECT * FROM system.dictionaries
```

``` text
┌─database─┬─name─┬─status─┬─origin──────┬─type─┬─key────┬─attribute.names──────────────────────┬─attribute.types─────┬─bytes_allocated─┬─query_count─┬─hit_rate─┬─element_count─┬───────────load_factor─┬─source─────────────────────┬─lifetime_min─┬─lifetime_max─┬──loading_start_time─┌──last_successful_update_time─┬──────loading_duration─┬─last_exception─┐
│ dictdb   │ dict │ LOADED │ dictdb.dict │ Flat │ UInt64 │ ['value_default','value_expression'] │ ['String','String'] │           74032 │           0 │        1 │             1 │ 0.0004887585532746823 │ ClickHouse: dictdb.dicttbl │            0 │            1 │ 2020-03-04 04:17:34 │   2020-03-04 04:30:34        │                 0.002 │                │
└──────────┴──────┴────────┴─────────────┴──────┴────────┴──────────────────────────────────────┴─────────────────────┴─────────────────┴─────────────┴──────────┴───────────────┴───────────────────────┴────────────────────────────┴──────────────┴──────────────┴─────────────────────┴──────────────────────────────┘───────────────────────┴────────────────┘
```

## system.events {#system_tables-events}

Contains information about the number of events that have occurred in the system. For example, in the table, you can find how many `SELECT` queries were processed since the ClickHouse server started.

Columns:

-   `event` ([String](../sql-reference/data-types/string.md)) — Event name.
-   `value` ([UInt64](../sql-reference/data-types/int-uint.md)) — Number of events occurred.
-   `description` ([String](../sql-reference/data-types/string.md)) — Event description.

**Example**

``` sql
SELECT * FROM system.events LIMIT 5
```

``` text
┌─event─────────────────────────────────┬─value─┬─description────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Query                                 │    12 │ Number of queries to be interpreted and potentially executed. Does not include queries that failed to parse or were rejected due to AST size limits, quota limits or limits on the number of simultaneously running queries. May include internal queries initiated by ClickHouse itself. Does not count subqueries.                  │
│ SelectQuery                           │     8 │ Same as Query, but only for SELECT queries.                                                                                                                                                                                                                │
│ FileOpen                              │    73 │ Number of files opened.                                                                                                                                                                                                                                    │
│ ReadBufferFromFileDescriptorRead      │   155 │ Number of reads (read/pread) from a file descriptor. Does not include sockets.                                                                                                                                                                             │
│ ReadBufferFromFileDescriptorReadBytes │  9931 │ Number of bytes read from file descriptors. If the file is compressed, this will show the compressed data size.                                                                                                                                              │
└───────────────────────────────────────┴───────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**See Also**

-   [system.asynchronous\_metrics](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [system.metrics](#system_tables-metrics) — Contains instantly calculated metrics.
-   [system.metric\_log](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.
-   [Monitoring](monitoring.md) — Base concepts of ClickHouse monitoring.

## system.functions {#system-functions}

Contains information about normal and aggregate functions.

Columns:

-   `name`(`String`) – The name of the function.
-   `is_aggregate`(`UInt8`) — Whether the function is aggregate.

## system.graphite\_retentions {#system-graphite-retentions}

Contains information about parameters [graphite\_rollup](server-configuration-parameters/settings.md#server_configuration_parameters-graphite) which are used in tables with [\*GraphiteMergeTree](../engines/table-engines/mergetree-family/graphitemergetree.md) engines.

Columns:

-   `config_name` (String) - `graphite_rollup` parameter name.
-   `regexp` (String) - A pattern for the metric name.
-   `function` (String) - The name of the aggregating function.
-   `age` (UInt64) - The minimum age of the data in seconds.
-   `precision` (UInt64) - How precisely to define the age of the data in seconds.
-   `priority` (UInt16) - Pattern priority.
-   `is_default` (UInt8) - Whether the pattern is the default.
-   `Tables.database` (Array(String)) - Array of names of database tables that use the `config_name` parameter.
-   `Tables.table` (Array(String)) - Array of table names that use the `config_name` parameter.

## system.merges {#system-merges}

Contains information about merges and part mutations currently in process for tables in the MergeTree family.

Columns:

-   `database` (String) — The name of the database the table is in.
-   `table` (String) — Table name.
-   `elapsed` (Float64) — The time elapsed (in seconds) since the merge started.
-   `progress` (Float64) — The percentage of completed work from 0 to 1.
-   `num_parts` (UInt64) — The number of pieces to be merged.
-   `result_part_name` (String) — The name of the part that will be formed as the result of merging.
-   `is_mutation` (UInt8) - 1 if this process is a part mutation.
-   `total_size_bytes_compressed` (UInt64) — The total size of the compressed data in the merged chunks.
-   `total_size_marks` (UInt64) — The total number of marks in the merged parts.
-   `bytes_read_uncompressed` (UInt64) — Number of bytes read, uncompressed.
-   `rows_read` (UInt64) — Number of rows read.
-   `bytes_written_uncompressed` (UInt64) — Number of bytes written, uncompressed.
-   `rows_written` (UInt64) — Number of rows written.

## system.metrics {#system_tables-metrics}

Contains metrics which can be calculated instantly, or have a current value. For example, the number of simultaneously processed queries or the current replica delay. This table is always up to date.

Columns:

-   `metric` ([String](../sql-reference/data-types/string.md)) — Metric name.
-   `value` ([Int64](../sql-reference/data-types/int-uint.md)) — Metric value.
-   `description` ([String](../sql-reference/data-types/string.md)) — Metric description.

The list of supported metrics you can find in the [src/Common/CurrentMetrics.cpp](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/CurrentMetrics.cpp) source file of ClickHouse.

**Example**

``` sql
SELECT * FROM system.metrics LIMIT 10
```

``` text
┌─metric─────────────────────┬─value─┬─description──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Query                      │     1 │ Number of executing queries                                                                                                                                                                      │
│ Merge                      │     0 │ Number of executing background merges                                                                                                                                                            │
│ PartMutation               │     0 │ Number of mutations (ALTER DELETE/UPDATE)                                                                                                                                                        │
│ ReplicatedFetch            │     0 │ Number of data parts being fetched from replicas                                                                                                                                                │
│ ReplicatedSend             │     0 │ Number of data parts being sent to replicas                                                                                                                                                      │
│ ReplicatedChecks           │     0 │ Number of data parts checking for consistency                                                                                                                                                    │
│ BackgroundPoolTask         │     0 │ Number of active tasks in BackgroundProcessingPool (merges, mutations, fetches, or replication queue bookkeeping)                                                                                │
│ BackgroundSchedulePoolTask │     0 │ Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old data parts, altering data parts, replica re-initialization, etc.   │
│ DiskSpaceReservedForMerge  │     0 │ Disk space reserved for currently running background merges. It is slightly more than the total size of currently merging parts.                                                                     │
│ DistributedSend            │     0 │ Number of connections to remote servers sending data that was INSERTed into Distributed tables. Both synchronous and asynchronous mode.                                                          │
└────────────────────────────┴───────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**See Also**

-   [system.asynchronous\_metrics](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [system.events](#system_tables-events) — Contains a number of events that occurred.
-   [system.metric\_log](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.
-   [Monitoring](monitoring.md) — Base concepts of ClickHouse monitoring.

## system.metric\_log {#system_tables-metric_log}

Contains history of metrics values from tables `system.metrics` and `system.events`, periodically flushed to disk.
To turn on metrics history collection on `system.metric_log`, create `/etc/clickhouse-server/config.d/metric_log.xml` with following content:

``` xml
<yandex>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
    </metric_log>
</yandex>
```

**Example**

``` sql
SELECT * FROM system.metric_log LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
event_date:                                                 2020-02-18
event_time:                                                 2020-02-18 07:15:33
milliseconds:                                               554
ProfileEvent_Query:                                         0
ProfileEvent_SelectQuery:                                   0
ProfileEvent_InsertQuery:                                   0
ProfileEvent_FileOpen:                                      0
ProfileEvent_Seek:                                          0
ProfileEvent_ReadBufferFromFileDescriptorRead:              1
ProfileEvent_ReadBufferFromFileDescriptorReadFailed:        0
ProfileEvent_ReadBufferFromFileDescriptorReadBytes:         0
ProfileEvent_WriteBufferFromFileDescriptorWrite:            1
ProfileEvent_WriteBufferFromFileDescriptorWriteFailed:      0
ProfileEvent_WriteBufferFromFileDescriptorWriteBytes:       56
...
CurrentMetric_Query:                                        0
CurrentMetric_Merge:                                        0
CurrentMetric_PartMutation:                                 0
CurrentMetric_ReplicatedFetch:                              0
CurrentMetric_ReplicatedSend:                               0
CurrentMetric_ReplicatedChecks:                             0
...
```

**See also**

-   [system.asynchronous\_metrics](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [system.events](#system_tables-events) — Contains a number of events that occurred.
-   [system.metrics](#system_tables-metrics) — Contains instantly calculated metrics.
-   [Monitoring](monitoring.md) — Base concepts of ClickHouse monitoring.

## system.numbers {#system-numbers}

This table contains a single UInt64 column named ‘number’ that contains almost all the natural numbers starting from zero.
You can use this table for tests, or if you need to do a brute force search.
Reads from this table are not parallelized.

## system.numbers\_mt {#system-numbers-mt}

The same as ‘system.numbers’ but reads are parallelized. The numbers can be returned in any order.
Used for tests.

## system.one {#system-one}

This table contains a single row with a single ‘dummy’ UInt8 column containing the value 0.
This table is used if a SELECT query doesn’t specify the FROM clause.
This is similar to the DUAL table found in other DBMSs.

## system.parts {#system_tables-parts}

Contains information about parts of [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) tables.

Each row describes one data part.

Columns:

-   `partition` (String) – The partition name. To learn what a partition is, see the description of the [ALTER](../sql-reference/statements/alter.md#query_language_queries_alter) query.

    Formats:

    -   `YYYYMM` for automatic partitioning by month.
    -   `any_string` when partitioning manually.

-   `name` (`String`) – Name of the data part.

-   `active` (`UInt8`) – Flag that indicates whether the data part is active. If a data part is active, it’s used in a table. Otherwise, it’s deleted. Inactive data parts remain after merging.

-   `marks` (`UInt64`) – The number of marks. To get the approximate number of rows in a data part, multiply `marks` by the index granularity (usually 8192) (this hint doesn’t work for adaptive granularity).

-   `rows` (`UInt64`) – The number of rows.

-   `bytes_on_disk` (`UInt64`) – Total size of all the data part files in bytes.

-   `data_compressed_bytes` (`UInt64`) – Total size of compressed data in the data part. All the auxiliary files (for example, files with marks) are not included.

-   `data_uncompressed_bytes` (`UInt64`) – Total size of uncompressed data in the data part. All the auxiliary files (for example, files with marks) are not included.

-   `marks_bytes` (`UInt64`) – The size of the file with marks.

-   `modification_time` (`DateTime`) – The time the directory with the data part was modified. This usually corresponds to the time of data part creation.\|

-   `remove_time` (`DateTime`) – The time when the data part became inactive.

-   `refcount` (`UInt32`) – The number of places where the data part is used. A value greater than 2 indicates that the data part is used in queries or merges.

-   `min_date` (`Date`) – The minimum value of the date key in the data part.

-   `max_date` (`Date`) – The maximum value of the date key in the data part.

-   `min_time` (`DateTime`) – The minimum value of the date and time key in the data part.

-   `max_time`(`DateTime`) – The maximum value of the date and time key in the data part.

-   `partition_id` (`String`) – ID of the partition.

-   `min_block_number` (`UInt64`) – The minimum number of data parts that make up the current part after merging.

-   `max_block_number` (`UInt64`) – The maximum number of data parts that make up the current part after merging.

-   `level` (`UInt32`) – Depth of the merge tree. Zero means that the current part was created by insert rather than by merging other parts.

-   `data_version` (`UInt64`) – Number that is used to determine which mutations should be applied to the data part (mutations with a version higher than `data_version`).

-   `primary_key_bytes_in_memory` (`UInt64`) – The amount of memory (in bytes) used by primary key values.

-   `primary_key_bytes_in_memory_allocated` (`UInt64`) – The amount of memory (in bytes) reserved for primary key values.

-   `is_frozen` (`UInt8`) – Flag that shows that a partition data backup exists. 1, the backup exists. 0, the backup doesn’t exist. For more details, see [FREEZE PARTITION](../sql-reference/statements/alter.md#alter_freeze-partition)

-   `database` (`String`) – Name of the database.

-   `table` (`String`) – Name of the table.

-   `engine` (`String`) – Name of the table engine without parameters.

-   `path` (`String`) – Absolute path to the folder with data part files.

-   `disk` (`String`) – Name of a disk that stores the data part.

-   `hash_of_all_files` (`String`) – [sipHash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) of compressed files.

-   `hash_of_uncompressed_files` (`String`) – [sipHash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) of uncompressed files (files with marks, index file etc.).

-   `uncompressed_hash_of_compressed_files` (`String`) – [sipHash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) of data in the compressed files as if they were uncompressed.

-   `bytes` (`UInt64`) – Alias for `bytes_on_disk`.

-   `marks_size` (`UInt64`) – Alias for `marks_bytes`.

## system.part\_log {#system_tables-part-log}

The `system.part_log` table is created only if the [part\_log](server-configuration-parameters/settings.md#server_configuration_parameters-part-log) server setting is specified.

This table contains information about events that occurred with [data parts](../engines/table-engines/mergetree-family/custom-partitioning-key.md) in the [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) family tables, such as adding or merging data.

The `system.part_log` table contains the following columns:

-   `event_type` (Enum) — Type of the event that occurred with the data part. Can have one of the following values:
    -   `NEW_PART` — Inserting of a new data part.
    -   `MERGE_PARTS` — Merging of data parts.
    -   `DOWNLOAD_PART` — Downloading a data part.
    -   `REMOVE_PART` — Removing or detaching a data part using [DETACH PARTITION](../sql-reference/statements/alter.md#alter_detach-partition).
    -   `MUTATE_PART` — Mutating of a data part.
    -   `MOVE_PART` — Moving the data part from the one disk to another one.
-   `event_date` (Date) — Event date.
-   `event_time` (DateTime) — Event time.
-   `duration_ms` (UInt64) — Duration.
-   `database` (String) — Name of the database the data part is in.
-   `table` (String) — Name of the table the data part is in.
-   `part_name` (String) — Name of the data part.
-   `partition_id` (String) — ID of the partition that the data part was inserted to. The column takes the ‘all’ value if the partitioning is by `tuple()`.
-   `rows` (UInt64) — The number of rows in the data part.
-   `size_in_bytes` (UInt64) — Size of the data part in bytes.
-   `merged_from` (Array(String)) — An array of names of the parts which the current part was made up from (after the merge).
-   `bytes_uncompressed` (UInt64) — Size of uncompressed bytes.
-   `read_rows` (UInt64) — The number of rows was read during the merge.
-   `read_bytes` (UInt64) — The number of bytes was read during the merge.
-   `error` (UInt16) — The code number of the occurred error.
-   `exception` (String) — Text message of the occurred error.

The `system.part_log` table is created after the first inserting data to the `MergeTree` table.

## system.processes {#system_tables-processes}

This system table is used for implementing the `SHOW PROCESSLIST` query.

Columns:

-   `user` (String) – The user who made the query. Keep in mind that for distributed processing, queries are sent to remote servers under the `default` user. The field contains the username for a specific query, not for a query that this query initiated.
-   `address` (String) – The IP address the request was made from. The same for distributed processing. To track where a distributed query was originally made from, look at `system.processes` on the query requestor server.
-   `elapsed` (Float64) – The time in seconds since request execution started.
-   `rows_read` (UInt64) – The number of rows read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.
-   `bytes_read` (UInt64) – The number of uncompressed bytes read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.
-   `total_rows_approx` (UInt64) – The approximation of the total number of rows that should be read. For distributed processing, on the requestor server, this is the total for all remote servers. It can be updated during request processing, when new sources to process become known.
-   `memory_usage` (UInt64) – Amount of RAM the request uses. It might not include some types of dedicated memory. See the [max\_memory\_usage](../operations/settings/query-complexity.md#settings_max_memory_usage) setting.
-   `query` (String) – The query text. For `INSERT`, it doesn’t include the data to insert.
-   `query_id` (String) – Query ID, if defined.

## system.text\_log {#system-tables-text-log}

Contains logging entries. Logging level which goes to this table can be limited with `text_log.level` server setting.

Columns:

-   `event_date` (Date) — Date of the entry.
-   `event_time` (DateTime) — Time of the entry.
-   `microseconds` (UInt32) — Microseconds of the entry.
-   `thread_name` (String) — Name of the thread from which the logging was done.
-   `thread_id` (UInt64) — OS thread ID.
-   `level` (`Enum8`) — Entry level. Possible values:
    -   `1` or `'Fatal'`.
    -   `2` or `'Critical'`.
    -   `3` or `'Error'`.
    -   `4` or `'Warning'`.
    -   `5` or `'Notice'`.
    -   `6` or `'Information'`.
    -   `7` or `'Debug'`.
    -   `8` or `'Trace'`.
-   `query_id` (String) — ID of the query.
-   `logger_name` (LowCardinality(String)) — Name of the logger (i.e. `DDLWorker`).
-   `message` (String) — The message itself.
-   `revision` (UInt32) — ClickHouse revision.
-   `source_file` (LowCardinality(String)) — Source file from which the logging was done.
-   `source_line` (UInt64) — Source line from which the logging was done.

## system.query_log {#system_tables-query_log}

Contains information about executed queries, for example, start time, duration of processing, error messages.

!!! note "Note"
    The table doesn’t contain input data for `INSERT` queries.

You can change settings of queries logging in the [query_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) section of the server configuration.

You can disable queries logging by setting [log_queries = 0](settings/settings.md#settings-log-queries). We don't recommend to turn off logging because information in this table is important for solving issues.

The flushing period of logs is set in `flush_interval_milliseconds` parameter of the [query_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) server settings section. To force flushing logs, use the [SYSTEM FLUSH LOGS](../sql-reference/statements/system.md#query_language-system-flush_logs) query.

ClickHouse doesn't delete logs from the table automatically. See [Introduction](#system-tables-introduction) for more details.

The `system.query_log` table registers two kinds of queries:

1.  Initial queries that were run directly by the client.
2.  Child queries that were initiated by other queries (for distributed query execution). For these types of queries, information about the parent queries is shown in the `initial_*` columns.

Each query creates one or two rows in the `query_log` table, depending on the status (see the `type` column) of the query:

1.  If the query execution was successful, two rows with the `QueryStart` and `QueryFinish` types are created .
2.  If an error occurred during query processing, two events with the `QueryStart` and `ExceptionWhileProcessing` types are created .
3.  If an error occurred before launching the query, a single event with the `ExceptionBeforeStart` type is created.

Columns:

-   `type` ([Enum8](../sql-reference/data-types/enum.md)) — Type of an event that occurred when executing the query. Values:
    -   `'QueryStart' = 1` — Successful start of query execution.
    -   `'QueryFinish' = 2` — Successful end of query execution.
    -   `'ExceptionBeforeStart' = 3` — Exception before the start of query execution.
    -   `'ExceptionWhileProcessing' = 4` — Exception during the query execution.
-   `event_date` ([Date](../sql-reference/data-types/date.md)) — Query starting date.
-   `event_time` ([DateTime](../sql-reference/data-types/datetime.md)) — Query starting time.
-   `query_start_time` ([DateTime](../sql-reference/data-types/datetime.md)) — Start time of query execution.
-   `query_duration_ms` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Duration of query execution in milliseconds.
-   `read_rows` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Total number or rows read from all tables and table functions participated in query. It includes usual subqueries, subqueries for `IN` and `JOIN`. For distributed queries `read_rows` includes the total number of rows read at all replicas. Each replica sends it's `read_rows` value, and the server-initiator of the query summarize all received and local values. The cache volumes doesn't affect this value.
-   `read_bytes` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Total number or bytes read from all tables and table functions participated in query. It includes usual subqueries, subqueries for `IN` and `JOIN`. For distributed queries `read_bytes` includes the total number of rows read at all replicas. Each replica sends it's `read_bytes` value, and the server-initiator of the query summarize all received and local values. The cache volumes doesn't affect this value.
-   `written_rows` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — For `INSERT` queries, the number of written rows. For other queries, the column value is 0.
-   `written_bytes` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — For `INSERT` queries, the number of written bytes. For other queries, the column value is 0.
-   `result_rows` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of rows in a result of the `SELECT` query, or a number of rows in the `INSERT` query.
-   `result_bytes` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — RAM volume in bytes used to store a query result.
-   `memory_usage` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Memory consumption by the query.
-   `query` ([String](../sql-reference/data-types/string.md)) — Query string.
-   `exception` ([String](../sql-reference/data-types/string.md)) — Exception message.
-   `exception_code` ([Int32](../sql-reference/data-types/int-uint.md)) — Code of an exception. 
-   `stack_trace` ([String](../sql-reference/data-types/string.md)) — [Stack trace](https://en.wikipedia.org/wiki/Stack_trace). An empty string, if the query was completed successfully.
-   `is_initial_query` ([UInt8](../sql-reference/data-types/int-uint.md)) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query as part of distributed query execution.
-   `user` ([String](../sql-reference/data-types/string.md)) — Name of the user who initiated the current query.
-   `query_id` ([String](../sql-reference/data-types/string.md)) — ID of the query.
-   `address` ([IPv6](../sql-reference/data-types/domains/ipv6.md)) — IP address that was used to make the query.
-   `port` ([UInt16](../sql-reference/data-types/int-uint.md)) — The client port that was used to make the query.
-   `initial_user` ([String](../sql-reference/data-types/string.md)) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` ([String](../sql-reference/data-types/string.md)) — ID of the initial query (for distributed query execution).
-   `initial_address` ([IPv6](../sql-reference/data-types/domains/ipv6.md)) — IP address that the parent query was launched from.
-   `initial_port` ([UInt16](../sql-reference/data-types/int-uint.md)) — The client port that was used to make the parent query.
-   `interface` ([UInt8](../sql-reference/data-types/int-uint.md)) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` ([String](../sql-reference/data-types/string.md)) — Operating system username who runs [clickhouse-client](../interfaces/cli.md).
-   `client_hostname` ([String](../sql-reference/data-types/string.md)) — Hostname of the client machine where the [clickhouse-client](../interfaces/cli.md) or another TCP client is run.
-   `client_name` ([String](../sql-reference/data-types/string.md)) — The [clickhouse-client](../interfaces/cli.md) or another TCP client name.
-   `client_revision` ([UInt32](../sql-reference/data-types/int-uint.md)) — Revision of the [clickhouse-client](../interfaces/cli.md) or another TCP client.
-   `client_version_major` ([UInt32](../sql-reference/data-types/int-uint.md)) — Major version of the [clickhouse-client](../interfaces/cli.md) or another TCP client.
-   `client_version_minor` ([UInt32](../sql-reference/data-types/int-uint.md)) — Minor version of the [clickhouse-client](../interfaces/cli.md) or another TCP client.
-   `client_version_patch` ([UInt32](../sql-reference/data-types/int-uint.md)) — Patch component of the [clickhouse-client](../interfaces/cli.md) or another TCP client version.
-   `http_method` (UInt8) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` method was used.
    -   2 — `POST` method was used.
-   `http_user_agent` ([String](../sql-reference/data-types/string.md)) — The `UserAgent` header passed in the HTTP request.
-   `quota_key` ([String](../sql-reference/data-types/string.md)) — The “quota key” specified in the [quotas](quotas.md) setting (see `keyed`).
-   `revision` ([UInt32](../sql-reference/data-types/int-uint.md)) — ClickHouse revision.
-   `thread_numbers` ([Array(UInt32)](../sql-reference/data-types/array.md)) — Number of threads that are participating in query execution.
-   `ProfileEvents.Names` ([Array(String)](../sql-reference/data-types/array.md)) — Counters that measure different metrics. The description of them could be found in the table [system.events](#system_tables-events)
-   `ProfileEvents.Values` ([Array(UInt64)](../sql-reference/data-types/array.md)) — Values of metrics that are listed in the `ProfileEvents.Names` column.
-   `Settings.Names` ([Array(String)](../sql-reference/data-types/array.md)) — Names of settings that were changed when the client ran the query. To enable logging changes to settings, set the `log_query_settings` parameter to 1.
-   `Settings.Values` ([Array(String)](../sql-reference/data-types/array.md)) — Values of settings that are listed in the `Settings.Names` column.

**Example**

``` sql
SELECT * FROM system.query_log LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
type:                 QueryStart
event_date:           2020-05-13
event_time:           2020-05-13 14:02:28
query_start_time:     2020-05-13 14:02:28
query_duration_ms:    0
read_rows:            0
read_bytes:           0
written_rows:         0
written_bytes:        0
result_rows:          0
result_bytes:         0
memory_usage:         0
query:                SELECT 1
exception_code:       0
exception:
stack_trace:
is_initial_query:     1
user:                 default
query_id:             5e834082-6f6d-4e34-b47b-cd1934f4002a
address:              ::ffff:127.0.0.1
port:                 57720
initial_user:         default
initial_query_id:     5e834082-6f6d-4e34-b47b-cd1934f4002a
initial_address:      ::ffff:127.0.0.1
initial_port:         57720
interface:            1
os_user:              bayonet
client_hostname:      clickhouse.ru-central1.internal
client_name:          ClickHouse client
client_revision:      54434
client_version_major: 20
client_version_minor: 4
client_version_patch: 1
http_method:          0
http_user_agent:
quota_key:
revision:             54434
thread_ids:           []
ProfileEvents.Names:  []
ProfileEvents.Values: []
Settings.Names:       ['use_uncompressed_cache','load_balancing','log_queries','max_memory_usage']
Settings.Values:      ['0','random','1','10000000000']

```
**See Also**

-   [system.query_thread_log](#system_tables-query_thread_log) — This table contains information about each query execution thread.

## system.query_thread_log {#system_tables-query_thread_log}

The table contains information about each query execution thread.

ClickHouse creates this table only if the [query\_thread\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query_thread_log) server parameter is specified. This parameter sets the logging rules, such as the logging interval or the name of the table the queries will be logged in. 

!!! note "Note"
    The storage period for logs is unlimited. Logs aren’t automatically deleted from the table. You need to organize the removal of outdated logs yourself.

To enable query logging, set the [log\_query\_threads](settings/settings.md#settings-log-query-threads) parameter to 1. For details, see the [Settings](settings/settings.md) section. 

The flushing period of logs is set in `flush_interval_milliseconds` parameter of the [query\_thread\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query_thread_log) server settings section. To force flushing logs, use the [SYSTEM FLUSH LOGS](../sql-reference/statements/system.md#query_language-system-flush_logs) query.

An arbitrary partitioning key parameter is set in the `system.query_thread_log` table of the [query\_thread\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) server setting. Use the `partition_by` parameter for setting it.

ClickHouse doesn't delete logs from the table automatically. See [Introduction](#system-tables-introduction) for more details.

Columns:

-   `event_date` ([Date](../sql-reference/data-types/date.md)) — the date when the thread has finished execution of the query.
-   `event_time` ([DateTime](../sql-reference/data-types/datetime.md)) — the date and time when the thread has finished execution of the query.
-   `query_start_time` ([DateTime](../sql-reference/data-types/datetime.md)) — Start time of query execution.
-   `query_duration_ms` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Duration of query execution.
-   `read_rows` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of read rows.
-   `read_bytes` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of read bytes.
-   `written_rows` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — For `INSERT` queries, the number of written rows. For other queries, the column value is 0.
-   `written_bytes` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — For `INSERT` queries, the number of written bytes. For other queries, the column value is 0.
-   `memory_usage` ([Int64](../sql-reference/data-types/int-uint.md)) — The difference between the amount of allocated and freed memory in context of this thread.
-   `peak_memory_usage` ([Int64](../sql-reference/data-types/int-uint.md)) — The maximum difference between the amount of allocated and freed memory in context of this thread.
-   `thread_name` ([String](../sql-reference/data-types/string.md)) — Name of the thread.
-   `thread_number` ([UInt32](../sql-reference/data-types/int-uint.md)) — Internal thread ID.
-   `thread_id` ([Int32](../sql-reference/data-types/int-uint.md)) — thread ID.
-   `master_thread_id` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — OS initial ID of initial thread.
-   `query` ([String](../sql-reference/data-types/string.md)) — Query string.
-   `is_initial_query` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query for distributed query execution.
-   `user` ([String](../sql-reference/data-types/string.md)) — Name of the user who initiated the current query.
-   `query_id` ([String](../sql-reference/data-types/string.md)) — ID of the query.
-   `address` ([IPv6](../sql-reference/data-types/domains/ipv6.md)) — IP address that was used to make the query.
-   `port` ([UInt16](../sql-reference/data-types/int-uint.md#uint-ranges)) — The client port that was used to make the query.
-   `initial_user` ([String](../sql-reference/data-types/string.md)) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` ([String](../sql-reference/data-types/string.md)) — ID of the initial query (for distributed query execution).
-   `initial_address` ([IPv6](../sql-reference/data-types/domains/ipv6.md)) — IP address that the parent query was launched from.
-   `initial_port` ([UInt16](../sql-reference/data-types/int-uint.md#uint-ranges)) — The client port that was used to make the parent query.
-   `interface` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` ([String](../sql-reference/data-types/string.md)) — OS’s username who runs [clickhouse-client](../interfaces/cli.md).
-   `client_hostname` ([String](../sql-reference/data-types/string.md)) — Hostname of the client machine where the [clickhouse-client](../interfaces/cli.md) or another TCP client is run.
-   `client_name` ([String](../sql-reference/data-types/string.md)) — The [clickhouse-client](../interfaces/cli.md) or another TCP client name.
-   `client_revision` ([UInt32](../sql-reference/data-types/int-uint.md)) — Revision of the [clickhouse-client](../interfaces/cli.md) or another TCP client.
-   `client_version_major` ([UInt32](../sql-reference/data-types/int-uint.md)) — Major version of the [clickhouse-client](../interfaces/cli.md) or another TCP client.
-   `client_version_minor` ([UInt32](../sql-reference/data-types/int-uint.md)) — Minor version of the [clickhouse-client](../interfaces/cli.md) or another TCP client.
-   `client_version_patch` ([UInt32](../sql-reference/data-types/int-uint.md)) — Patch component of the [clickhouse-client](../interfaces/cli.md) or another TCP client version.
-   `http_method` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` method was used.
    -   2 — `POST` method was used.
-   `http_user_agent` ([String](../sql-reference/data-types/string.md)) — The `UserAgent` header passed in the HTTP request.
-   `quota_key` ([String](../sql-reference/data-types/string.md)) — The “quota key” specified in the [quotas](quotas.md) setting (see `keyed`).
-   `revision` ([UInt32](../sql-reference/data-types/int-uint.md)) — ClickHouse revision.
-   `ProfileEvents.Names` ([Array(String)](../sql-reference/data-types/array.md)) — Counters that measure different metrics for this thread. The description of them could be found in the table [system.events](#system_tables-events)
-   `ProfileEvents.Values` ([Array(UInt64)](../sql-reference/data-types/array.md)) — Values of metrics for this thread that are listed in the `ProfileEvents.Names` column.

**Example**

``` sql
 SELECT * FROM system.query_thread_log LIMIT 1 FORMAT Vertical
```

``` text
Row 1:
──────
event_date:           2020-05-13
event_time:           2020-05-13 14:02:28
query_start_time:     2020-05-13 14:02:28
query_duration_ms:    0
read_rows:            1
read_bytes:           1
written_rows:         0
written_bytes:        0
memory_usage:         0
peak_memory_usage:    0
thread_name:          QueryPipelineEx
thread_id:            28952
master_thread_id:     28924
query:                SELECT 1
is_initial_query:     1
user:                 default
query_id:             5e834082-6f6d-4e34-b47b-cd1934f4002a
address:              ::ffff:127.0.0.1
port:                 57720
initial_user:         default
initial_query_id:     5e834082-6f6d-4e34-b47b-cd1934f4002a
initial_address:      ::ffff:127.0.0.1
initial_port:         57720
interface:            1
os_user:              bayonet
client_hostname:      clickhouse.ru-central1.internal
client_name:          ClickHouse client
client_revision:      54434
client_version_major: 20
client_version_minor: 4
client_version_patch: 1
http_method:          0
http_user_agent:
quota_key:
revision:             54434
ProfileEvents.Names:  ['ContextLock','RealTimeMicroseconds','UserTimeMicroseconds','OSCPUWaitMicroseconds','OSCPUVirtualTimeMicroseconds']
ProfileEvents.Values: [1,97,81,5,81]
...
```

**See Also**

-   [system.query\_log](#system_tables-query_log) — Contains information about an execution of the queries.

## system.trace\_log {#system_tables-trace_log}

Contains stack traces collected by the sampling query profiler.

ClickHouse creates this table when the [trace\_log](server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) server configuration section is set. Also the [query\_profiler\_real\_time\_period\_ns](settings/settings.md#query_profiler_real_time_period_ns) and [query\_profiler\_cpu\_time\_period\_ns](settings/settings.md#query_profiler_cpu_time_period_ns) settings should be set.

To analyze logs, use the `addressToLine`, `addressToSymbol` and `demangle` introspection functions.

Columns:

-   `event_date` ([Date](../sql-reference/data-types/date.md)) — Date of sampling moment.

-   `event_time` ([DateTime](../sql-reference/data-types/datetime.md)) — Timestamp of the sampling moment.

-   `timestamp_ns` ([UInt64](../sql-reference/data-types/int-uint.md)) — Timestamp of the sampling moment in nanoseconds.

-   `revision` ([UInt32](../sql-reference/data-types/int-uint.md)) — ClickHouse server build revision.

    When connecting to server by `clickhouse-client`, you see the string similar to `Connected to ClickHouse server version 19.18.1 revision 54429.`. This field contains the `revision`, but not the `version` of a server.

-   `timer_type` ([Enum8](../sql-reference/data-types/enum.md)) — Timer type:

    -   `Real` represents wall-clock time.
    -   `CPU` represents CPU time.

-   `thread_number` ([UInt32](../sql-reference/data-types/int-uint.md)) — Thread identifier.

-   `query_id` ([String](../sql-reference/data-types/string.md)) — Query identifier that can be used to get details about a query that was running from the [query\_log](#system_tables-query_log) system table.

-   `trace` ([Array(UInt64)](../sql-reference/data-types/array.md)) — Stack trace at the moment of sampling. Each element is a virtual memory address inside ClickHouse server process.

**Example**

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G
```

``` text
Row 1:
──────
event_date:    2019-11-15
event_time:    2019-11-15 15:09:38
revision:      54428
timer_type:    Real
thread_number: 48
query_id:      acc4d61f-5bd1-4a3e-bc91-2180be37c915
trace:         [94222141367858,94222152240175,94222152325351,94222152329944,94222152330796,94222151449980,94222144088167,94222151682763,94222144088167,94222151682763,94222144088167,94222144058283,94222144059248,94222091840750,94222091842302,94222091831228,94222189631488,140509950166747,140509942945935]
```

## system.replicas {#system_tables-replicas}

Contains information and status for replicated tables residing on the local server.
This table can be used for monitoring. The table contains a row for every Replicated\* table.

Example:

``` sql
SELECT *
FROM system.replicas
WHERE table = 'visits'
FORMAT Vertical
```

``` text
Row 1:
──────
database:                   merge
table:                      visits
engine:                     ReplicatedCollapsingMergeTree
is_leader:                  1
can_become_leader:          1
is_readonly:                0
is_session_expired:         0
future_parts:               1
parts_to_check:             0
zookeeper_path:             /clickhouse/tables/01-06/visits
replica_name:               example01-06-1.yandex.ru
replica_path:               /clickhouse/tables/01-06/visits/replicas/example01-06-1.yandex.ru
columns_version:            9
queue_size:                 1
inserts_in_queue:           0
merges_in_queue:            1
part_mutations_in_queue:    0
queue_oldest_time:          2020-02-20 08:34:30
inserts_oldest_time:        0000-00-00 00:00:00
merges_oldest_time:         2020-02-20 08:34:30
part_mutations_oldest_time: 0000-00-00 00:00:00
oldest_part_to_get:
oldest_part_to_merge_to:    20200220_20284_20840_7
oldest_part_to_mutate_to:
log_max_index:              596273
log_pointer:                596274
last_queue_update:          2020-02-20 08:34:32
absolute_delay:             0
total_replicas:             2
active_replicas:            2
```

Columns:

-   `database` (`String`) - Database name
-   `table` (`String`) - Table name
-   `engine` (`String`) - Table engine name
-   `is_leader` (`UInt8`) - Whether the replica is the leader.
    Only one replica at a time can be the leader. The leader is responsible for selecting background merges to perform.
    Note that writes can be performed to any replica that is available and has a session in ZK, regardless of whether it is a leader.
-   `can_become_leader` (`UInt8`) - Whether the replica can be elected as a leader.
-   `is_readonly` (`UInt8`) - Whether the replica is in read-only mode.
    This mode is turned on if the config doesn’t have sections with ZooKeeper, if an unknown error occurred when reinitializing sessions in ZooKeeper, and during session reinitialization in ZooKeeper.
-   `is_session_expired` (`UInt8`) - the session with ZooKeeper has expired. Basically the same as `is_readonly`.
-   `future_parts` (`UInt32`) - The number of data parts that will appear as the result of INSERTs or merges that haven’t been done yet.
-   `parts_to_check` (`UInt32`) - The number of data parts in the queue for verification. A part is put in the verification queue if there is suspicion that it might be damaged.
-   `zookeeper_path` (`String`) - Path to table data in ZooKeeper.
-   `replica_name` (`String`) - Replica name in ZooKeeper. Different replicas of the same table have different names.
-   `replica_path` (`String`) - Path to replica data in ZooKeeper. The same as concatenating ‘zookeeper\_path/replicas/replica\_path’.
-   `columns_version` (`Int32`) - Version number of the table structure. Indicates how many times ALTER was performed. If replicas have different versions, it means some replicas haven’t made all of the ALTERs yet.
-   `queue_size` (`UInt32`) - Size of the queue for operations waiting to be performed. Operations include inserting blocks of data, merges, and certain other actions. It usually coincides with `future_parts`.
-   `inserts_in_queue` (`UInt32`) - Number of inserts of blocks of data that need to be made. Insertions are usually replicated fairly quickly. If this number is large, it means something is wrong.
-   `merges_in_queue` (`UInt32`) - The number of merges waiting to be made. Sometimes merges are lengthy, so this value may be greater than zero for a long time.
-   `part_mutations_in_queue` (`UInt32`) - The number of mutations waiting to be made.
-   `queue_oldest_time` (`DateTime`) - If `queue_size` greater than 0, shows when the oldest operation was added to the queue.
-   `inserts_oldest_time` (`DateTime`) - See `queue_oldest_time`
-   `merges_oldest_time` (`DateTime`) - See `queue_oldest_time`
-   `part_mutations_oldest_time` (`DateTime`) - See `queue_oldest_time`

The next 4 columns have a non-zero value only where there is an active session with ZK.

-   `log_max_index` (`UInt64`) - Maximum entry number in the log of general activity.
-   `log_pointer` (`UInt64`) - Maximum entry number in the log of general activity that the replica copied to its execution queue, plus one. If `log_pointer` is much smaller than `log_max_index`, something is wrong.
-   `last_queue_update` (`DateTime`) - When the queue was updated last time.
-   `absolute_delay` (`UInt64`) - How big lag in seconds the current replica has.
-   `total_replicas` (`UInt8`) - The total number of known replicas of this table.
-   `active_replicas` (`UInt8`) - The number of replicas of this table that have a session in ZooKeeper (i.e., the number of functioning replicas).

If you request all the columns, the table may work a bit slowly, since several reads from ZooKeeper are made for each row.
If you don’t request the last 4 columns (log\_max\_index, log\_pointer, total\_replicas, active\_replicas), the table works quickly.

For example, you can check that everything is working correctly like this:

``` sql
SELECT
    database,
    table,
    is_leader,
    is_readonly,
    is_session_expired,
    future_parts,
    parts_to_check,
    columns_version,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    log_max_index,
    log_pointer,
    total_replicas,
    active_replicas
FROM system.replicas
WHERE
       is_readonly
    OR is_session_expired
    OR future_parts > 20
    OR parts_to_check > 10
    OR queue_size > 20
    OR inserts_in_queue > 10
    OR log_max_index - log_pointer > 10
    OR total_replicas < 2
    OR active_replicas < total_replicas
```

If this query doesn’t return anything, it means that everything is fine.

## system.settings {#system-tables-system-settings}

Contains information about session settings for current user.

Columns:

-   `name` ([String](../sql-reference/data-types/string.md)) — Setting name.
-   `value` ([String](../sql-reference/data-types/string.md)) — Setting value.
-   `changed` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether a setting is changed from its default value.
-   `description` ([String](../sql-reference/data-types/string.md)) — Short setting description.
-   `min` ([Nullable](../sql-reference/data-types/nullable.md)([String](../sql-reference/data-types/string.md))) — Minimum value of the setting, if any is set via [constraints](settings/constraints-on-settings.md#constraints-on-settings). If the setting has no minimum value, contains [NULL](../sql-reference/syntax.md#null-literal).
-   `max` ([Nullable](../sql-reference/data-types/nullable.md)([String](../sql-reference/data-types/string.md))) — Maximum value of the setting, if any is set via [constraints](settings/constraints-on-settings.md#constraints-on-settings). If the setting has no maximum value, contains [NULL](../sql-reference/syntax.md#null-literal).
-   `readonly` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether the current user can change the setting:
    -   `0` — Current user can change the setting.
    -   `1` — Current user can’t change the setting.

**Example**

The following example shows how to get information about settings which name contains `min_i`.

``` sql
SELECT *
FROM system.settings
WHERE name LIKE '%min_i%'
```

``` text
┌─name────────────────────────────────────────┬─value─────┬─changed─┬─description───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─min──┬─max──┬─readonly─┐
│ min_insert_block_size_rows                  │ 1048576   │       0 │ Squash blocks passed to INSERT query to specified size in rows, if blocks are not big enough.                                                                         │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
│ min_insert_block_size_bytes                 │ 268435456 │       0 │ Squash blocks passed to INSERT query to specified size in bytes, if blocks are not big enough.                                                                        │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
│ read_backoff_min_interval_between_events_ms │ 1000      │       0 │ Settings to reduce the number of threads in case of slow reads. Do not pay attention to the event, if the previous one has passed less than a certain amount of time. │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
└─────────────────────────────────────────────┴───────────┴─────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────┴──────┴──────────┘
```

Using of `WHERE changed` can be useful, for example, when you want to check:

-   Whether settings in configuration files are loaded correctly and are in use.
-   Settings that changed in the current session.

<!-- -->

``` sql
SELECT * FROM system.settings WHERE changed AND name='load_balancing'
```

**See also**

-   [Settings](settings/index.md#session-settings-intro)
-   [Permissions for Queries](settings/permissions-for-queries.md#settings_readonly)
-   [Constraints on Settings](settings/constraints-on-settings.md)

## system.table\_engines {#system.table_engines}

``` text
┌─name───────────────────┬─value───────┐
│ max_threads            │ 8           │
│ use_uncompressed_cache │ 0           │
│ load_balancing         │ random      │
│ max_memory_usage       │ 10000000000 │
└────────────────────────┴─────────────┘
```

## system.merge\_tree\_settings {#system-merge_tree_settings}

Contains information about settings for `MergeTree` tables.

Columns:

-   `name` (String) — Setting name.
-   `value` (String) — Setting value.
-   `description` (String) — Setting description.
-   `type` (String) — Setting type (implementation specific string value).
-   `changed` (UInt8) — Whether the setting was explicitly defined in the config or explicitly changed.

## system.table\_engines {#system-table-engines}

Contains description of table engines supported by server and their feature support information.

This table contains the following columns (the column type is shown in brackets):

-   `name` (String) — The name of table engine.
-   `supports_settings` (UInt8) — Flag that indicates if table engine supports `SETTINGS` clause.
-   `supports_skipping_indices` (UInt8) — Flag that indicates if table engine supports [skipping indices](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes).
-   `supports_ttl` (UInt8) — Flag that indicates if table engine supports [TTL](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).
-   `supports_sort_order` (UInt8) — Flag that indicates if table engine supports clauses `PARTITION_BY`, `PRIMARY_KEY`, `ORDER_BY` and `SAMPLE_BY`.
-   `supports_replication` (UInt8) — Flag that indicates if table engine supports [data replication](../engines/table-engines/mergetree-family/replication.md).
-   `supports_duduplication` (UInt8) — Flag that indicates if table engine supports data deduplication.

Example:

``` sql
SELECT *
FROM system.table_engines
WHERE name in ('Kafka', 'MergeTree', 'ReplicatedCollapsingMergeTree')
```

``` text
┌─name──────────────────────────┬─supports_settings─┬─supports_skipping_indices─┬─supports_sort_order─┬─supports_ttl─┬─supports_replication─┬─supports_deduplication─┐
│ Kafka                         │                 1 │                         0 │                   0 │            0 │                    0 │                      0 │
│ MergeTree                     │                 1 │                         1 │                   1 │            1 │                    0 │                      0 │
│ ReplicatedCollapsingMergeTree │                 1 │                         1 │                   1 │            1 │                    1 │                      1 │
└───────────────────────────────┴───────────────────┴───────────────────────────┴─────────────────────┴──────────────┴──────────────────────┴────────────────────────┘
```

**See also**

-   MergeTree family [query clauses](../engines/table-engines/mergetree-family/mergetree.md#mergetree-query-clauses)
-   Kafka [settings](../engines/table-engines/integrations/kafka.md#table_engine-kafka-creating-a-table)
-   Join [settings](../engines/table-engines/special/join.md#join-limitations-and-settings)

## system.tables {#system-tables}

Contains metadata of each table that the server knows about. Detached tables are not shown in `system.tables`.

This table contains the following columns (the column type is shown in brackets):

-   `database` (String) — The name of the database the table is in.

-   `name` (String) — Table name.

-   `engine` (String) — Table engine name (without parameters).

-   `is_temporary` (UInt8) - Flag that indicates whether the table is temporary.

-   `data_path` (String) - Path to the table data in the file system.

-   `metadata_path` (String) - Path to the table metadata in the file system.

-   `metadata_modification_time` (DateTime) - Time of latest modification of the table metadata.

-   `dependencies_database` (Array(String)) - Database dependencies.

-   `dependencies_table` (Array(String)) - Table dependencies ([MaterializedView](../engines/table-engines/special/materializedview.md) tables based on the current table).

-   `create_table_query` (String) - The query that was used to create the table.

-   `engine_full` (String) - Parameters of the table engine.

-   `partition_key` (String) - The partition key expression specified in the table.

-   `sorting_key` (String) - The sorting key expression specified in the table.

-   `primary_key` (String) - The primary key expression specified in the table.

-   `sampling_key` (String) - The sampling key expression specified in the table.

-   `storage_policy` (String) - The storage policy:

    -   [MergeTree](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)
    -   [Distributed](../engines/table-engines/special/distributed.md#distributed)

-   `total_rows` (Nullable(UInt64)) - Total number of rows, if it is possible to quickly determine exact number of rows in the table, otherwise `Null` (including underying `Buffer` table).

-   `total_bytes` (Nullable(UInt64)) - Total number of bytes, if it is possible to quickly determine exact number of bytes for the table on storage, otherwise `Null` (**does not** includes any underlying storage).

    -   If the table stores data on disk, returns used space on disk (i.e. compressed).
    -   If the table stores data in memory, returns approximated number of used bytes in memory.

The `system.tables` table is used in `SHOW TABLES` query implementation.

## system.zookeeper {#system-zookeeper}

The table does not exist if ZooKeeper is not configured. Allows reading data from the ZooKeeper cluster defined in the config.
The query must have a ‘path’ equality condition in the WHERE clause. This is the path in ZooKeeper for the children that you want to get data for.

The query `SELECT * FROM system.zookeeper WHERE path = '/clickhouse'` outputs data for all children on the `/clickhouse` node.
To output data for all root nodes, write path = ‘/’.
If the path specified in ‘path’ doesn’t exist, an exception will be thrown.

Columns:

-   `name` (String) — The name of the node.
-   `path` (String) — The path to the node.
-   `value` (String) — Node value.
-   `dataLength` (Int32) — Size of the value.
-   `numChildren` (Int32) — Number of descendants.
-   `czxid` (Int64) — ID of the transaction that created the node.
-   `mzxid` (Int64) — ID of the transaction that last changed the node.
-   `pzxid` (Int64) — ID of the transaction that last deleted or added descendants.
-   `ctime` (DateTime) — Time of node creation.
-   `mtime` (DateTime) — Time of the last modification of the node.
-   `version` (Int32) — Node version: the number of times the node was changed.
-   `cversion` (Int32) — Number of added or removed descendants.
-   `aversion` (Int32) — Number of changes to the ACL.
-   `ephemeralOwner` (Int64) — For ephemeral nodes, the ID of the session that owns this node.

Example:

``` sql
SELECT *
FROM system.zookeeper
WHERE path = '/clickhouse/tables/01-08/visits/replicas'
FORMAT Vertical
```

``` text
Row 1:
──────
name:           example01-08-1.yandex.ru
value:
czxid:          932998691229
mzxid:          932998691229
ctime:          2015-03-27 16:49:51
mtime:          2015-03-27 16:49:51
version:        0
cversion:       47
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021031383
path:           /clickhouse/tables/01-08/visits/replicas

Row 2:
──────
name:           example01-08-2.yandex.ru
value:
czxid:          933002738135
mzxid:          933002738135
ctime:          2015-03-27 16:57:01
mtime:          2015-03-27 16:57:01
version:        0
cversion:       37
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021252247
path:           /clickhouse/tables/01-08/visits/replicas
```

## system.mutations {#system_tables-mutations}

The table contains information about [mutations](../sql-reference/statements/alter.md#alter-mutations) of MergeTree tables and their progress. Each mutation command is represented by a single row. The table has the following columns:

**database**, **table** - The name of the database and table to which the mutation was applied.

**mutation\_id** - The ID of the mutation. For replicated tables these IDs correspond to znode names in the `<table_path_in_zookeeper>/mutations/` directory in ZooKeeper. For unreplicated tables the IDs correspond to file names in the data directory of the table.

**command** - The mutation command string (the part of the query after `ALTER TABLE [db.]table`).

**create\_time** - When this mutation command was submitted for execution.

**block\_numbers.partition\_id**, **block\_numbers.number** - A nested column. For mutations of replicated tables, it contains one record for each partition: the partition ID and the block number that was acquired by the mutation (in each partition, only parts that contain blocks with numbers less than the block number acquired by the mutation in that partition will be mutated). In non-replicated tables, block numbers in all partitions form a single sequence. This means that for mutations of non-replicated tables, the column will contain one record with a single block number acquired by the mutation.

**parts\_to\_do** - The number of data parts that need to be mutated for the mutation to finish.

**is\_done** - Is the mutation done? Note that even if `parts_to_do = 0` it is possible that a mutation of a replicated table is not done yet because of a long-running INSERT that will create a new data part that will need to be mutated.

If there were problems with mutating some parts, the following columns contain additional information:

**latest\_failed\_part** - The name of the most recent part that could not be mutated.

**latest\_fail\_time** - The time of the most recent part mutation failure.

**latest\_fail\_reason** - The exception message that caused the most recent part mutation failure.

## system.disks {#system_tables-disks}

Contains information about disks defined in the [server configuration](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Columns:

-   `name` ([String](../sql-reference/data-types/string.md)) — Name of a disk in the server configuration.
-   `path` ([String](../sql-reference/data-types/string.md)) — Path to the mount point in the file system.
-   `free_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — Free space on disk in bytes.
-   `total_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — Disk volume in bytes.
-   `keep_free_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — Amount of disk space that should stay free on disk in bytes. Defined in the `keep_free_space_bytes` parameter of disk configuration.

## system.storage\_policies {#system_tables-storage_policies}

Contains information about storage policies and volumes defined in the [server configuration](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Columns:

-   `policy_name` ([String](../sql-reference/data-types/string.md)) — Name of the storage policy.
-   `volume_name` ([String](../sql-reference/data-types/string.md)) — Volume name defined in the storage policy.
-   `volume_priority` ([UInt64](../sql-reference/data-types/int-uint.md)) — Volume order number in the configuration.
-   `disks` ([Array(String)](../sql-reference/data-types/array.md)) — Disk names, defined in the storage policy.
-   `max_data_part_size` ([UInt64](../sql-reference/data-types/int-uint.md)) — Maximum size of a data part that can be stored on volume disks (0 — no limit).
-   `move_factor` ([Float64](../sql-reference/data-types/float.md)) — Ratio of free disk space. When the ratio exceeds the value of configuration parameter, ClickHouse start to move data to the next volume in order.

If the storage policy contains more then one volume, then information for each volume is stored in the individual row of the table.

[Original article](https://clickhouse.tech/docs/en/operations/system_tables/) <!--hide-->
