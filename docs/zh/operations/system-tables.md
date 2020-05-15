---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 52
toc_title: "\u7CFB\u7EDF\u8868"
---

# 系统表 {#system-tables}

系统表用于实现系统的部分功能，并提供对有关系统如何工作的信息的访问。
您无法删除系统表（但可以执行分离）。
系统表没有包含磁盘上数据的文件或包含元数据的文件。 服务器在启动时创建所有系统表。
系统表是只读的。
它们位于 ‘system’ 数据库。

## 系统。asynchronous\_metrics {#system_tables-asynchronous_metrics}

包含在后台定期计算的指标。 例如，在使用的RAM量。

列:

-   `metric` ([字符串](../sql-reference/data-types/string.md)) — Metric name.
-   `value` ([Float64](../sql-reference/data-types/float.md)) — Metric value.

**示例**

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

**另请参阅**

-   [监测](monitoring.md) — Base concepts of ClickHouse monitoring.
-   [系统。指标](#system_tables-metrics) — Contains instantly calculated metrics.
-   [系统。活动](#system_tables-events) — Contains a number of events that have occurred.
-   [系统。metric\_log](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.

## 系统。集群 {#system-clusters}

包含有关配置文件中可用的集群及其中的服务器的信息。

列:

-   `cluster` (String) — The cluster name.
-   `shard_num` (UInt32) — The shard number in the cluster, starting from 1.
-   `shard_weight` (UInt32) — The relative weight of the shard when writing data.
-   `replica_num` (UInt32) — The replica number in the shard, starting from 1.
-   `host_name` (String) — The host name, as specified in the config.
-   `host_address` (String) — The host IP address obtained from DNS.
-   `port` (UInt16) — The port to use for connecting to the server.
-   `user` (String) — The name of the user for connecting to the server.
-   `errors_count` (UInt32)-此主机无法到达副本的次数。
-   `estimated_recovery_time` (UInt32)-剩下的秒数，直到副本错误计数归零，它被认为是恢复正常。

请注意 `errors_count` 每个查询集群更新一次，但 `estimated_recovery_time` 按需重新计算。 所以有可能是非零的情况 `errors_count` 和零 `estimated_recovery_time`，下一个查询将为零 `errors_count` 并尝试使用副本，就好像它没有错误。

**另请参阅**

-   [表引擎分布式](../engines/table-engines/special/distributed.md)
-   [distributed\_replica\_error\_cap设置](settings/settings.md#settings-distributed_replica_error_cap)
-   [distributed\_replica\_error\_half\_life设置](settings/settings.md#settings-distributed_replica_error_half_life)

## 系统。列 {#system-columns}

包含有关所有表中列的信息。

您可以使用此表获取类似于以下内容的信息 [DESCRIBE TABLE](../sql-reference/statements/misc.md#misc-describe-table) 查询，但对于多个表一次。

该 `system.columns` 表包含以下列（列类型显示在括号中):

-   `database` (String) — Database name.
-   `table` (String) — Table name.
-   `name` (String) — Column name.
-   `type` (String) — Column type.
-   `default_kind` (String) — Expression type (`DEFAULT`, `MATERIALIZED`, `ALIAS`)为默认值，如果没有定义，则为空字符串。
-   `default_expression` (String) — Expression for the default value, or an empty string if it is not defined.
-   `data_compressed_bytes` (UInt64) — The size of compressed data, in bytes.
-   `data_uncompressed_bytes` (UInt64) — The size of decompressed data, in bytes.
-   `marks_bytes` (UInt64) — The size of marks, in bytes.
-   `comment` (String) — Comment on the column, or an empty string if it is not defined.
-   `is_in_partition_key` (UInt8) — Flag that indicates whether the column is in the partition expression.
-   `is_in_sorting_key` (UInt8) — Flag that indicates whether the column is in the sorting key expression.
-   `is_in_primary_key` (UInt8) — Flag that indicates whether the column is in the primary key expression.
-   `is_in_sampling_key` (UInt8) — Flag that indicates whether the column is in the sampling key expression.

## 系统。贡献者 {#system-contributors}

包含有关贡献者的信息。 按随机顺序排列所有构造。 该顺序在查询执行时是随机的。

列:

-   `name` (String) — Contributor (author) name from git log.

**示例**

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

要在表中找出自己，请使用查询:

``` sql
SELECT * FROM system.contributors WHERE name='Olga Khvostikova'
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
└──────────────────┘
```

## 系统。数据库 {#system-databases}

此表包含一个名为"字符串"的列 ‘name’ – the name of a database.
服务器知道的每个数据库在表中都有相应的条目。
该系统表用于实现 `SHOW DATABASES` 查询。

## 系统。detached\_parts {#system_tables-detached_parts}

包含有关分离部分的信息 [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) 桌子 该 `reason` 列指定分离部件的原因。 对于用户分离的部件，原因是空的。 这些部件可以附加 [ALTER TABLE ATTACH PARTITION\|PART](../sql-reference/statements/alter.md#alter_attach-partition) 指挥部 有关其他列的说明，请参阅 [系统。零件](#system_tables-parts). 如果部件名称无效，某些列的值可能为 `NULL`. 这些部分可以删除 [ALTER TABLE DROP DETACHED PART](../sql-reference/statements/alter.md#alter_drop-detached).

## 系统。字典 {#system_tables-dictionaries}

包含以下信息 [外部字典](../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

列:

-   `database` ([字符串](../sql-reference/data-types/string.md)) — Name of the database containing the dictionary created by DDL query. Empty string for other dictionaries.
-   `name` ([字符串](../sql-reference/data-types/string.md)) — [字典名称](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict.md).
-   `status` ([枚举8](../sql-reference/data-types/enum.md)) — Dictionary status. Possible values:
    -   `NOT_LOADED` — Dictionary was not loaded because it was not used.
    -   `LOADED` — Dictionary loaded successfully.
    -   `FAILED` — Unable to load the dictionary as a result of an error.
    -   `LOADING` — Dictionary is loading now.
    -   `LOADED_AND_RELOADING` — Dictionary is loaded successfully, and is being reloaded right now (frequent reasons: [SYSTEM RELOAD DICTIONARY](../sql-reference/statements/system.md#query_language-system-reload-dictionary) 查询，超时，字典配置已更改）。
    -   `FAILED_AND_RELOADING` — Could not load the dictionary as a result of an error and is loading now.
-   `origin` ([字符串](../sql-reference/data-types/string.md)) — Path to the configuration file that describes the dictionary.
-   `type` ([字符串](../sql-reference/data-types/string.md)) — Type of a dictionary allocation. [在内存中存储字典](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md).
-   `key` — [密钥类型](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-key):数字键 ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) or Сomposite key ([字符串](../sql-reference/data-types/string.md)) — form “(type 1, type 2, …, type n)”.
-   `attribute.names` ([阵列](../sql-reference/data-types/array.md)([字符串](../sql-reference/data-types/string.md))) — Array of [属性名称](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) 由字典提供。
-   `attribute.types` ([阵列](../sql-reference/data-types/array.md)([字符串](../sql-reference/data-types/string.md))) — Corresponding array of [属性类型](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) 这是由字典提供。
-   `bytes_allocated` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Amount of RAM allocated for the dictionary.
-   `query_count` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of queries since the dictionary was loaded or since the last successful reboot.
-   `hit_rate` ([Float64](../sql-reference/data-types/float.md)) — For cache dictionaries, the percentage of uses for which the value was in the cache.
-   `element_count` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of items stored in the dictionary.
-   `load_factor` ([Float64](../sql-reference/data-types/float.md)) — Percentage filled in the dictionary (for a hashed dictionary, the percentage filled in the hash table).
-   `source` ([字符串](../sql-reference/data-types/string.md)) — Text describing the [数据源](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md) 为了字典
-   `lifetime_min` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Minimum [使用寿命](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) 在内存中的字典，之后ClickHouse尝试重新加载字典（如果 `invalidate_query` 被设置，那么只有当它已经改变）。 在几秒钟内设置。
-   `lifetime_max` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Maximum [使用寿命](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) 在内存中的字典，之后ClickHouse尝试重新加载字典（如果 `invalidate_query` 被设置，那么只有当它已经改变）。 在几秒钟内设置。
-   `loading_start_time` ([日期时间](../sql-reference/data-types/datetime.md)) — Start time for loading the dictionary.
-   `last_successful_update_time` ([日期时间](../sql-reference/data-types/datetime.md)) — End time for loading or updating the dictionary. Helps to monitor some troubles with external sources and investigate causes.
-   `loading_duration` ([Float32](../sql-reference/data-types/float.md)) — Duration of a dictionary loading.
-   `last_exception` ([字符串](../sql-reference/data-types/string.md)) — Text of the error that occurs when creating or reloading the dictionary if the dictionary couldn't be created.

**示例**

配置字典。

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

确保字典已加载。

``` sql
SELECT * FROM system.dictionaries
```

``` text
┌─database─┬─name─┬─status─┬─origin──────┬─type─┬─key────┬─attribute.names──────────────────────┬─attribute.types─────┬─bytes_allocated─┬─query_count─┬─hit_rate─┬─element_count─┬───────────load_factor─┬─source─────────────────────┬─lifetime_min─┬─lifetime_max─┬──loading_start_time─┌──last_successful_update_time─┬──────loading_duration─┬─last_exception─┐
│ dictdb   │ dict │ LOADED │ dictdb.dict │ Flat │ UInt64 │ ['value_default','value_expression'] │ ['String','String'] │           74032 │           0 │        1 │             1 │ 0.0004887585532746823 │ ClickHouse: dictdb.dicttbl │            0 │            1 │ 2020-03-04 04:17:34 │   2020-03-04 04:30:34        │                 0.002 │                │
└──────────┴──────┴────────┴─────────────┴──────┴────────┴──────────────────────────────────────┴─────────────────────┴─────────────────┴─────────────┴──────────┴───────────────┴───────────────────────┴────────────────────────────┴──────────────┴──────────────┴─────────────────────┴──────────────────────────────┘───────────────────────┴────────────────┘
```

## 系统。活动 {#system_tables-events}

包含有关系统中发生的事件数的信息。 例如，在表中，您可以找到多少 `SELECT` 自ClickHouse服务器启动以来已处理查询。

列:

-   `event` ([字符串](../sql-reference/data-types/string.md)) — Event name.
-   `value` ([UInt64](../sql-reference/data-types/int-uint.md)) — Number of events occurred.
-   `description` ([字符串](../sql-reference/data-types/string.md)) — Event description.

**示例**

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

**另请参阅**

-   [系统。asynchronous\_metrics](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [系统。指标](#system_tables-metrics) — Contains instantly calculated metrics.
-   [系统。metric\_log](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.
-   [监测](monitoring.md) — Base concepts of ClickHouse monitoring.

## 系统。功能 {#system-functions}

包含有关正常函数和聚合函数的信息。

列:

-   `name`(`String`) – The name of the function.
-   `is_aggregate`(`UInt8`) — Whether the function is aggregate.

## 系统。graphite\_retentions {#system-graphite-retentions}

包含有关参数的信息 [graphite\_rollup](server-configuration-parameters/settings.md#server_configuration_parameters-graphite) 这是在表中使用 [\*GraphiteMergeTree](../engines/table-engines/mergetree-family/graphitemergetree.md) 引擎

列:

-   `config_name` (字符串) - `graphite_rollup` 参数名称。
-   `regexp` (String)-指标名称的模式。
-   `function` (String)-聚合函数的名称。
-   `age` (UInt64)-以秒为单位的数据的最小期限。
-   `precision` （UInt64）-如何精确地定义以秒为单位的数据的年龄。
-   `priority` (UInt16)-模式优先级。
-   `is_default` (UInt8)-模式是否为默认值。
-   `Tables.database` (Array(String))-使用数据库表名称的数组 `config_name` 参数。
-   `Tables.table` (Array(String))-使用表名称的数组 `config_name` 参数。

## 系统。合并 {#system-merges}

包含有关MergeTree系列中表当前正在进行的合并和部件突变的信息。

列:

-   `database` (String) — The name of the database the table is in.
-   `table` (String) — Table name.
-   `elapsed` (Float64) — The time elapsed (in seconds) since the merge started.
-   `progress` (Float64) — The percentage of completed work from 0 to 1.
-   `num_parts` (UInt64) — The number of pieces to be merged.
-   `result_part_name` (String) — The name of the part that will be formed as the result of merging.
-   `is_mutation` (UInt8)-1如果这个过程是一个部分突变.
-   `total_size_bytes_compressed` (UInt64) — The total size of the compressed data in the merged chunks.
-   `total_size_marks` (UInt64) — The total number of marks in the merged parts.
-   `bytes_read_uncompressed` (UInt64) — Number of bytes read, uncompressed.
-   `rows_read` (UInt64) — Number of rows read.
-   `bytes_written_uncompressed` (UInt64) — Number of bytes written, uncompressed.
-   `rows_written` (UInt64) — Number of rows written.

## 系统。指标 {#system_tables-metrics}

包含可以立即计算或具有当前值的指标。 例如，同时处理的查询的数量或当前副本的延迟。 此表始终是最新的。

列:

-   `metric` ([字符串](../sql-reference/data-types/string.md)) — Metric name.
-   `value` ([Int64](../sql-reference/data-types/int-uint.md)) — Metric value.
-   `description` ([字符串](../sql-reference/data-types/string.md)) — Metric description.

支持的指标列表，您可以在 [src/Common/CurrentMetrics.cpp](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/CurrentMetrics.cpp) ClickHouse的源文件。

**示例**

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

**另请参阅**

-   [系统。asynchronous\_metrics](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [系统。活动](#system_tables-events) — Contains a number of events that occurred.
-   [系统。metric\_log](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.
-   [监测](monitoring.md) — Base concepts of ClickHouse monitoring.

## 系统。metric\_log {#system_tables-metric_log}

包含表中度量值的历史记录 `system.metrics` 和 `system.events`，定期刷新到磁盘。
打开指标历史记录收集 `system.metric_log`,创建 `/etc/clickhouse-server/config.d/metric_log.xml` 具有以下内容:

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

**示例**

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

**另请参阅**

-   [系统。asynchronous\_metrics](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [系统。活动](#system_tables-events) — Contains a number of events that occurred.
-   [系统。指标](#system_tables-metrics) — Contains instantly calculated metrics.
-   [监测](monitoring.md) — Base concepts of ClickHouse monitoring.

## 系统。数字 {#system-numbers}

此表包含一个名为UInt64的列 ‘number’ 它包含几乎所有从零开始的自然数。
您可以使用此表进行测试，或者如果您需要进行暴力搜索。
从此表中读取的内容不是并行的。

## 系统。numbers\_mt {#system-numbers-mt}

一样的 ‘system.numbers’ 但读取是并行的。 这些数字可以以任何顺序返回。
用于测试。

## 系统。一 {#system-one}

此表包含一行，其中包含一行 ‘dummy’ UInt8列包含值0。
如果SELECT查询未指定FROM子句，则使用此表。
这与其他Dbms中的双表类似。

## 系统。零件 {#system_tables-parts}

包含有关的部分信息 [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) 桌子

每行描述一个数据部分。

列:

-   `partition` (String) – The partition name. To learn what a partition is, see the description of the [ALTER](../sql-reference/statements/alter.md#query_language_queries_alter) 查询。

    格式:

    -   `YYYYMM` 用于按月自动分区。
    -   `any_string` 手动分区时。

-   `name` (`String`) – Name of the data part.

-   `active` (`UInt8`) – Flag that indicates whether the data part is active. If a data part is active, it's used in a table. Otherwise, it's deleted. Inactive data parts remain after merging.

-   `marks` (`UInt64`) – The number of marks. To get the approximate number of rows in a data part, multiply `marks` 通过索引粒度（通常为8192）（此提示不适用于自适应粒度）。

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

-   `is_frozen` (`UInt8`) – Flag that shows that a partition data backup exists. 1, the backup exists. 0, the backup doesn't exist. For more details, see [FREEZE PARTITION](../sql-reference/statements/alter.md#alter_freeze-partition)

-   `database` (`String`) – Name of the database.

-   `table` (`String`) – Name of the table.

-   `engine` (`String`) – Name of the table engine without parameters.

-   `path` (`String`) – Absolute path to the folder with data part files.

-   `disk` (`String`) – Name of a disk that stores the data part.

-   `hash_of_all_files` (`String`) – [sipHash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) 的压缩文件。

-   `hash_of_uncompressed_files` (`String`) – [sipHash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) 未压缩的文件（带标记的文件，索引文件等。).

-   `uncompressed_hash_of_compressed_files` (`String`) – [sipHash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) 压缩文件中的数据，就好像它们是未压缩的。

-   `bytes` (`UInt64`) – Alias for `bytes_on_disk`.

-   `marks_size` (`UInt64`) – Alias for `marks_bytes`.

## 系统。part\_log {#system_tables-part-log}

该 `system.part_log` 表只有当创建 [part\_log](server-configuration-parameters/settings.md#server_configuration_parameters-part-log) 指定了服务器设置。

此表包含与以下情况发生的事件有关的信息 [数据部分](../engines/table-engines/mergetree-family/custom-partitioning-key.md) 在 [MergeTree](../engines/table-engines/mergetree-family/mergetree.md) 家庭表，例如添加或合并数据。

该 `system.part_log` 表包含以下列:

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
-   `partition_id` (String) — ID of the partition that the data part was inserted to. The column takes the ‘all’ 值，如果分区是由 `tuple()`.
-   `rows` (UInt64) — The number of rows in the data part.
-   `size_in_bytes` (UInt64) — Size of the data part in bytes.
-   `merged_from` (Array(String)) — An array of names of the parts which the current part was made up from (after the merge).
-   `bytes_uncompressed` (UInt64) — Size of uncompressed bytes.
-   `read_rows` (UInt64) — The number of rows was read during the merge.
-   `read_bytes` (UInt64) — The number of bytes was read during the merge.
-   `error` (UInt16) — The code number of the occurred error.
-   `exception` (String) — Text message of the occurred error.

该 `system.part_log` 表的第一个插入数据到后创建 `MergeTree` 桌子

## 系统。流程 {#system_tables-processes}

该系统表用于实现 `SHOW PROCESSLIST` 查询。

列:

-   `user` (String) – The user who made the query. Keep in mind that for distributed processing, queries are sent to remote servers under the `default` 用户。 该字段包含特定查询的用户名，而不是此查询启动的查询的用户名。
-   `address` (String) – The IP address the request was made from. The same for distributed processing. To track where a distributed query was originally made from, look at `system.processes` 查询请求者服务器上。
-   `elapsed` (Float64) – The time in seconds since request execution started.
-   `rows_read` (UInt64) – The number of rows read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.
-   `bytes_read` (UInt64) – The number of uncompressed bytes read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.
-   `total_rows_approx` (UInt64) – The approximation of the total number of rows that should be read. For distributed processing, on the requestor server, this is the total for all remote servers. It can be updated during request processing, when new sources to process become known.
-   `memory_usage` (UInt64) – Amount of RAM the request uses. It might not include some types of dedicated memory. See the [max\_memory\_usage](../operations/settings/query-complexity.md#settings_max_memory_usage) 设置。
-   `query` (String) – The query text. For `INSERT`，它不包括要插入的数据。
-   `query_id` (String) – Query ID, if defined.

## 系统。text\_log {#system-tables-text-log}

包含日志记录条目。 进入该表的日志记录级别可以通过以下方式进行限制 `text_log.level` 服务器设置。

列:

-   `event_date` (`Date`)-条目的日期。
-   `event_time` (`DateTime`)-条目的时间。
-   `microseconds` (`UInt32`）-条目的微秒。
-   `thread_name` (String) — Name of the thread from which the logging was done.
-   `thread_id` (UInt64) — OS thread ID.
-   `level` (`Enum8`）-入门级。
    -   `'Fatal' = 1`
    -   `'Critical' = 2`
    -   `'Error' = 3`
    -   `'Warning' = 4`
    -   `'Notice' = 5`
    -   `'Information' = 6`
    -   `'Debug' = 7`
    -   `'Trace' = 8`
-   `query_id` (`String`)-查询的ID。
-   `logger_name` (`LowCardinality(String)`) - Name of the logger (i.e. `DDLWorker`)
-   `message` (`String`）-消息本身。
-   `revision` (`UInt32`)-ClickHouse修订。
-   `source_file` (`LowCardinality(String)`)-从中完成日志记录的源文件。
-   `source_line` (`UInt64`)-从中完成日志记录的源代码行。

## 系统。query\_log {#system_tables-query_log}

包含有关查询执行的信息。 对于每个查询，您可以看到处理开始时间，处理持续时间，错误消息和其他信息。

!!! note "注"
    该表不包含以下内容的输入数据 `INSERT` 查询。

ClickHouse仅在以下情况下创建此表 [query\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) 指定服务器参数。 此参数设置日志记录规则，例如日志记录间隔或将记录查询的表的名称。

要启用查询日志记录，请设置 [log\_queries](settings/settings.md#settings-log-queries) 参数为1。 有关详细信息，请参阅 [设置](settings/settings.md) 科。

该 `system.query_log` 表注册两种查询:

1.  客户端直接运行的初始查询。
2.  由其他查询启动的子查询（用于分布式查询执行）。 对于这些类型的查询，有关父查询的信息显示在 `initial_*` 列。

列:

-   `type` (`Enum8`) — Type of event that occurred when executing the query. Values:
    -   `'QueryStart' = 1` — Successful start of query execution.
    -   `'QueryFinish' = 2` — Successful end of query execution.
    -   `'ExceptionBeforeStart' = 3` — Exception before the start of query execution.
    -   `'ExceptionWhileProcessing' = 4` — Exception during the query execution.
-   `event_date` (Date) — Query starting date.
-   `event_time` (DateTime) — Query starting time.
-   `query_start_time` (DateTime) — Start time of query execution.
-   `query_duration_ms` (UInt64) — Duration of query execution.
-   `read_rows` (UInt64) — Number of read rows.
-   `read_bytes` (UInt64) — Number of read bytes.
-   `written_rows` (UInt64) — For `INSERT` 查询，写入的行数。 对于其他查询，列值为0。
-   `written_bytes` (UInt64) — For `INSERT` 查询时，写入的字节数。 对于其他查询，列值为0。
-   `result_rows` (UInt64) — Number of rows in the result.
-   `result_bytes` (UInt64) — Number of bytes in the result.
-   `memory_usage` (UInt64) — Memory consumption by the query.
-   `query` (String) — Query string.
-   `exception` (String) — Exception message.
-   `stack_trace` (String) — Stack trace (a list of methods called before the error occurred). An empty string, if the query is completed successfully.
-   `is_initial_query` (UInt8) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query for distributed query execution.
-   `user` (String) — Name of the user who initiated the current query.
-   `query_id` (String) — ID of the query.
-   `address` (IPv6) — IP address that was used to make the query.
-   `port` (UInt16) — The client port that was used to make the query.
-   `initial_user` (String) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` (String) — ID of the initial query (for distributed query execution).
-   `initial_address` (IPv6) — IP address that the parent query was launched from.
-   `initial_port` (UInt16) — The client port that was used to make the parent query.
-   `interface` (UInt8) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` (String) — OS's username who runs [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../interfaces/cli.md).
-   `client_hostname` (String) — Hostname of the client machine where the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../interfaces/cli.md) 或者运行另一个TCP客户端。
-   `client_name` (String) — The [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../interfaces/cli.md) 或另一个TCP客户端名称。
-   `client_revision` (UInt32) — Revision of the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../interfaces/cli.md) 或另一个TCP客户端。
-   `client_version_major` (UInt32) — Major version of the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../interfaces/cli.md) 或另一个TCP客户端。
-   `client_version_minor` (UInt32) — Minor version of the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../interfaces/cli.md) 或另一个TCP客户端。
-   `client_version_patch` (UInt32) — Patch component of the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../interfaces/cli.md) 或另一个TCP客户端版本。
-   `http_method` (UInt8) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` 方法被使用。
    -   2 — `POST` 方法被使用。
-   `http_user_agent` (String) — The `UserAgent` http请求中传递的标头。
-   `quota_key` (String) — The “quota key” 在指定 [配额](quotas.md) 设置（见 `keyed`).
-   `revision` (UInt32) — ClickHouse revision.
-   `thread_numbers` (Array(UInt32)) — Number of threads that are participating in query execution.
-   `ProfileEvents.Names` (Array(String)) — Counters that measure different metrics. The description of them could be found in the table [系统。活动](#system_tables-events)
-   `ProfileEvents.Values` (Array(UInt64)) — Values of metrics that are listed in the `ProfileEvents.Names` 列。
-   `Settings.Names` (Array(String)) — Names of settings that were changed when the client ran the query. To enable logging changes to settings, set the `log_query_settings` 参数为1。
-   `Settings.Values` (Array(String)) — Values of settings that are listed in the `Settings.Names` 列。

每个查询创建一个或两个行中 `query_log` 表，具体取决于查询的状态:

1.  如果查询执行成功，将创建两个类型为1和2的事件（请参阅 `type` 列）。
2.  如果在查询处理过程中发生错误，将创建两个类型为1和4的事件。
3.  如果在启动查询之前发生错误，将创建类型为3的单个事件。

默认情况下，日志以7.5秒的间隔添加到表中。 您可以在设置此时间间隔 [query\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) 服务器设置（请参阅 `flush_interval_milliseconds` 参数）。 要强制将日志从内存缓冲区刷新到表中，请使用 `SYSTEM FLUSH LOGS` 查询。

当手动删除表时，它将自动动态创建。 请注意，所有以前的日志将被删除。

!!! note "注"
    日志的存储周期是无限的。 日志不会自动从表中删除。 您需要自己组织删除过时的日志。

您可以指定一个任意的分区键 `system.query_log` 表中的 [query\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) 服务器设置（请参阅 `partition_by` 参数）。

## 系统。query\_thread\_log {#system_tables-query-thread-log}

该表包含有关每个查询执行线程的信息。

ClickHouse仅在以下情况下创建此表 [query\_thread\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) 指定服务器参数。 此参数设置日志记录规则，例如日志记录间隔或将记录查询的表的名称。

要启用查询日志记录，请设置 [log\_query\_threads](settings/settings.md#settings-log-query-threads) 参数为1。 有关详细信息，请参阅 [设置](settings/settings.md) 科。

列:

-   `event_date` (Date) — the date when the thread has finished execution of the query.
-   `event_time` (DateTime) — the date and time when the thread has finished execution of the query.
-   `query_start_time` (DateTime) — Start time of query execution.
-   `query_duration_ms` (UInt64) — Duration of query execution.
-   `read_rows` (UInt64) — Number of read rows.
-   `read_bytes` (UInt64) — Number of read bytes.
-   `written_rows` (UInt64) — For `INSERT` 查询，写入的行数。 对于其他查询，列值为0。
-   `written_bytes` (UInt64) — For `INSERT` 查询时，写入的字节数。 对于其他查询，列值为0。
-   `memory_usage` (Int64) — The difference between the amount of allocated and freed memory in context of this thread.
-   `peak_memory_usage` (Int64) — The maximum difference between the amount of allocated and freed memory in context of this thread.
-   `thread_name` (String) — Name of the thread.
-   `thread_number` (UInt32) — Internal thread ID.
-   `os_thread_id` (Int32) — OS thread ID.
-   `master_thread_id` (UInt64) — OS initial ID of initial thread.
-   `query` (String) — Query string.
-   `is_initial_query` (UInt8) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query for distributed query execution.
-   `user` (String) — Name of the user who initiated the current query.
-   `query_id` (String) — ID of the query.
-   `address` (IPv6) — IP address that was used to make the query.
-   `port` (UInt16) — The client port that was used to make the query.
-   `initial_user` (String) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` (String) — ID of the initial query (for distributed query execution).
-   `initial_address` (IPv6) — IP address that the parent query was launched from.
-   `initial_port` (UInt16) — The client port that was used to make the parent query.
-   `interface` (UInt8) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` (String) — OS's username who runs [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../interfaces/cli.md).
-   `client_hostname` (String) — Hostname of the client machine where the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../interfaces/cli.md) 或者运行另一个TCP客户端。
-   `client_name` (String) — The [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../interfaces/cli.md) 或另一个TCP客户端名称。
-   `client_revision` (UInt32) — Revision of the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../interfaces/cli.md) 或另一个TCP客户端。
-   `client_version_major` (UInt32) — Major version of the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../interfaces/cli.md) 或另一个TCP客户端。
-   `client_version_minor` (UInt32) — Minor version of the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../interfaces/cli.md) 或另一个TCP客户端。
-   `client_version_patch` (UInt32) — Patch component of the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../interfaces/cli.md) 或另一个TCP客户端版本。
-   `http_method` (UInt8) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` 方法被使用。
    -   2 — `POST` 方法被使用。
-   `http_user_agent` (String) — The `UserAgent` http请求中传递的标头。
-   `quota_key` (String) — The “quota key” 在指定 [配额](quotas.md) 设置（见 `keyed`).
-   `revision` (UInt32) — ClickHouse revision.
-   `ProfileEvents.Names` (Array(String)) — Counters that measure different metrics for this thread. The description of them could be found in the table [系统。活动](#system_tables-events)
-   `ProfileEvents.Values` (Array(UInt64)) — Values of metrics for this thread that are listed in the `ProfileEvents.Names` 列。

默认情况下，日志以7.5秒的间隔添加到表中。 您可以在设置此时间间隔 [query\_thread\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) 服务器设置（请参阅 `flush_interval_milliseconds` 参数）。 要强制将日志从内存缓冲区刷新到表中，请使用 `SYSTEM FLUSH LOGS` 查询。

当手动删除表时，它将自动动态创建。 请注意，所有以前的日志将被删除。

!!! note "注"
    日志的存储周期是无限的。 日志不会自动从表中删除。 您需要自己组织删除过时的日志。

您可以指定一个任意的分区键 `system.query_thread_log` 表中的 [query\_thread\_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) 服务器设置（请参阅 `partition_by` 参数）。

## 系统。trace\_log {#system_tables-trace_log}

包含采样查询探查器收集的堆栈跟踪。

ClickHouse创建此表时 [trace\_log](server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) 服务器配置部分被设置。 也是 [query\_profiler\_real\_time\_period\_ns](settings/settings.md#query_profiler_real_time_period_ns) 和 [query\_profiler\_cpu\_time\_period\_ns](settings/settings.md#query_profiler_cpu_time_period_ns) 应设置设置。

要分析日志，请使用 `addressToLine`, `addressToSymbol` 和 `demangle` 内省功能。

列:

-   `event_date` ([日期](../sql-reference/data-types/date.md)) — Date of sampling moment.

-   `event_time` ([日期时间](../sql-reference/data-types/datetime.md)) — Timestamp of the sampling moment.

-   `timestamp_ns` ([UInt64](../sql-reference/data-types/int-uint.md)) — Timestamp of the sampling moment in nanoseconds.

-   `revision` ([UInt32](../sql-reference/data-types/int-uint.md)) — ClickHouse server build revision.

    通过以下方式连接到服务器 `clickhouse-client`，你看到的字符串类似于 `Connected to ClickHouse server version 19.18.1 revision 54429.`. 该字段包含 `revision`，但不是 `version` 的服务器。

-   `timer_type` ([枚举8](../sql-reference/data-types/enum.md)) — Timer type:

    -   `Real` 表示挂钟时间。
    -   `CPU` 表示CPU时间。

-   `thread_number` ([UInt32](../sql-reference/data-types/int-uint.md)) — Thread identifier.

-   `query_id` ([字符串](../sql-reference/data-types/string.md)) — Query identifier that can be used to get details about a query that was running from the [query\_log](#system_tables-query_log) 系统表.

-   `trace` ([数组(UInt64)](../sql-reference/data-types/array.md)) — Stack trace at the moment of sampling. Each element is a virtual memory address inside ClickHouse server process.

**示例**

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

## 系统。副本 {#system_tables-replicas}

包含驻留在本地服务器上的复制表的信息和状态。
此表可用于监视。 该表对于每个已复制的\*表都包含一行。

示例:

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

列:

-   `database` (`String`)-数据库名称
-   `table` (`String`)-表名
-   `engine` (`String`)-表引擎名称
-   `is_leader` (`UInt8`)-副本是否是领导者。
    一次只有一个副本可以成为领导者。 领导者负责选择要执行的后台合并。
    请注意，可以对任何可用且在ZK中具有会话的副本执行写操作，而不管该副本是否为leader。
-   `can_become_leader` (`UInt8`)-副本是否可以当选为领导者。
-   `is_readonly` (`UInt8`)-副本是否处于只读模式。
    如果配置没有ZooKeeper的部分，如果在ZooKeeper中重新初始化会话时发生未知错误，以及在ZooKeeper中重新初始化会话时发生未知错误，则此模式将打开。
-   `is_session_expired` (`UInt8`)-与ZooKeeper的会话已经过期。 基本上一样 `is_readonly`.
-   `future_parts` (`UInt32`)-由于尚未完成的插入或合并而显示的数据部分的数量。
-   `parts_to_check` (`UInt32`)-队列中用于验证的数据部分的数量。 如果怀疑零件可能已损坏，则将其放入验证队列。
-   `zookeeper_path` (`String`)-在ZooKeeper中的表数据路径。
-   `replica_name` (`String`)-在动物园管理员副本名称. 同一表的不同副本具有不同的名称。
-   `replica_path` (`String`)-在ZooKeeper中的副本数据的路径。 与连接相同 ‘zookeeper\_path/replicas/replica\_path’.
-   `columns_version` (`Int32`)-表结构的版本号。 指示执行ALTER的次数。 如果副本有不同的版本，这意味着一些副本还没有做出所有的改变。
-   `queue_size` (`UInt32`)-等待执行的操作的队列大小。 操作包括插入数据块、合并和某些其他操作。 它通常与 `future_parts`.
-   `inserts_in_queue` (`UInt32`)-需要插入数据块的数量。 插入通常复制得相当快。 如果这个数字很大，这意味着有什么不对劲。
-   `merges_in_queue` (`UInt32`)-等待进行合并的数量。 有时合并时间很长，因此此值可能长时间大于零。
-   `part_mutations_in_queue` (`UInt32`）-等待进行的突变的数量。
-   `queue_oldest_time` (`DateTime`)-如果 `queue_size` 大于0，显示何时将最旧的操作添加到队列中。
-   `inserts_oldest_time` (`DateTime`）-看 `queue_oldest_time`
-   `merges_oldest_time` (`DateTime`）-看 `queue_oldest_time`
-   `part_mutations_oldest_time` (`DateTime`）-看 `queue_oldest_time`

接下来的4列只有在有ZK活动会话的情况下才具有非零值。

-   `log_max_index` (`UInt64`)-一般活动日志中的最大条目数。
-   `log_pointer` (`UInt64`)-副本复制到其执行队列的常规活动日志中的最大条目数加一。 如果 `log_pointer` 比 `log_max_index`，有点不对劲。
-   `last_queue_update` (`DateTime`)-上次更新队列时。
-   `absolute_delay` (`UInt64`）-当前副本有多大滞后秒。
-   `total_replicas` (`UInt8`)-此表的已知副本总数。
-   `active_replicas` (`UInt8`)-在ZooKeeper中具有会话的此表的副本的数量（即正常运行的副本的数量）。

如果您请求所有列，表可能会工作得有点慢，因为每行都会从ZooKeeper进行几次读取。
如果您没有请求最后4列（log\_max\_index，log\_pointer，total\_replicas，active\_replicas），表工作得很快。

例如，您可以检查一切是否正常工作，如下所示:

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

如果这个查询没有返回任何东西，这意味着一切都很好。

## 系统。设置 {#system-tables-system-settings}

包含有关当前用户的会话设置的信息。

列:

-   `name` ([字符串](../sql-reference/data-types/string.md)) — Setting name.
-   `value` ([字符串](../sql-reference/data-types/string.md)) — Setting value.
-   `changed` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether a setting is changed from its default value.
-   `description` ([字符串](../sql-reference/data-types/string.md)) — Short setting description.
-   `min` ([可为空](../sql-reference/data-types/nullable.md)([字符串](../sql-reference/data-types/string.md))) — Minimum value of the setting, if any is set via [制约因素](settings/constraints-on-settings.md#constraints-on-settings). 如果设置没有最小值，则包含 [NULL](../sql-reference/syntax.md#null-literal).
-   `max` ([可为空](../sql-reference/data-types/nullable.md)([字符串](../sql-reference/data-types/string.md))) — Maximum value of the setting, if any is set via [制约因素](settings/constraints-on-settings.md#constraints-on-settings). 如果设置没有最大值，则包含 [NULL](../sql-reference/syntax.md#null-literal).
-   `readonly` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether the current user can change the setting:
    -   `0` — Current user can change the setting.
    -   `1` — Current user can't change the setting.

**示例**

下面的示例演示如何获取有关名称包含的设置的信息 `min_i`.

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

使用 `WHERE changed` 可以是有用的，例如，当你想检查:

-   配置文件中的设置是否正确加载并正在使用。
-   在当前会话中更改的设置。

<!-- -->

``` sql
SELECT * FROM system.settings WHERE changed AND name='load_balancing'
```

**另请参阅**

-   [设置](settings/index.md#session-settings-intro)
-   [查询权限](settings/permissions-for-queries.md#settings_readonly)
-   [对设置的限制](settings/constraints-on-settings.md)

## 系统。表\_engines {#system.table_engines}

``` text
┌─name───────────────────┬─value───────┐
│ max_threads            │ 8           │
│ use_uncompressed_cache │ 0           │
│ load_balancing         │ random      │
│ max_memory_usage       │ 10000000000 │
└────────────────────────┴─────────────┘
```

## 系统。merge\_tree\_settings {#system-merge_tree_settings}

包含有关以下设置的信息 `MergeTree` 桌子

列:

-   `name` (String) — Setting name.
-   `value` (String) — Setting value.
-   `description` (String) — Setting description.
-   `type` (String) — Setting type (implementation specific string value).
-   `changed` (UInt8) — Whether the setting was explicitly defined in the config or explicitly changed.

## 系统。表\_engines {#system-table-engines}

包含服务器支持的表引擎的描述及其功能支持信息。

此表包含以下列（列类型显示在括号中):

-   `name` (String) — The name of table engine.
-   `supports_settings` (UInt8) — Flag that indicates if table engine supports `SETTINGS` 条款
-   `supports_skipping_indices` (UInt8) — Flag that indicates if table engine supports [跳过索引](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes).
-   `supports_ttl` (UInt8) — Flag that indicates if table engine supports [TTL](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).
-   `supports_sort_order` (UInt8) — Flag that indicates if table engine supports clauses `PARTITION_BY`, `PRIMARY_KEY`, `ORDER_BY` 和 `SAMPLE_BY`.
-   `supports_replication` (UInt8) — Flag that indicates if table engine supports [数据复制](../engines/table-engines/mergetree-family/replication.md).
-   `supports_duduplication` (UInt8) — Flag that indicates if table engine supports data deduplication.

示例:

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

**另请参阅**

-   梅树家族 [查询子句](../engines/table-engines/mergetree-family/mergetree.md#mergetree-query-clauses)
-   卡夫卡 [设置](../engines/table-engines/integrations/kafka.md#table_engine-kafka-creating-a-table)
-   加入我们 [设置](../engines/table-engines/special/join.md#join-limitations-and-settings)

## 系统。表 {#system-tables}

包含服务器知道的每个表的元数据。 分离的表不显示在 `system.tables`.

此表包含以下列（列类型显示在括号中):

-   `database` (String) — The name of the database the table is in.

-   `name` (String) — Table name.

-   `engine` (String) — Table engine name (without parameters).

-   `is_temporary` (UInt8)-指示表是否是临时的标志。

-   `data_path` (String)-文件系统中表数据的路径。

-   `metadata_path` (String)-文件系统中表元数据的路径。

-   `metadata_modification_time` (DateTime)-表元数据的最新修改时间。

-   `dependencies_database` (数组(字符串))-数据库依赖关系.

-   `dependencies_table` （数组（字符串））-表依赖关系 ([MaterializedView](../engines/table-engines/special/materializedview.md) 基于当前表的表）。

-   `create_table_query` (String)-用于创建表的查询。

-   `engine_full` (String)-表引擎的参数。

-   `partition_key` (String)-表中指定的分区键表达式。

-   `sorting_key` (String)-表中指定的排序键表达式。

-   `primary_key` (String)-表中指定的主键表达式。

-   `sampling_key` (String)-表中指定的采样键表达式。

-   `storage_policy` (字符串)-存储策略:

    -   [MergeTree](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)
    -   [分布](../engines/table-engines/special/distributed.md#distributed)

-   `total_rows` (Nullable(UInt64))-总行数，如果可以快速确定表中的确切行数，否则 `Null` （包括内衣 `Buffer` 表）。

-   `total_bytes` (Nullable(UInt64))-总字节数，如果可以快速确定存储表的确切字节数，否则 `Null` (**不** 包括任何底层存储）。

    -   If the table stores data on disk, returns used space on disk (i.e. compressed).
    -   如果表在内存中存储数据,返回在内存中使用的近似字节数.

该 `system.tables` 表中使用 `SHOW TABLES` 查询实现。

## 系统。动物园管理员 {#system-zookeeper}

如果未配置ZooKeeper，则表不存在。 允许从配置中定义的ZooKeeper集群读取数据。
查询必须具有 ‘path’ WHERE子句中的平等条件。 这是ZooKeeper中您想要获取数据的孩子的路径。

查询 `SELECT * FROM system.zookeeper WHERE path = '/clickhouse'` 输出对所有孩子的数据 `/clickhouse` 节点。
要输出所有根节点的数据，write path= ‘/’.
如果在指定的路径 ‘path’ 不存在，将引发异常。

列:

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

示例:

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

## 系统。突变 {#system_tables-mutations}

该表包含以下信息 [突变](../sql-reference/statements/alter.md#alter-mutations) MergeTree表及其进展。 每个突变命令由一行表示。 该表具有以下列:

**数据库**, **表** -应用突变的数据库和表的名称。

**mutation\_id** -变异的ID 对于复制的表，这些Id对应于znode中的名称 `<table_path_in_zookeeper>/mutations/` 动物园管理员的目录。 对于未复制的表，Id对应于表的数据目录中的文件名。

**命令** -Mutation命令字符串（查询后的部分 `ALTER TABLE [db.]table`).

**create\_time** -当这个突变命令被提交执行。

**block\_numbers.partition\_id**, **block\_numbers.编号** -嵌套列。 对于复制表的突变，它包含每个分区的一条记录：分区ID和通过突变获取的块编号（在每个分区中，只有包含编号小于该分区中突变获取的块编号的块的 在非复制表中，所有分区中的块编号形成一个序列。 这意味着对于非复制表的突变，该列将包含一条记录，其中包含由突变获取的单个块编号。

**parts\_to\_do** -为了完成突变，需要突变的数据部分的数量。

**is\_done** -变异完成了?？ 请注意，即使 `parts_to_do = 0` 由于长时间运行的INSERT将创建需要突变的新数据部分，因此可能尚未完成复制表的突变。

如果在改变某些部分时出现问题，以下列将包含其他信息:

**latest\_failed\_part** -不能变异的最新部分的名称。

**latest\_fail\_time** -最近的部分突变失败的时间。

**latest\_fail\_reason** -导致最近部件变异失败的异常消息。

## 系统。磁盘 {#system_tables-disks}

包含有关在定义的磁盘信息 [服务器配置](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

列:

-   `name` ([字符串](../sql-reference/data-types/string.md)) — Name of a disk in the server configuration.
-   `path` ([字符串](../sql-reference/data-types/string.md)) — Path to the mount point in the file system.
-   `free_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — Free space on disk in bytes.
-   `total_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — Disk volume in bytes.
-   `keep_free_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — Amount of disk space that should stay free on disk in bytes. Defined in the `keep_free_space_bytes` 磁盘配置参数。

## 系统。storage\_policies {#system_tables-storage_policies}

包含有关存储策略和卷中定义的信息 [服务器配置](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

列:

-   `policy_name` ([字符串](../sql-reference/data-types/string.md)) — Name of the storage policy.
-   `volume_name` ([字符串](../sql-reference/data-types/string.md)) — Volume name defined in the storage policy.
-   `volume_priority` ([UInt64](../sql-reference/data-types/int-uint.md)) — Volume order number in the configuration.
-   `disks` ([数组（字符串)](../sql-reference/data-types/array.md)) — Disk names, defined in the storage policy.
-   `max_data_part_size` ([UInt64](../sql-reference/data-types/int-uint.md)) — Maximum size of a data part that can be stored on volume disks (0 — no limit).
-   `move_factor` ([Float64](../sql-reference/data-types/float.md)) — Ratio of free disk space. When the ratio exceeds the value of configuration parameter, ClickHouse start to move data to the next volume in order.

如果存储策略包含多个卷，则每个卷的信息将存储在表的单独行中。

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/) <!--hide-->
