# system.metrics {#system_tables-metrics}

包含可以实时计算或具有当前值的指标。 例如，同时处理的查询的数量或当前副本的延迟。 此表始终是最新的。

列:

-   `metric` ([String](../../sql-reference/data-types/string.md)) — 指标名称。
-   `value` ([Int64](../../sql-reference/data-types/int-uint.md)) — 指标值。
-   `description` ([字符串](../../sql-reference/data-types/string.md)) — 指标描述。

您可以在ClickHouse的源文件 [src/Common/CurrentMetrics.cpp](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/CurrentMetrics.cpp) 中找到支持的指标列表，。

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

-   [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics) — 包含定期计算的指标。
-   [system.events](../../operations/system-tables/events.md#system_tables-events) — 包含已发生的事件。
-   [system.metric_log](../../operations/system-tables/metric_log.md#system_tables-metric_log) — 包含来自表 `system.metrics` 和 `system.events`的指标值的历史记录。
-   [监测](../../operations/monitoring.md) — BClickHouse监测的基本概念。

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/metrics) <!--hide-->
