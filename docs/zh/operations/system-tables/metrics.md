# system.metrics {#system_tables-metrics}

此系统表包含可以即时计算或具有当前值的指标。例如，同时处理的查询数量或当前的复制延迟。这个表始终是最新的。

列:

-   `metric` ([字符串](../../sql-reference/data-types/string.md)) — 指标名称.
-   `value` ([Int64](../../sql-reference/data-types/int-uint.md)) — 指标的值.
-   `description` ([字符串](../../sql-reference/data-types/string.md)) — 指标的描述.

对于支持的指标列表，您可以查看 [src/Common/CurrentMetrics.cpp](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/CurrentMetrics.cpp) ClickHouse 的源文件。

**示例**

``` sql
SELECT * FROM system.metrics LIMIT 10
```

``` text
┌─metric───────────────────────────────┬─value─┬─description────────────────────────────────────────────────────────────┐
│ Query                                │     1 │ Number of executing queries                                            │
│ Merge                                │     0 │ Number of executing background merges                                  │
│ PartMutation                         │     0 │ Number of mutations (ALTER DELETE/UPDATE)                              │
│ ReplicatedFetch                      │     0 │ Number of data parts being fetched from replicas                       │
│ ReplicatedSend                       │     0 │ Number of data parts being sent to replicas                            │
│ ReplicatedChecks                     │     0 │ Number of data parts checking for consistency                          │
│ BackgroundMergesAndMutationsPoolTask │     0 │ Number of active merges and mutations in an associated background pool │
│ BackgroundFetchesPoolTask            │     0 │ Number of active fetches in an associated background pool              │
│ BackgroundCommonPoolTask             │     0 │ Number of active tasks in an associated background pool                │
│ BackgroundMovePoolTask               │     0 │ Number of active tasks in BackgroundProcessingPool for moves           │
└──────────────────────────────────────┴───────┴────────────────────────────────────────────────────────────────────────┘
```

**另请参阅**

-   [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics) — 包含周期性的计算指标。
-   [system.events](../../operations/system-tables/events.md#system_tables-events) — 包含发生的一些事件。
-   [system.metric_log](../../operations/system-tables/metric_log.md#system_tables-metric_log) — 包含`system.metrics`表和`system.events`表的历史指标值。
-   [监控](../../operations/monitoring.md) — ClickHouse 监控的基本概念。
