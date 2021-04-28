# system.events {#system_tables-events}

包含有关系统中发生的事件数的信息。 例如，在表中，您可以找到自ClickHouse服务器启动以来处理的 `SELECT` 查询数。

列:

-   `event` ([String](../../sql-reference/data-types/string.md)) — 事件名称。
-   `value` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 事件发生的次数。
-   `description` ([String](../../sql-reference/data-types/string.md)) — 事件描述。

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

-   [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics) — 包含在后台定期计算的指标。
-   [system.metrics](../../operations/system-tables/metrics.md#system_tables-metrics) — 包含实时计算的指标。
-   [system.metric_log](../../operations/system-tables/metric_log.md#system_tables-metric_log) — 包含来自表 `system.metrics` 和 `system.events`的指标值的历史记录。
-   [监测](../../operations/monitoring.md) — ClickHouse监测的基本概念。

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/events) <!--hide-->
