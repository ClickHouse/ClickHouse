---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。活动 {#system_tables-events}

包含有关系统中发生的事件数的信息。 例如，在表中，您可以找到多少 `SELECT` 自ClickHouse服务器启动以来已处理查询。

列:

-   `event` ([字符串](../../sql-reference/data-types/string.md)) — Event name.
-   `value` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Number of events occurred.
-   `description` ([字符串](../../sql-reference/data-types/string.md)) — Event description.

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

-   [系统。asynchronous\_metrics](../../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [系统。指标](../../operations/system-tables/metrics.md#system_tables-metrics) — Contains instantly calculated metrics.
-   [系统。metric\_log](../../operations/system-tables/metric_log.md#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.
-   [监测](../../operations/monitoring.md) — Base concepts of ClickHouse monitoring.
