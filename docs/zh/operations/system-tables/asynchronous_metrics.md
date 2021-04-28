# system.asynchronous_metrics {#system_tables-asynchronous_metrics}

包含在后台定期计算的指标。 例如，正在使用的RAM量。

列:

-   `metric` ([String](../../sql-reference/data-types/string.md)) — 指标名称。
-   `value` ([Float64](../../sql-reference/data-types/float.md)) — 指标值。

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

-   [监测](../../operations/monitoring.md) — ClickHouse监测的基本概念。
-   [system.metrics](../../operations/system-tables/metrics.md#system_tables-metrics) — 包含实时计算的指标。
-   [system.events](../../operations/system-tables/events.md#system_tables-events) — 包含已发生的事件。
-   [system.metric_log](../../operations/system-tables/metric_log.md#system_tables-metric_log) — 包含来自表 `system.metrics` 和 `system.events`的指标值的历史记录。

 [原始文章](https://clickhouse.tech/docs/en/operations/system_tables/asynchronous_metrics) <!--hide-->
 