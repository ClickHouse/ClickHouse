---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。asynchronous\_metrics {#system_tables-asynchronous_metrics}

包含在后台定期计算的指标。 例如，在使用的RAM量。

列:

-   `metric` ([字符串](../../sql-reference/data-types/string.md)) — Metric name.
-   `value` ([Float64](../../sql-reference/data-types/float.md)) — Metric value.

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

-   [监测](../../operations/monitoring.md) — Base concepts of ClickHouse monitoring.
-   [系统。指标](../../operations/system-tables/metrics.md#system_tables-metrics) — Contains instantly calculated metrics.
-   [系统。活动](../../operations/system-tables/events.md#system_tables-events) — Contains a number of events that have occurred.
-   [系统。metric\_log](../../operations/system-tables/metric_log.md#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.
