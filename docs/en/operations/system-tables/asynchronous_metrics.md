# system.asynchronous_metrics {#system_tables-asynchronous_metrics}

Contains metrics that are calculated periodically in the background. For example, the amount of RAM in use.

Columns:

-   `metric` ([String](../../sql-reference/data-types/string.md)) — Metric name.
-   `value` ([Float64](../../sql-reference/data-types/float.md)) — Metric value.

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

-   [Monitoring](../../operations/monitoring.md) — Base concepts of ClickHouse monitoring.
-   [system.metrics](../../operations/system-tables/metrics.md#system_tables-metrics) — Contains instantly calculated metrics.
-   [system.events](../../operations/system-tables/events.md#system_tables-events) — Contains a number of events that have occurred.
-   [system.metric_log](../../operations/system-tables/metric_log.md#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.

 [Original article](https://clickhouse.tech/docs/en/operations/system_tables/asynchronous_metrics) <!--hide-->