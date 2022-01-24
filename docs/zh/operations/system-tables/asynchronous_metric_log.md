## system.asynchronous_metric_log {#system-tables-async-log}

包含每分钟记录一次的 `system.asynchronous_metrics`历史值。默认开启。

列：
-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — 事件日期。
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 事件时间。
-   `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — 事件时间(微秒)。
-   `name` ([String](../../sql-reference/data-types/string.md)) — 指标名。
-   `value` ([Float64](../../sql-reference/data-types/float.md)) — 指标值。

**示例**
``` sql
SELECT * FROM system.asynchronous_metric_log LIMIT 10
```
``` text
┌─event_date─┬──────────event_time─┬────event_time_microseconds─┬─name─────────────────────────────────────┬─────value─┐
│ 2020-09-05 │ 2020-09-05 15:56:30 │ 2020-09-05 15:56:30.025227 │ CPUFrequencyMHz_0                        │    2120.9 │
│ 2020-09-05 │ 2020-09-05 15:56:30 │ 2020-09-05 15:56:30.025227 │ jemalloc.arenas.all.pmuzzy               │       743 │
│ 2020-09-05 │ 2020-09-05 15:56:30 │ 2020-09-05 15:56:30.025227 │ jemalloc.arenas.all.pdirty               │     26288 │
│ 2020-09-05 │ 2020-09-05 15:56:30 │ 2020-09-05 15:56:30.025227 │ jemalloc.background_thread.run_intervals │         0 │
│ 2020-09-05 │ 2020-09-05 15:56:30 │ 2020-09-05 15:56:30.025227 │ jemalloc.background_thread.num_runs      │         0 │
│ 2020-09-05 │ 2020-09-05 15:56:30 │ 2020-09-05 15:56:30.025227 │ jemalloc.retained                        │  60694528 │
│ 2020-09-05 │ 2020-09-05 15:56:30 │ 2020-09-05 15:56:30.025227 │ jemalloc.mapped                          │ 303161344 │
│ 2020-09-05 │ 2020-09-05 15:56:30 │ 2020-09-05 15:56:30.025227 │ jemalloc.resident                        │ 260931584 │
│ 2020-09-05 │ 2020-09-05 15:56:30 │ 2020-09-05 15:56:30.025227 │ jemalloc.metadata                        │  12079488 │
│ 2020-09-05 │ 2020-09-05 15:56:30 │ 2020-09-05 15:56:30.025227 │ jemalloc.allocated                       │ 133756128 │
└────────────┴─────────────────────┴────────────────────────────┴──────────────────────────────────────────┴───────────┘
```

**另请参阅**
-   [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics) — 包含在后台定期计算的指标.
-   [system.metric_log](../../operations/system-tables/metric_log.md#system_tables-metric_log) — 包含定期刷新到磁盘表 `system.metrics` 以及 `system.events` 中的指标值历史记录.
