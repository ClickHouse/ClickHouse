---
slug: /en/operations/system-tables/asynchronous_metric_log
---
# asynchronous_metric_log

Contains the historical values for `system.asynchronous_metrics`, which are saved once per minute. Enabled by default.

Columns:

- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Event date.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Event time.
- `name` ([String](../../sql-reference/data-types/string.md)) — Metric name.
- `value` ([Float64](../../sql-reference/data-types/float.md)) — Metric value.

**Example**

``` sql
SELECT * FROM system.asynchronous_metric_log LIMIT 10
```

``` text
┌─event_date─┬──────────event_time─┬─name─────────────────────────────────────┬─────value─┐
│ 2020-09-05 │ 2020-09-05 15:56:30 │ CPUFrequencyMHz_0                        │    2120.9 │
│ 2020-09-05 │ 2020-09-05 15:56:30 │ jemalloc.arenas.all.pmuzzy               │       743 │
│ 2020-09-05 │ 2020-09-05 15:56:30 │ jemalloc.arenas.all.pdirty               │     26288 │
│ 2020-09-05 │ 2020-09-05 15:56:30 │ jemalloc.background_thread.run_intervals │         0 │
│ 2020-09-05 │ 2020-09-05 15:56:30 │ jemalloc.background_thread.num_runs      │         0 │
│ 2020-09-05 │ 2020-09-05 15:56:30 │ jemalloc.retained                        │  60694528 │
│ 2020-09-05 │ 2020-09-05 15:56:30 │ jemalloc.mapped                          │ 303161344 │
│ 2020-09-05 │ 2020-09-05 15:56:30 │ jemalloc.resident                        │ 260931584 │
│ 2020-09-05 │ 2020-09-05 15:56:30 │ jemalloc.metadata                        │  12079488 │
│ 2020-09-05 │ 2020-09-05 15:56:30 │ jemalloc.allocated                       │ 133756128 │
└────────────┴─────────────────────┴──────────────────────────────────────────┴───────────┘
```

**See Also**

- [system.asynchronous_metrics](../system-tables/asynchronous_metrics.md) — Contains metrics, calculated periodically in the background.
- [system.metric_log](../system-tables/metric_log.md) — Contains history of metrics values from tables `system.metrics` and `system.events`, periodically flushed to disk.
