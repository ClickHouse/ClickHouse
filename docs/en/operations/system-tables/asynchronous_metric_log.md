## system.asynchronous_metric_log {#system-tables_asynchronous_metric_log}
## system.asynchronous\_metric\_log {#system-tables-async-log}

Contains the historical values for `system.asynchronous_log`.

Columns:
-   `event_date` ([Date](../sql-reference/data-types/date.md)) — Description.
-   `event_time` ([DateTime](../sql-reference/data-types/datetime.md)) — Description.
-   `name` ([String](../sql-reference/data-types/string.md)) — Description.
-   `value` ([Float64](../sql-reference/data-types/float.md)) — Description.

**Example**

``` sql
SELECT * FROM system.asynchronous_metric_log LIMIT 10
```

``` text
┌─event_date─┬──────────event_time─┬─name─────────────────────────────────────┬────value─┐
│ 2020-06-22 │ 2020-06-22 06:57:30 │ jemalloc.arenas.all.pmuzzy               │        0 │
│ 2020-06-22 │ 2020-06-22 06:57:30 │ jemalloc.arenas.all.pdirty               │     4214 │
│ 2020-06-22 │ 2020-06-22 06:57:30 │ jemalloc.background_thread.run_intervals │        0 │
│ 2020-06-22 │ 2020-06-22 06:57:30 │ jemalloc.background_thread.num_runs      │        0 │
│ 2020-06-22 │ 2020-06-22 06:57:30 │ jemalloc.retained                        │ 17657856 │
│ 2020-06-22 │ 2020-06-22 06:57:30 │ jemalloc.mapped                          │ 71471104 │
│ 2020-06-22 │ 2020-06-22 06:57:30 │ jemalloc.resident                        │ 61538304 │
│ 2020-06-22 │ 2020-06-22 06:57:30 │ jemalloc.metadata                        │  6199264 │
│ 2020-06-22 │ 2020-06-22 06:57:30 │ jemalloc.allocated                       │ 38074336 │
│ 2020-06-22 │ 2020-06-22 06:57:30 │ jemalloc.epoch                           │        2 │
└────────────┴─────────────────────┴──────────────────────────────────────────┴──────────┘
```

**See Also**

- [Article name](path/to/article_name.md) — 
- [system.asynchronous\_metrics](../../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics) — Some words about referenced information.
