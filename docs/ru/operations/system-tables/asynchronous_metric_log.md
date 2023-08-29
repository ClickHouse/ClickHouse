## system.asynchronous_metric_log {#system-tables-async-log}

Содержит исторические значения метрик из таблицы `system.asynchronous_metrics`, которые сохраняются раз в минуту. По умолчанию включена.

Столбцы:
-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — дата события.
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — время события.
-   `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — время события в микросекундах.
-   `name` ([String](../../sql-reference/data-types/string.md)) — название метрики.
-   `value` ([Float64](../../sql-reference/data-types/float.md)) — значение метрики.

**Пример**

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

**Смотрите также**
- [system.asynchronous_metrics](#system_tables-asynchronous_metrics) — Содержит метрики, которые периодически вычисляются в фоновом режиме.
- [system.metric_log](#system_tables-metric_log) — таблица фиксирующая историю значений метрик из `system.metrics` и `system.events`.

