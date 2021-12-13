# system.asynchronous_metrics {#system_tables-asynchronous_metrics}

Содержит метрики, которые периодически вычисляются в фоновом режиме. Например, объём используемой оперативной памяти.

Столбцы:

-   `metric` ([String](../../sql-reference/data-types/string.md)) — название метрики.
-   `value` ([Float64](../../sql-reference/data-types/float.md)) — значение метрики.

**Пример**

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

**Смотрите также**

-   [Мониторинг](../../operations/monitoring.md) — основы мониторинга в ClickHouse.
-   [system.metrics](#system_tables-metrics) — таблица с мгновенно вычисляемыми метриками.
-   [system.events](#system_tables-events) — таблица с количеством произошедших событий.
-   [system.metric_log](#system_tables-metric_log) — таблица фиксирующая историю значений метрик из `system.metrics` и `system.events`.

 [Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/asynchronous_metrics) <!--hide-->
 