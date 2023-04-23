# system.metrics {#system_tables-metrics}

Содержит метрики, которые могут быть рассчитаны мгновенно или имеют текущее значение. Например, число одновременно обрабатываемых запросов или текущее значение задержки реплики. Эта таблица всегда актуальна.

Столбцы:

-   `metric` ([String](../../sql-reference/data-types/string.md)) — название метрики.
-   `value` ([Int64](../../sql-reference/data-types/int-uint.md)) — значение метрики.
-   `description` ([String](../../sql-reference/data-types/string.md)) — описание метрики.

Список поддерживаемых метрик смотрите в файле [src/Common/CurrentMetrics.cpp](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/CurrentMetrics.cpp).

**Пример**

``` sql
SELECT * FROM system.metrics LIMIT 10
```

``` text
┌─metric───────────────────────────────┬─value─┬─description────────────────────────────────────────────────────────────┐
│ Query                                │     1 │ Number of executing queries                                            │
│ Merge                                │     0 │ Number of executing background merges                                  │
│ PartMutation                         │     0 │ Number of mutations (ALTER DELETE/UPDATE)                              │
│ ReplicatedFetch                      │     0 │ Number of data parts being fetched from replicas                       │
│ ReplicatedSend                       │     0 │ Number of data parts being sent to replicas                            │
│ ReplicatedChecks                     │     0 │ Number of data parts checking for consistency                          │
│ BackgroundMergesAndMutationsPoolTask │     0 │ Number of active merges and mutations in an associated background pool │
│ BackgroundFetchesPoolTask            │     0 │ Number of active fetches in an associated background pool              │
│ BackgroundCommonPoolTask             │     0 │ Number of active tasks in an associated background pool                │
│ BackgroundMovePoolTask               │     0 │ Number of active tasks in BackgroundProcessingPool for moves           │
└──────────────────────────────────────┴───────┴────────────────────────────────────────────────────────────────────────┘
```

**Смотрите также**

-   [system.asynchronous_metrics](#system_tables-asynchronous_metrics) — таблица с периодически вычисляемыми метриками.
-   [system.events](#system_tables-events) — таблица с количеством произошедших событий.
-   [system.metric_log](#system_tables-metric_log) — таблица фиксирующая историю значений метрик из `system.metrics` и `system.events`.
-   [Мониторинг](../../operations/monitoring.md) — основы мониторинга в ClickHouse.

