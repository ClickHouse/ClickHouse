# system.events {#system_tables-events}

Содержит информацию о количестве событий, произошедших в системе. Например, в таблице можно найти, сколько запросов `SELECT` обработано с момента запуска сервера ClickHouse.

Столбцы:

-   `event` ([String](../../sql-reference/data-types/string.md)) — имя события.
-   `value` ([UInt64](../../sql-reference/data-types/int-uint.md)) — количество произошедших событий.
-   `description` ([String](../../sql-reference/data-types/string.md)) — описание события.

**Пример**

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

**Смотрите также**

-   [system.asynchronous_metrics](#system_tables-asynchronous_metrics) — таблица с периодически вычисляемыми метриками.
-   [system.metrics](#system_tables-metrics) — таблица с мгновенно вычисляемыми метриками.
-   [system.metric_log](#system_tables-metric_log) — таблица фиксирующая историю значений метрик из `system.metrics` и `system.events`.
-   [Мониторинг](../../operations/monitoring.md) — основы мониторинга в ClickHouse.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/events) <!--hide-->
