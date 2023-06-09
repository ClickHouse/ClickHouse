# system.crash_log {#system-tables_crash_log}

Содержит информацию о трассировках стека для фатальных ошибок. Таблица не содержится в базе данных по умолчанию, а создается только при возникновении фатальных ошибок.

Колонки:

-   `event_date` ([Datetime](../../sql-reference/data-types/datetime.md)) — Дата события.
-   `event_time` ([Datetime](../../sql-reference/data-types/datetime.md)) — Время события.
-   `timestamp_ns` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Время события с наносекундами.
-   `signal` ([Int32](../../sql-reference/data-types/int-uint.md)) — Номер сигнала, пришедшего в поток.
-   `thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Идентификатор треда.
-   `query_id` ([String](../../sql-reference/data-types/string.md)) — Идентификатор запроса.
-   `trace` ([Array](../../sql-reference/data-types/array.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Трассировка стека в момент ошибки. Представляет собой список физических адресов, по которым расположены вызываемые методы.
-   `trace_full` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Трассировка стека в момент ошибки. Содержит вызываемые методы.
-   `version` ([String](../../sql-reference/data-types/string.md)) — Версия сервера ClickHouse.
-   `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Ревизия сборки сервера ClickHouse.
-   `build_id` ([String](../../sql-reference/data-types/string.md)) — BuildID, сгенерированный компилятором.

**Пример**

Запрос:

``` sql
SELECT * FROM system.crash_log ORDER BY event_time DESC LIMIT 1;
```

Результат (приведён не полностью):

``` text
Row 1:
──────
event_date:   2020-10-14
event_time:   2020-10-14 15:47:40
timestamp_ns: 1602679660271312710
signal:       11
thread_id:    23624
query_id:     428aab7c-8f5c-44e9-9607-d16b44467e69
trace:        [188531193,...]
trace_full:   ['3. DB::(anonymous namespace)::FunctionFormatReadableTimeDelta::executeImpl(std::__1::vector<DB::ColumnWithTypeAndName, std::__1::allocator<DB::ColumnWithTypeAndName> >&, std::__1::vector<unsigned long, std::__1::allocator<unsigned long> > const&, unsigned long, unsigned long) const @ 0xb3cc1f9 in /home/username/work/ClickHouse/build/programs/clickhouse',...]
version:      ClickHouse 20.11.1.1
revision:     54442
build_id:
```

**См. также**
-   Системная таблица [trace_log](../../operations/system-tables/trace_log.md)

[Original article](https://clickhouse.com/docs/en/operations/system-tables/crash-log)
