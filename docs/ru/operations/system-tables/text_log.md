# system.text_log {#system_tables-text_log}

Содержит записи логов. Уровень логирования для таблицы может быть ограничен параметром сервера `text_log.level`.

Столбцы:

-   `event_date` (Date) — дата создания записи.
-   `event_time` (DateTime) — время создания записи.
-   `event_time_microseconds` (DateTime) — время создания записи с точностью до микросекунд.
-   `microseconds` (UInt32) — время создания записи в микросекундах.
-   `thread_name` (String) — название потока, из которого была сделана запись.
-   `thread_id` (UInt64) — идентификатор потока ОС.
-   `level` (Enum8) — уровень логирования записи. Возможные значения:
    -   `1` или `'Fatal'`.
    -   `2` или `'Critical'`.
    -   `3` или `'Error'`.
    -   `4` или `'Warning'`.
    -   `5` или `'Notice'`.
    -   `6` или `'Information'`.
    -   `7` или `'Debug'`.
    -   `8` или `'Trace'`.
-   `query_id` (String) — идентификатор запроса.
-   `logger_name` (LowCardinality(String)) — название логгера (`DDLWorker`).
-   `message` (String) — само тело записи.
-   `revision` (UInt32) — ревизия ClickHouse.
-   `source_file` (LowCardinality(String)) — исходный файл, из которого была сделана запись.
-   `source_line` (UInt64) — исходная строка, из которой была сделана запись.

**Пример**

``` sql
SELECT * FROM system.text_log LIMIT 1 \G
```

``` text
Row 1:
──────
event_date:              2020-09-10
event_time:              2020-09-10 11:23:07
event_time_microseconds: 2020-09-10 11:23:07.871397
microseconds:            871397
thread_name:             clickhouse-serv
thread_id:               564917
level:                   Information
query_id:                
logger_name:             DNSCacheUpdater
message:                 Update period 15 seconds
revision:                54440
source_file:             /ClickHouse/src/Interpreters/DNSCacheUpdater.cpp; void DB::DNSCacheUpdater::start()
source_line:             45
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/text_log) <!--hide-->
