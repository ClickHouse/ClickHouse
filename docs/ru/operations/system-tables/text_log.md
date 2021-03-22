# system.text_log {#system_tables-text_log}

Содержит записи логов. Уровень логирования для таблицы может быть ограничен параметром сервера `text_log.level`.

Столбцы:

-   `event_date` (Date) — Дата создания записи.
-   `event_time` (DateTime) — Время создания записи.
-   `microseconds` (UInt32) — Время создания записи в микросекундах.
-   `thread_name` (String) — Название потока, из которого была сделана запись.
-   `thread_id` (UInt64) — Идентификатор потока ОС.
-   `level` (Enum8) — Уровень логирования записи. Возможные значения:
    -   `1` или `'Fatal'`.
    -   `2` или `'Critical'`.
    -   `3` или `'Error'`.
    -   `4` или `'Warning'`.
    -   `5` или `'Notice'`.
    -   `6` или `'Information'`.
    -   `7` или `'Debug'`.
    -   `8` или `'Trace'`.
-   `query_id` (String) — Идентификатор запроса.
-   `logger_name` (LowCardinality(String)) — Название логгера (`DDLWorker`).
-   `message` (String) — Само тело записи.
-   `revision` (UInt32) — Ревизия ClickHouse.
-   `source_file` (LowCardinality(String)) — Исходный файл, из которого была сделана запись.
-   `source_line` (UInt64) — Исходная строка, из которой была сделана запись.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/text_log) <!--hide-->
