# system.text\_log {#system-tables-text-log}

Contains logging entries. Logging level which goes to this table can be limited with `text_log.level` server setting.

Columns:

-   `event_date` (Date) — Date of the entry.
-   `event_time` (DateTime) — Time of the entry.
-   `microseconds` (UInt32) — Microseconds of the entry.
-   `thread_name` (String) — Name of the thread from which the logging was done.
-   `thread_id` (UInt64) — OS thread ID.
-   `level` (`Enum8`) — Entry level. Possible values:
    -   `1` or `'Fatal'`.
    -   `2` or `'Critical'`.
    -   `3` or `'Error'`.
    -   `4` or `'Warning'`.
    -   `5` or `'Notice'`.
    -   `6` or `'Information'`.
    -   `7` or `'Debug'`.
    -   `8` or `'Trace'`.
-   `query_id` (String) — ID of the query.
-   `logger_name` (LowCardinality(String)) — Name of the logger (i.e. `DDLWorker`).
-   `message` (String) — The message itself.
-   `revision` (UInt32) — ClickHouse revision.
-   `source_file` (LowCardinality(String)) — Source file from which the logging was done.
-   `source_line` (UInt64) — Source line from which the logging was done.
