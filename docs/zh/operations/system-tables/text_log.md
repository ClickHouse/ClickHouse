---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。text\_log {#system-tables-text-log}

包含日志记录条目。 进入该表的日志记录级别可以通过以下方式进行限制 `text_log.level` 服务器设置。

列:

-   `event_date` (Date) — Date of the entry.
-   `event_time` (DateTime) — Time of the entry.
-   `microseconds` (UInt32) — Microseconds of the entry.
-   `thread_name` (String) — Name of the thread from which the logging was done.
-   `thread_id` (UInt64) — OS thread ID.
-   `level` (`Enum8`) — Entry level. Possible values:
    -   `1` 或 `'Fatal'`.
    -   `2` 或 `'Critical'`.
    -   `3` 或 `'Error'`.
    -   `4` 或 `'Warning'`.
    -   `5` 或 `'Notice'`.
    -   `6` 或 `'Information'`.
    -   `7` 或 `'Debug'`.
    -   `8` 或 `'Trace'`.
-   `query_id` (String) — ID of the query.
-   `logger_name` (LowCardinality(String)) — Name of the logger (i.e. `DDLWorker`).
-   `message` (String) — The message itself.
-   `revision` (UInt32) — ClickHouse revision.
-   `source_file` (LowCardinality(String)) — Source file from which the logging was done.
-   `source_line` (UInt64) — Source line from which the logging was done.
