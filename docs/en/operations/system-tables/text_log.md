# system.text_log {#system_tables-text_log}

Contains logging entries. The logging level which goes to this table can be limited to the `text_log.level` server setting.

Columns:

-   `event_date` (Date) — Date of the entry.
-   `event_time` (DateTime) — Time of the entry.
-   `event_time_microseconds` (DateTime) — Time of the entry with microseconds precision.
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

**Example**

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
 
 [Original article](https://clickhouse.tech/docs/en/operations/system_tables/text_log) <!--hide-->