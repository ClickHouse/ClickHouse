---
slug: /en/operations/system-tables/text_log
---
# text_log

Contains logging entries. The logging level which goes to this table can be limited to the `text_log.level` server setting.

Columns:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Hostname of the server executing the query.
- `event_date` (Date) — Date of the entry.
- `event_time` (DateTime) — Time of the entry.
- `event_time_microseconds` (DateTime64) — Time of the entry with microseconds precision.
- `microseconds` (UInt32) — Microseconds of the entry.
- `thread_name` (String) — Name of the thread from which the logging was done.
- `thread_id` (UInt64) — OS thread ID.
- `level` (`Enum8`) — Entry level. Possible values:
    - `1` or `'Fatal'`.
    - `2` or `'Critical'`.
    - `3` or `'Error'`.
    - `4` or `'Warning'`.
    - `5` or `'Notice'`.
    - `6` or `'Information'`.
    - `7` or `'Debug'`.
    - `8` or `'Trace'`.
- `query_id` (String) — ID of the query.
- `logger_name` (LowCardinality(String)) — Name of the logger (i.e. `DDLWorker`).
- `message` (String) — The message itself.
- `revision` (UInt32) — ClickHouse revision.
- `source_file` (LowCardinality(String)) — Source file from which the logging was done.
- `source_line` (UInt64) — Source line from which the logging was done.
- `message_format_string` (LowCardinality(String)) — A format string that was used to format the message.
- `value1` (String) - Argument 1 that was used to format the message.
- `value2` (String) - Argument 2 that was used to format the message.
- `value3` (String) - Argument 3 that was used to format the message.
- `value4` (String) - Argument 4 that was used to format the message.
- `value5` (String) - Argument 5 that was used to format the message.
- `value6` (String) - Argument 6 that was used to format the message.
- `value7` (String) - Argument 7 that was used to format the message.
- `value8` (String) - Argument 8 that was used to format the message.
- `value9` (String) - Argument 9 that was used to format the message.
- `value10` (String) - Argument 10 that was used to format the message.

**Example**

``` sql
SELECT * FROM system.text_log LIMIT 1 \G
```

``` text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
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
message_format_string:   Update period {} seconds
value1:                  15
value2:                  
value3:                  
value4:                  
value5:                  
value6:                  
value7:                  
value8:                  
value9:                  
value10:                  
```
