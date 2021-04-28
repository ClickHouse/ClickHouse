# system.text_log {#system_tables-text_log}

包含日志记录。 进入该表的日志记录级别可以通过`text_log.level` 服务器设置进行限制。

列:

-   `event_date` (Date) — 记录日期。
-   `event_time` (DateTime) — 记录时间。
-   `event_time_microseconds` (DateTime) — 记录时间，以微秒为单位。
-   `microseconds` (UInt32) — 记录微秒。
-   `thread_name` (String) — 日志记录的线程的名称。
-   `thread_id` (UInt64) — 操作系统线程ID。
-   `level` (`Enum8`) — 记录级别。可能的值：
    -   `1` 或 `'Fatal'`.
    -   `2` 或 `'Critical'`.
    -   `3` 或 `'Error'`.
    -   `4` 或 `'Warning'`.
    -   `5` 或 `'Notice'`.
    -   `6` 或 `'Information'`.
    -   `7` 或 `'Debug'`.
    -   `8` 或 `'Trace'`.
-   `query_id` (String) — 查询的ID。
-   `logger_name` (LowCardinality(String)) — 记录器的名称 (i.e. `DDLWorker`).
-   `message` (String) — 消息本身。
-   `revision` (UInt32) — ClickHouse修订版。
-   `source_file` (LowCardinality(String)) — 从中完成日志记录的源文件。
-   `source_line` (UInt64) — 从中完成日志记录的源代码行。

**示例**

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
 
 [原始文章](https://clickhouse.tech/docs/en/operations/system_tables/text_log) <!--hide-->
 