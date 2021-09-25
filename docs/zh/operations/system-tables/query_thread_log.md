---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。query_thread_log {#system_tables-query_thread_log}

包含有关执行查询的线程的信息，例如，线程名称、线程开始时间、查询处理的持续时间。

开始记录:

1.  在配置参数 [query_thread_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query_thread_log) 科。
2.  设置 [log_query_threads](../../operations/settings/settings.md#settings-log-query-threads) 到1。

数据的冲洗周期设置在 `flush_interval_milliseconds` 的参数 [query_thread_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query_thread_log) 服务器设置部分。 要强制冲洗，请使用 [SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs) 查询。

ClickHouse不会自动从表中删除数据。 看 [导言](../../operations/system-tables/index.md#system-tables-introduction) 欲了解更多详情。

列:

-   `event_date` ([日期](../../sql-reference/data-types/date.md)) — The date when the thread has finished execution of the query.
-   `event_time` ([日期时间](../../sql-reference/data-types/datetime.md)) — The date and time when the thread has finished execution of the query.
-   `query_start_time` ([日期时间](../../sql-reference/data-types/datetime.md)) — Start time of query execution.
-   `query_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Duration of query execution.
-   `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of read rows.
-   `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of read bytes.
-   `written_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — For `INSERT` 查询，写入的行数。 对于其他查询，列值为0。
-   `written_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — For `INSERT` 查询时，写入的字节数。 对于其他查询，列值为0。
-   `memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — The difference between the amount of allocated and freed memory in context of this thread.
-   `peak_memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — The maximum difference between the amount of allocated and freed memory in context of this thread.
-   `thread_name` ([字符串](../../sql-reference/data-types/string.md)) — Name of the thread.
-   `thread_number` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Internal thread ID.
-   `thread_id` ([Int32](../../sql-reference/data-types/int-uint.md)) — thread ID.
-   `master_thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — OS initial ID of initial thread.
-   `query` ([字符串](../../sql-reference/data-types/string.md)) — Query string.
-   `is_initial_query` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query for distributed query execution.
-   `user` ([字符串](../../sql-reference/data-types/string.md)) — Name of the user who initiated the current query.
-   `query_id` ([字符串](../../sql-reference/data-types/string.md)) — ID of the query.
-   `address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — IP address that was used to make the query.
-   `port` ([UInt16](../../sql-reference/data-types/int-uint.md#uint-ranges)) — The client port that was used to make the query.
-   `initial_user` ([字符串](../../sql-reference/data-types/string.md)) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` ([字符串](../../sql-reference/data-types/string.md)) — ID of the initial query (for distributed query execution).
-   `initial_address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — IP address that the parent query was launched from.
-   `initial_port` ([UInt16](../../sql-reference/data-types/int-uint.md#uint-ranges)) — The client port that was used to make the parent query.
-   `interface` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` ([字符串](../../sql-reference/data-types/string.md)) — OS's username who runs [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../../interfaces/cli.md).
-   `client_hostname` ([字符串](../../sql-reference/data-types/string.md)) — Hostname of the client machine where the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../../interfaces/cli.md) 或者运行另一个TCP客户端。
-   `client_name` ([字符串](../../sql-reference/data-types/string.md)) — The [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../../interfaces/cli.md) 或另一个TCP客户端名称。
-   `client_revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Revision of the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../../interfaces/cli.md) 或另一个TCP客户端。
-   `client_version_major` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Major version of the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../../interfaces/cli.md) 或另一个TCP客户端。
-   `client_version_minor` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Minor version of the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../../interfaces/cli.md) 或另一个TCP客户端。
-   `client_version_patch` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Patch component of the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../../interfaces/cli.md) 或另一个TCP客户端版本。
-   `http_method` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` 方法被使用。
    -   2 — `POST` 方法被使用。
-   `http_user_agent` ([字符串](../../sql-reference/data-types/string.md)) — The `UserAgent` http请求中传递的标头。
-   `quota_key` ([字符串](../../sql-reference/data-types/string.md)) — The “quota key” 在指定 [配额](../../operations/quotas.md) 设置（见 `keyed`).
-   `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ClickHouse revision.
-   `ProfileEvents` ([数组（字符串, UInt64)](../../sql-reference/data-types/array.md)) — Counters that measure different metrics for this thread. The description of them could be found in the table [系统。活动](#system_tables-events).

**示例**

``` sql
 SELECT * FROM system.query_thread_log LIMIT 1 FORMAT Vertical
```

``` text
Row 1:
──────
event_date:           2020-05-13
event_time:           2020-05-13 14:02:28
query_start_time:     2020-05-13 14:02:28
query_duration_ms:    0
read_rows:            1
read_bytes:           1
written_rows:         0
written_bytes:        0
memory_usage:         0
peak_memory_usage:    0
thread_name:          QueryPipelineEx
thread_id:            28952
master_thread_id:     28924
query:                SELECT 1
is_initial_query:     1
user:                 default
query_id:             5e834082-6f6d-4e34-b47b-cd1934f4002a
address:              ::ffff:127.0.0.1
port:                 57720
initial_user:         default
initial_query_id:     5e834082-6f6d-4e34-b47b-cd1934f4002a
initial_address:      ::ffff:127.0.0.1
initial_port:         57720
interface:            1
os_user:              bayonet
client_hostname:      clickhouse.ru-central1.internal
client_name:          ClickHouse client
client_revision:      54434
client_version_major: 20
client_version_minor: 4
client_version_patch: 1
http_method:          0
http_user_agent:
quota_key:
revision:             54434
ProfileEvents:        {'Query':1,'SelectQuery':1,'ReadCompressedBytes':36,'CompressedReadBufferBlocks':1,'CompressedReadBufferBytes':10,'IOBufferAllocs':1,'IOBufferAllocBytes':89,'ContextLock':15,'RWLockAcquiredReadLocks':1}
...
```

**另请参阅**

-   [系统。query_log](../../operations/system-tables/query_log.md#system_tables-query_log) — Description of the `query_log` 系统表，其中包含有关查询执行的公共信息。
