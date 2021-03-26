---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。query\_log {#system_tables-query_log}

包含有关已执行查询的信息，例如，开始时间、处理持续时间、错误消息。

!!! note "注"
    此表不包含以下内容的摄取数据 `INSERT` 查询。

您可以更改查询日志记录的设置 [query\_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query-log) 服务器配置部分。

您可以通过设置禁用查询日志记录 [log\_queries=0](../../operations/settings/settings.md#settings-log-queries). 我们不建议关闭日志记录，因为此表中的信息对于解决问题很重要。

数据的冲洗周期设置在 `flush_interval_milliseconds` 的参数 [query\_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query-log) 服务器设置部分。 要强制冲洗，请使用 [SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs) 查询。

ClickHouse不会自动从表中删除数据。 看 [导言](../../operations/system-tables/index.md#system-tables-introduction) 欲了解更多详情。

该 `system.query_log` 表注册两种查询:

1.  客户端直接运行的初始查询。
2.  由其他查询启动的子查询（用于分布式查询执行）。 对于这些类型的查询，有关父查询的信息显示在 `initial_*` 列。

每个查询创建一个或两个行中 `query_log` 表，这取决于状态（见 `type` 列）的查询:

1.  如果查询执行成功，则两行具有 `QueryStart` 和 `QueryFinish` 创建类型。
2.  如果在查询处理过程中发生错误，两个事件与 `QueryStart` 和 `ExceptionWhileProcessing` 创建类型。
3.  如果在启动查询之前发生错误，则单个事件具有 `ExceptionBeforeStart` 创建类型。

列:

-   `type` ([枚举8](../../sql-reference/data-types/enum.md)) — Type of an event that occurred when executing the query. Values:
    -   `'QueryStart' = 1` — Successful start of query execution.
    -   `'QueryFinish' = 2` — Successful end of query execution.
    -   `'ExceptionBeforeStart' = 3` — Exception before the start of query execution.
    -   `'ExceptionWhileProcessing' = 4` — Exception during the query execution.
-   `event_date` ([日期](../../sql-reference/data-types/date.md)) — Query starting date.
-   `event_time` ([日期时间](../../sql-reference/data-types/datetime.md)) — Query starting time.
-   `query_start_time` ([日期时间](../../sql-reference/data-types/datetime.md)) — Start time of query execution.
-   `query_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Duration of query execution in milliseconds.
-   `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Total number or rows read from all tables and table functions participated in query. It includes usual subqueries, subqueries for `IN` 和 `JOIN`. 对于分布式查询 `read_rows` 包括在所有副本上读取的行总数。 每个副本发送它的 `read_rows` 值，并且查询的服务器-发起方汇总所有接收到的和本地的值。 缓存卷不会影响此值。
-   `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Total number or bytes read from all tables and table functions participated in query. It includes usual subqueries, subqueries for `IN` 和 `JOIN`. 对于分布式查询 `read_bytes` 包括在所有副本上读取的行总数。 每个副本发送它的 `read_bytes` 值，并且查询的服务器-发起方汇总所有接收到的和本地的值。 缓存卷不会影响此值。
-   `written_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — For `INSERT` 查询，写入的行数。 对于其他查询，列值为0。
-   `written_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — For `INSERT` 查询时，写入的字节数。 对于其他查询，列值为0。
-   `result_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of rows in a result of the `SELECT` 查询，或者在一些行 `INSERT` 查询。
-   `result_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — RAM volume in bytes used to store a query result.
-   `memory_usage` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Memory consumption by the query.
-   `query` ([字符串](../../sql-reference/data-types/string.md)) — Query string.
-   `exception` ([字符串](../../sql-reference/data-types/string.md)) — Exception message.
-   `exception_code` ([Int32](../../sql-reference/data-types/int-uint.md)) — Code of an exception.
-   `stack_trace` ([字符串](../../sql-reference/data-types/string.md)) — [堆栈跟踪](https://en.wikipedia.org/wiki/Stack_trace). 如果查询成功完成，则为空字符串。
-   `is_initial_query` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query as part of distributed query execution.
-   `user` ([字符串](../../sql-reference/data-types/string.md)) — Name of the user who initiated the current query.
-   `query_id` ([字符串](../../sql-reference/data-types/string.md)) — ID of the query.
-   `address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — IP address that was used to make the query.
-   `port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — The client port that was used to make the query.
-   `initial_user` ([字符串](../../sql-reference/data-types/string.md)) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` ([字符串](../../sql-reference/data-types/string.md)) — ID of the initial query (for distributed query execution).
-   `initial_address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — IP address that the parent query was launched from.
-   `initial_port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — The client port that was used to make the parent query.
-   `interface` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` ([字符串](../../sql-reference/data-types/string.md)) — Operating system username who runs [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../../interfaces/cli.md).
-   `client_hostname` ([字符串](../../sql-reference/data-types/string.md)) — Hostname of the client machine where the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../../interfaces/cli.md) 或者运行另一个TCP客户端。
-   `client_name` ([字符串](../../sql-reference/data-types/string.md)) — The [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../../interfaces/cli.md) 或另一个TCP客户端名称。
-   `client_revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Revision of the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../../interfaces/cli.md) 或另一个TCP客户端。
-   `client_version_major` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Major version of the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../../interfaces/cli.md) 或另一个TCP客户端。
-   `client_version_minor` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Minor version of the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../../interfaces/cli.md) 或另一个TCP客户端。
-   `client_version_patch` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Patch component of the [ﾂ环板clientｮﾂ嘉ｯﾂ偲](../../interfaces/cli.md) 或另一个TCP客户端版本。
-   `http_method` (UInt8) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` 方法被使用。
    -   2 — `POST` 方法被使用。
-   `http_user_agent` ([字符串](../../sql-reference/data-types/string.md)) — The `UserAgent` http请求中传递的标头。
-   `quota_key` ([字符串](../../sql-reference/data-types/string.md)) — The “quota key” 在指定 [配额](../../operations/quotas.md) 设置（见 `keyed`).
-   `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ClickHouse revision.
-   `thread_numbers` ([数组(UInt32)](../../sql-reference/data-types/array.md)) — Number of threads that are participating in query execution.
-   `ProfileEvents.Names` ([数组（字符串)](../../sql-reference/data-types/array.md)) — Counters that measure different metrics. The description of them could be found in the table [系统。活动](../../operations/system-tables/events.md#system_tables-events)
-   `ProfileEvents.Values` ([数组(UInt64)](../../sql-reference/data-types/array.md)) — Values of metrics that are listed in the `ProfileEvents.Names` 列。
-   `Settings.Names` ([数组（字符串)](../../sql-reference/data-types/array.md)) — Names of settings that were changed when the client ran the query. To enable logging changes to settings, set the `log_query_settings` 参数为1。
-   `Settings.Values` ([数组（字符串)](../../sql-reference/data-types/array.md)) — Values of settings that are listed in the `Settings.Names` 列。

**示例**

``` sql
SELECT * FROM system.query_log LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
type:                 QueryStart
event_date:           2020-05-13
event_time:           2020-05-13 14:02:28
query_start_time:     2020-05-13 14:02:28
query_duration_ms:    0
read_rows:            0
read_bytes:           0
written_rows:         0
written_bytes:        0
result_rows:          0
result_bytes:         0
memory_usage:         0
query:                SELECT 1
exception_code:       0
exception:
stack_trace:
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
thread_ids:           []
ProfileEvents.Names:  []
ProfileEvents.Values: []
Settings.Names:       ['use_uncompressed_cache','load_balancing','log_queries','max_memory_usage']
Settings.Values:      ['0','random','1','10000000000']
```

**另请参阅**

-   [系统。query\_thread\_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log) — This table contains information about each query execution thread.
