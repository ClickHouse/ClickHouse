---
slug: /zh/operations/system-tables/query_log
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# system.query_log {#system_tables-query_log}

包含已执行查询的相关信息，例如：开始时间、处理持续时间、错误消息。

:::note
此表不包含以下内容的摄取数据 `INSERT` 查询。
:::

您可以更改query_log的设置，在服务器配置的 [query_log](../../operations/server-configuration-parameters/settings.md/operations/server-configuration-parameters/settings#query-log) 部分。

您可以通过设置 [log_queries=0](../../operations/settings/settings.md#settings-log-queries)来禁用query_log. 我们不建议关闭此日志，因为此表中的信息对于解决问题很重要。

数据刷新的周期可通过 `flush_interval_milliseconds` 参数来设置 [query_log](../../operations/server-configuration-parameters/settings.md/operations/server-configuration-parameters/settings#query-log) 。 要强制刷新，请使用 [SYSTEM FLUSH LOGS](/sql-reference/statements/system#flush-logs)。

ClickHouse不会自动从表中删除数据。更多详情请看 [introduction](../../operations/system-tables/index.md#system-tables-introduction) 。

`system.query_log` 表注册两种查询:

1.  客户端直接运行的初始查询。
2.  由其他查询启动的子查询（用于分布式查询执行）。 对于这些类型的查询，有关父查询的信息显示在 `initial_*` 列。

每个查询在`query_log` 表中创建一或两行记录，这取决于查询的状态（见 `type` 列）:

1.  如果查询执行成功，会创建type分别为`QueryStart` 和 `QueryFinish` 的两行记录。
2.  如果在查询处理过程中发生错误，会创建type分别为`QueryStart` 和 `ExceptionWhileProcessing` 的两行记录。
3.  如果在启动查询之前发生错误，则创建一行type为`ExceptionBeforeStart` 的记录。

列:

-   `type` ([Enum8](../../sql-reference/data-types/enum.md)) — 执行查询时的事件类型. 值:
    -   `'QueryStart' = 1` — 查询成功启动.
    -   `'QueryFinish' = 2` — 查询成功完成.
    -   `'ExceptionBeforeStart' = 3` — 查询执行前有异常.
    -   `'ExceptionWhileProcessing' = 4` — 查询执行期间有异常.
-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — 查询开始日期.
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 查询开始时间.
-   `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — 查询开始时间（毫秒精度）.
-   `query_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 查询执行的开始时间.
-   `query_start_time_microseconds` (DateTime64) — 查询执行的开始时间（毫秒精度）.
-   `query_duration_ms` ([UInt64](/sql-reference/data-types/int-uint#integer-ranges)) — 查询消耗的时间（毫秒）.
-   `read_rows` ([UInt64](/sql-reference/data-types/int-uint#integer-ranges)) — 从参与了查询的所有表和表函数读取的总行数. 包括：普通的子查询,  `IN` 和 `JOIN`的子查询. 对于分布式查询 `read_rows` 包括在所有副本上读取的行总数。 每个副本发送它的 `read_rows` 值，并且查询的服务器-发起方汇总所有接收到的和本地的值。 缓存卷不会影响此值。
-   `read_bytes` ([UInt64](/sql-reference/data-types/int-uint#integer-ranges)) — 从参与了查询的所有表和表函数读取的总字节数. 包括：普通的子查询,  `IN` 和 `JOIN`的子查询. 对于分布式查询 `read_bytes` 包括在所有副本上读取的字节总数。 每个副本发送它的 `read_bytes` 值，并且查询的服务器-发起方汇总所有接收到的和本地的值。 缓存卷不会影响此值。
-   `written_rows` ([UInt64](/sql-reference/data-types/int-uint#integer-ranges)) — 对于 `INSERT` 查询，为写入的行数。 对于其他查询，值为0。
-   `written_bytes` ([UInt64](/sql-reference/data-types/int-uint#integer-ranges)) — 对于 `INSERT` 查询时，为写入的字节数。 对于其他查询，值为0。
-   `result_rows` ([UInt64](/sql-reference/data-types/int-uint#integer-ranges)) — `SELECT` 查询结果的行数，或`INSERT` 的行数。
-   `result_bytes` ([UInt64](/sql-reference/data-types/int-uint#integer-ranges)) — 存储查询结果的RAM量.
-   `memory_usage` ([UInt64](/sql-reference/data-types/int-uint#integer-ranges)) — 查询使用的内存.
-   `query` ([String](../../sql-reference/data-types/string.md)) — 查询语句.
-   `exception` ([String](../../sql-reference/data-types/string.md)) — 异常信息.
-   `exception_code` ([Int32](../../sql-reference/data-types/int-uint.md)) — 异常码.
-   `stack_trace` ([String](../../sql-reference/data-types/string.md)) — [Stack Trace](https://en.wikipedia.org/wiki/Stack_trace). 如果查询成功完成，则为空字符串。
-   `is_initial_query` ([UInt8](../../sql-reference/data-types/int-uint.md)) — 查询类型. 可能的值:
    -   1 — 客户端发起的查询.
    -   0 — 由另一个查询发起的，作为分布式查询的一部分.
-   `user` ([String](../../sql-reference/data-types/string.md)) — 发起查询的用户.
-   `query_id` ([String](../../sql-reference/data-types/string.md)) — 查询ID.
-   `address` ([IPv6](../../sql-reference/data-types/ipv6.md)) — 发起查询的客户端IP地址.
-   `port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — 发起查询的客户端端口.
-   `initial_user` ([String](../../sql-reference/data-types/string.md)) — 初始查询的用户名（用于分布式查询执行）.
-   `initial_query_id` ([String](../../sql-reference/data-types/string.md)) — 运行初始查询的ID（用于分布式查询执行）.
-   `initial_address` ([IPv6](../../sql-reference/data-types/ipv6.md)) — 运行父查询的IP地址.
-   `initial_port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — 发起父查询的客户端端口.
-   `interface` ([UInt8](../../sql-reference/data-types/int-uint.md)) — 发起查询的接口. 可能的值:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` ([String](../../sql-reference/data-types/string.md)) — 运行 [clickhouse-client](../../interfaces/cli.md)的操作系统用户名.
-   `client_hostname` ([String](../../sql-reference/data-types/string.md)) — 运行[clickhouse-client](../../interfaces/cli.md) 或其他TCP客户端的机器的主机名。
-   `client_name` ([String](../../sql-reference/data-types/string.md)) — [clickhouse-client](../../interfaces/cli.md) 或其他TCP客户端的名称。
-   `client_revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) 或其他TCP客户端的Revision。
-   `client_version_major` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) 或其他TCP客户端的Major version。
-   `client_version_minor` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) 或其他TCP客户端的Minor version。
-   `client_version_patch` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) 或其他TCP客户端的Patch component。
-   `http_method` (UInt8) — 发起查询的HTTP方法. 可能值:
    -   0 — TCP接口的查询.
    -   1 — `GET`
    -   2 — `POST`
-   `http_user_agent` ([String](../../sql-reference/data-types/string.md)) — The `UserAgent` The UserAgent header passed in the HTTP request。
-   `quota_key` ([String](../../sql-reference/data-types/string.md)) — 在[quotas](../../operations/quotas.md) 配置里设置的“quota key” （见 `keyed`).
-   `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ClickHouse revision.
-   `ProfileEvents` ([Map(String, UInt64))](../../sql-reference/data-types/array.md)) — Counters that measure different metrics. The description of them could be found in the table [系统。活动](/operations/system-tables/events)
-   `Settings` ([Map(String, String)](../../sql-reference/data-types/array.md)) — Names of settings that were changed when the client ran the query. To enable logging changes to settings, set the `log_query_settings` 参数为1。
-   `thread_ids` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — 参与查询的线程数.
-   `Settings.Names` ([Array（String)](../../sql-reference/data-types/array.md)) — 客户端运行查询时更改的设置的名称。 要启用对设置的日志记录更改，请将log_query_settings参数设置为1。
-   `Settings.Values` ([Array（String)](../../sql-reference/data-types/array.md)) — `Settings.Names` 列中列出的设置的值。
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
ProfileEvents:        {'Query':1,'SelectQuery':1,'ReadCompressedBytes':36,'CompressedReadBufferBlocks':1,'CompressedReadBufferBytes':10,'IOBufferAllocs':1,'IOBufferAllocBytes':89,'ContextLock':15,'RWLockAcquiredReadLocks':1}
Settings:             {'background_pool_size':'32','load_balancing':'random','allow_suspicious_low_cardinality_types':'1','distributed_aggregation_memory_efficient':'1','skip_unavailable_shards':'1','log_queries':'1','max_bytes_before_external_group_by':'20000000000','max_bytes_before_external_sort':'20000000000','allow_introspection_functions':'1'}
```

**另请参阅**

-   [system.query_thread_log](/operations/system-tables/query_thread_log) — 这个表包含了每个查询执行线程的信息
