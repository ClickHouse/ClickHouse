# 系统。query_thread_log {#system_tables-query_thread_log}

包含有关执行查询的线程的信息，例如，线程名称、线程开始时间、查询处理的持续时间。

开启日志功能:

1.  在配置参数 [query_thread_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query_thread_log) 部分。
2.  设置 [log_query_threads](../../operations/settings/settings.md#settings-log-query-threads) 为1。

数据从缓存写入数据表周期时间参数 `flush_interval_milliseconds` 位于 [query_thread_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query_thread_log) 服务器设置部分。如果需要强制从缓存写入数据表，请使用 [SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs) 查询请求。

ClickHouse不会自动从表中删除数据。 欲了解更多详情，请参照 [介绍](../../operations/system-tables/index.md#system-tables-introduction)。

列:

-   `event_date` ([日期](../../sql-reference/data-types/date.md)) — 该查询线程执行完成的日期。
-   `event_time` ([日期时间](../../sql-reference/data-types/datetime.md)) — 该查询线程执行完成的时间。
-   `query_start_time` ([日期时间](../../sql-reference/data-types/datetime.md)) — 查询的开始时间。
-   `query_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 查询执行持续的时间。
-   `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 读取的行数。
-   `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 读取的字节数。
-   `written_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 对于 `INSERT` 查询，写入的行数。 对于其他查询，为0。
-   `written_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 对于 `INSERT` 查询，写入的字节数。 对于其他查询，为0。
-   `memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — 在线程上下文，分配的内存和空闲内存之差。
-   `peak_memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — 在线程上下文，分配的内存和空闲内存之差的最大值。
-   `thread_name` ([字符串](../../sql-reference/data-types/string.md)) — 线程名。
-   `thread_number` ([UInt32](../../sql-reference/data-types/int-uint.md)) — 内部线程ID。
-   `thread_id` ([Int32](../../sql-reference/data-types/int-uint.md)) — 线程ID。
-   `master_thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — OS初始线程的初始ID。
-   `query` ([字符串](../../sql-reference/data-types/string.md)) — 查询语句。
-   `is_initial_query` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 查询类型，可能的值：
    -   1 — 由用户发起的查询。
    -   0 — 由其他查询发起的分布式查询。
-   `user` ([字符串](../../sql-reference/data-types/string.md)) — 发起查询的用户名。
-   `query_id` ([字符串](../../sql-reference/data-types/string.md)) — 查询的ID。
-   `address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — 发起查询的IP地址。
-   `port` ([UInt16](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 发起查询的端口。
-   `initial_user` ([字符串](../../sql-reference/data-types/string.md)) — 首次发起查询的用户名（对于分布式查询）。
-   `initial_query_id` ([字符串](../../sql-reference/data-types/string.md)) — 首次发起查询的ID（对于分布式查询）。
-   `initial_address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — 发起该查询的父查询IP地址。
-   `initial_port` ([UInt16](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 发起该查询的父查询端口。
-   `interface` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 发起查询的界面，可能的值:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` ([字符串](../../sql-reference/data-types/string.md)) — 使用 [clickhouse-client](../../interfaces/cli.md) 的系统用户名。
-   `client_hostname` ([字符串](../../sql-reference/data-types/string.md)) — 运行 [clickhouse-client](../../interfaces/cli.md) 或另一个TCP客户端的主机名。
-   `client_name` ([字符串](../../sql-reference/data-types/string.md)) — [clickhouse-client](../../interfaces/cli.md) 或另一个TCP客户端的名称。
-   `client_revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) 或另一个TCP客户端的修订号。
-   `client_version_major` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) 或另一个TCP客户端的主版本号。
-   `client_version_minor` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) 或另一个TCP客户端的次版本号。
-   `client_version_patch` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) 或另一个TCP客户端的补丁版本号。
-   `http_method` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 发起查询的HTTP方法，可能的值：
    -   0 — 查询通过TCP界面发起。
    -   1 — `GET` 方法被使用。
    -   2 — `POST` 方法被使用。
-   `http_user_agent` ([字符串](../../sql-reference/data-types/string.md)) — `UserAgent` HTTP请求中传递的UA表头。
-   `quota_key` ([字符串](../../sql-reference/data-types/string.md)) —  “quota key” 在 [配额](../../operations/quotas.md) 设置内（详见 `keyed`).
-   `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ClickHouse 修订版本号.
-   `ProfileEvents` ([数组（字符串, UInt64)](../../sql-reference/data-types/array.md)) — 对于该线程的多个指标计数器。这一项可以参考 [system.events](#system_tables-events).

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

-   [system.query_log](../../operations/system-tables/query_log.md#system_tables-query_log) — `query_log` 系统表描述，其中包含有关查询执行的公共信息。
-   [system.query_views_log](../../operations/system-tables/query_views_log.md#system_tables-query_views_log) — 这个表包含在查询线程中使用的各个视图的信息。
