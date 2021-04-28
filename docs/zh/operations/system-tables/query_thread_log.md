# system.query_thread_log {#system_tables-query_thread_log}

包含有关执行查询的线程的信息，例如，线程名称、线程开始时间、查询处理的持续时间。

开始记录:

1.  在[query_thread_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query_thread_log) 部分中配置参数。
2.  将[log_query_threads](../../operations/settings/settings.md#settings-log-query-threads) 设置为1。

[query_thread_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query_thread_log) 数据的刷新的周期可通过 `flush_interval_milliseconds` 参数来设置。 要强制刷新，请使用 [SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs) 查询。

ClickHouse不会自动从表中删除数据。 有关更多详细信息，请参见 [介绍](../../operations/system-tables/index.md#system-tables-introduction) 。

列:

-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — 线程完成查询执行的日期。
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 线程完成查询执行的时间。
-   `query_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 查询执行的开始时间。
-   `query_start_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — 查询执行的开始时间，以微秒为单位。
-   `query_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 查询执行的持续时间。
-   `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 读取的行数。
-   `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 读取的字节数。
-   `written_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — For `INSERT` 查询，写入的行数。 对于其他查询，列值为0。
-   `written_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — For `INSERT` 查询时，写入的字节数。 对于其他查询，列值为0。
-   `memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — 在此线程的上下文中分配的内存和释放的内存之间的差。
-   `peak_memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — 在此线程的上下文中分配的内存和已释放的内存之间的最大差异。
-   `thread_name` ([String](../../sql-reference/data-types/string.md)) — 线程的名称。
-   `thread_number` ([UInt32](../../sql-reference/data-types/int-uint.md)) — 内部线程ID。
-   `thread_id` ([Int32](../../sql-reference/data-types/int-uint.md)) — 线程ID。
-   `master_thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 初始线程的OS初始ID。
-   `query` ([String](../../sql-reference/data-types/string.md)) — 查询语句。
-   `is_initial_query` ([UInt8](../../sql-reference/data-types/int-uint.md)) — 查询类型。可能的值:
    -   1 — 客户端发起的查询。
    -   0 — 由另一个查询发起的，作为分布式查询的一部分。
-   `user` ([String](../../sql-reference/data-types/string.md)) — 发起查询的用户。
-   `query_id` ([String](../../sql-reference/data-types/string.md)) — 查询ID。
-   `address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — 发起查询的客户端IP地址。
-   `port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — 发起查询的客户端端口。
-   `initial_user` ([String](../../sql-reference/data-types/string.md)) — 父查询的用户名（用于分布式查询执行）。
-   `initial_query_id` ([String](../../sql-reference/data-types/string.md)) — 运行父查询的ID（用于分布式查询执行）。
-   `initial_address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — 运行父查询的IP地址。
-   `initial_port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — 发起父查询的客户端端口。
-   `interface` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 发起查询的接口。可能的值:
    -   1 — TCP。
    -   2 — HTTP。
-   `os_user` ([String](../../sql-reference/data-types/string.md)) — 运行 [clickhouse-client](../../interfaces/cli.md)的操作系统用户名。
-   `client_hostname` ([String](../../sql-reference/data-types/string.md)) — 运行[clickhouse-client](../../interfaces/cli.md) 或其他TCP客户端的机器的主机名。
-   `client_name` ([String](../../sql-reference/data-types/string.md)) — [clickhouse-client](../../interfaces/cli.md) 或其他TCP客户端的名称。
-   `client_revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) 或其他TCP客户端的内部版本。
-   `client_version_major` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) 或其他TCP客户端的主要版本。
-   `client_version_minor` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) 或其他TCP客户端的次要版本。
-   `client_version_patch` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) 或其他TCP客户端的修补组件。
-   `http_method` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 发起查询的HTTP方法。 可能值:
    -   0 — 查询是从TCP接口发起的。
    -   1 — 使用 `GET` 方法。
    -   2 — 使用 `POST` 方法。
-   `http_user_agent` ([String](../../sql-reference/data-types/string.md)) — HTTP请求中传递的标头 `UserAgent` 。
-   `quota_key` ([String](../../sql-reference/data-types/string.md)) — 在[配额](../../operations/quotas.md)设置中指定的 “quota key”（请参阅 `keyed`)。
-   `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ClickHouse版本。
-   `ProfileEvents.Names` ([Array(String)](../../sql-reference/data-types/array.md)) — 计数器，用于为此线程测量不同的指标。可以在表 [system.events](#system_tables-events)中找到它们的描述。
-   `ProfileEvents.Values` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — `ProfileEvents.Names` 列中列出的该线程的指标值。

**示例**

``` sql
 SELECT * FROM system.query_thread_log LIMIT 1 \G
```

``` text
Row 1:
──────
event_date:                    2020-09-11
event_time:                    2020-09-11 10:08:17
event_time_microseconds:       2020-09-11 10:08:17.134042
query_start_time:              2020-09-11 10:08:17
query_start_time_microseconds: 2020-09-11 10:08:17.063150
query_duration_ms:             70
read_rows:                     0
read_bytes:                    0
written_rows:                  1
written_bytes:                 12
memory_usage:                  4300844
peak_memory_usage:             4300844
thread_name:                   TCPHandler
thread_id:                     638133
master_thread_id:              638133
query:                         INSERT INTO test1 VALUES
is_initial_query:              1
user:                          default
query_id:                      50a320fd-85a8-49b8-8761-98a86bcbacef
address:                       ::ffff:127.0.0.1
port:                          33452
initial_user:                  default
initial_query_id:              50a320fd-85a8-49b8-8761-98a86bcbacef
initial_address:               ::ffff:127.0.0.1
initial_port:                  33452
interface:                     1
os_user:                       bharatnc
client_hostname:               tower
client_name:                   ClickHouse 
client_revision:               54437
client_version_major:          20
client_version_minor:          7
client_version_patch:          2
http_method:                   0
http_user_agent:               
quota_key:                     
revision:                      54440
ProfileEvents.Names:           ['Query','InsertQuery','FileOpen','WriteBufferFromFileDescriptorWrite','WriteBufferFromFileDescriptorWriteBytes','ReadCompressedBytes','CompressedReadBufferBlocks','CompressedReadBufferBytes','IOBufferAllocs','IOBufferAllocBytes','FunctionExecute','CreatedWriteBufferOrdinary','DiskWriteElapsedMicroseconds','NetworkReceiveElapsedMicroseconds','NetworkSendElapsedMicroseconds','InsertedRows','InsertedBytes','SelectedRows','SelectedBytes','MergeTreeDataWriterRows','MergeTreeDataWriterUncompressedBytes','MergeTreeDataWriterCompressedBytes','MergeTreeDataWriterBlocks','MergeTreeDataWriterBlocksAlreadySorted','ContextLock','RWLockAcquiredReadLocks','RealTimeMicroseconds','UserTimeMicroseconds','SoftPageFaults','OSCPUVirtualTimeMicroseconds','OSWriteBytes','OSReadChars','OSWriteChars']
ProfileEvents.Values:          [1,1,11,11,591,148,3,71,29,6533808,1,11,72,18,47,1,12,1,12,1,12,189,1,1,10,2,70853,2748,49,2747,45056,422,1520]
```

**另请参阅**

-   [system.query_log](../../operations/system-tables/query_log.md#system_tables-query_log) —  `query_log` 系统表的描述，其中包含有关查询执行的常见信息。

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/query_thread_log) <!--hide-->
