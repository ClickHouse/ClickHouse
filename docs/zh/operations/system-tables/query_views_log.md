# system.query_views_log {#system_tables-query_views_log}

包含有关运行查询时执行的从属视图的信息，例如视图类型或执行时间.

开始记录:

1.  在 [query_views_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query_views_log) 部分配置参数.
2.  设置 [log_query_views](../../operations/settings/settings.md#settings-log-query-views) 为 1.

数据的刷新周期是在[query_views_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query_views_log)服务器设置部分的 `flush_interval_milliseconds` 参数中设置的. 要强制刷新，请使用[SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs)查询.

ClickHouse不会自动从表中删除数据. 详见 [Introduction](../../operations/system-tables/index.md#system-tables-introduction).

您可以使用[log_queries_probability](../../operations/settings/settings.md#log-queries-probability)设置来减少在 `query_views_log` 表中注册的查询数量.

列信息:

-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — 视图的最后一个事件发生的日期.
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 视图完成执行的日期和时间.
-   `event_time_microseconds` ([DateTime](../../sql-reference/data-types/datetime.md)) — 视图以微秒精度完成执行的日期和时间.
-   `view_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 视图执行的持续时间(各阶段之和), 以毫秒为单位.
-   `initial_query_id` ([String](../../sql-reference/data-types/string.md)) — 初始查询的ID (用于分布式查询执行).
-   `view_name` ([String](../../sql-reference/data-types/string.md)) — 视图名称.
-   `view_uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — 视图的UUID.
-   `view_type` ([Enum8](../../sql-reference/data-types/enum.md)) — 视图类型. 值:
    -   `'Default' = 1` — [Default views](../../sql-reference/statements/create/view.md#normal). 不应该出现在日志中.
    -   `'Materialized' = 2` — [Materialized views](../../sql-reference/statements/create/view.md#materialized).
    -   `'Live' = 3` — [Live views](../../sql-reference/statements/create/view.md#live-view).
-   `view_query` ([String](../../sql-reference/data-types/string.md)) — 视图执行的查询.
-   `view_target` ([String](../../sql-reference/data-types/string.md)) — 视图目标表的名称.
-   `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 读行数.
-   `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 读字节数.
-   `written_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 写入行数.
-   `written_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 写入字节数.
-   `peak_memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — 在此视图上下文中, 已分配内存和已释放内存之间的最大差值.
-   `ProfileEvents` ([Map(String, UInt64)](../../sql-reference/data-types/array.md)) — ProfileEvents度量不同的指标. 它们的描述可以在表 [system.events](../../operations/system-tables/events.md#system_tables-events) 中找到.
-   `status` ([Enum8](../../sql-reference/data-types/enum.md)) — 视图状态. 值:
    -   `'QueryStart' = 1` — 成功启动视图执行. 不应该出现.
    -   `'QueryFinish' = 2` — 视图执行成功结束.
    -   `'ExceptionBeforeStart' = 3` — 视图执行开始前的异常.
    -   `'ExceptionWhileProcessing' = 4` — 视图执行期间的异常.
-   `exception_code` ([Int32](../../sql-reference/data-types/int-uint.md)) — 异常代码.
-   `exception` ([String](../../sql-reference/data-types/string.md)) — 异常报文.
-   `stack_trace` ([String](../../sql-reference/data-types/string.md)) — [堆栈跟踪](https://en.wikipedia.org/wiki/Stack_trace). 如果查询成功完成, 则为空字符串.

**示例**

查询:

``` sql
SELECT * FROM system.query_views_log LIMIT 1 \G;
```

结果:

``` text
Row 1:
──────
event_date:              2021-06-22
event_time:              2021-06-22 13:23:07
event_time_microseconds: 2021-06-22 13:23:07.738221
view_duration_ms:        0
initial_query_id:        c3a1ac02-9cad-479b-af54-9e9c0a7afd70
view_name:               default.matview_inner
view_uuid:               00000000-0000-0000-0000-000000000000
view_type:               Materialized
view_query:              SELECT * FROM default.table_b
view_target:             default.`.inner.matview_inner`
read_rows:               4
read_bytes:              64
written_rows:            2
written_bytes:           32
peak_memory_usage:       4196188
ProfileEvents:           {'FileOpen':2,'WriteBufferFromFileDescriptorWrite':2,'WriteBufferFromFileDescriptorWriteBytes':187,'IOBufferAllocs':3,'IOBufferAllocBytes':3145773,'FunctionExecute':3,'DiskWriteElapsedMicroseconds':13,'InsertedRows':2,'InsertedBytes':16,'SelectedRows':4,'SelectedBytes':48,'ContextLock':16,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':698,'SoftPageFaults':4,'OSReadChars':463}
status:                  QueryFinish
exception_code:          0
exception:
stack_trace:
```

**另请参阅**

-   [system.query_log](../../operations/system-tables/query_log.md#system_tables-query_log) — 包含查询执行的常用信息的 `query_log`系统表的描述.
-   [system.query_thread_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log) — 包含关于每个查询执行线程的信息.

[原始文章](https://clickhouse.com/docs/en/operations/system_tables/query_thread_log) <!--hide-->
