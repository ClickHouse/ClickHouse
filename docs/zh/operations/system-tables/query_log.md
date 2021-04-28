# system.query_log {#system_tables-query_log}

包含已执行查询的相关信息，例如：开始时间、处理持续时间、错误消息。

!!! note "注意"
    此表不包含 `INSERT` 查询的获取数据。

您可以在服务器配置的 [query_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query-log) 部分中更改查询日志记录表query_log的设置。

您可以通过设置 [log_queries=0](../../operations/settings/settings.md#settings-log-queries)来禁用query_log。我们不建议关闭此日志，因为此表中的信息对于解决问题很重要。

[query_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query-log)数据刷新的周期可通过 `flush_interval_milliseconds` 参数来设置。 要强制刷新，请使用 [SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs) 查询。

ClickHouse不会自动从表中删除数据。有关更多详细信息，请参见 [introduction](../../operations/system-tables/index.md#system-tables-introduction) 。

`system.query_log` 表注册两种查询:

1.  客户端直接运行的初始查询。
2.  由其他查询启动的子查询（用于分布式查询执行）。 对于这些类型的查询，有关父查询的信息显示在 `initial_*` 列。

每个查询在`query_log` 表中创建一或两行记录，这取决于查询的状态（请参见 `type` 列）:

1.  如果查询执行成功，会创建type分别为 `QueryStart` 和 `QueryFinish` 的两行记录。
2.  如果在查询处理过程中发生错误，会创建type分别为` QueryStart` 和 `ExceptionWhileProcessing` 的两行记录。
3.  如果在启动查询之前发生错误，则创建一行type为 `ExceptionBeforeStart` 的记录。

列:

-   `type` ([Enum8](../../sql-reference/data-types/enum.md)) — 执行查询时的事件类型. 值:
    -   `'QueryStart' = 1` — 查询成功开始执行。
    -   `'QueryFinish' = 2` — 查询成功完成执行。
    -   `'ExceptionBeforeStart' = 3` — 查询执行前有异常。
    -   `'ExceptionWhileProcessing' = 4` — 查询执行期间有异常。
-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — 查询开始日期。
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 查询开始时间。
-   `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — 查询开始时间（微秒）。
-   `query_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 查询执行的开始时间。
-   `query_start_time_microseconds` (DateTime64) — 查询执行的开始时间（微秒）。
-   `query_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 查询执行的时间（毫秒）。
-   `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 从参与了查询的所有表和表函数读取的总行数. 包括：普通的子查询,  `IN` 和 `JOIN`的子查询。对于分布式查询 `read_rows` 包括在所有副本上读取的行总数。 每个副本发送它的 `read_rows` 值，并且查询的服务器-发起方汇总所有接收到的和本地的值。 缓存卷不会影响此值。
-   `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 从参与了查询的所有表和表函数读取的总字节数. 包括：普通的子查询,  `IN` 和 `JOIN`的子查询。 对于分布式查询 `read_bytes` 包括在所有副本上读取的字节总数。 每个副本发送它的 `read_bytes` 值，并且查询的服务器-发起方汇总所有接收到的和本地的值。 缓存卷不会影响此值。
-   `written_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 对于 `INSERT` 查询，为写入的行数。 对于其他查询，值为0。
-   `written_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 对于 `INSERT` 查询时，为写入的字节数。 对于其他查询，值为0。
-   `result_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — `SELECT` 查询结果的行数，或`INSERT` 查询中的行数。
-   `result_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 存储查询结果的RAM量（以字节为单位）。
-   `memory_usage` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 查询使用的内存。
-   `query` ([String](../../sql-reference/data-types/string.md)) — 查询语句。
-   `exception_code` ([Int32](../../sql-reference/data-types/int-uint.md)) — 异常代码。
-   `exception` ([String](../../sql-reference/data-types/string.md)) — 异常信息。
-   `stack_trace` ([String](../../sql-reference/data-types/string.md)) — [堆栈跟踪](https://en.wikipedia.org/wiki/Stack_trace). 如果查询成功完成，则为空字符串。
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
-   `interface` ([UInt8](../../sql-reference/data-types/int-uint.md)) — 发起查询的接口。可能的值:
    -   1 — TCP。
    -   2 — HTTP。
-   `os_user` ([String](../../sql-reference/data-types/string.md)) — 运行 [clickhouse-client](../../interfaces/cli.md)的操作系统用户名。
-   `client_hostname` ([String](../../sql-reference/data-types/string.md)) — 运行[clickhouse-client](../../interfaces/cli.md) 或其他TCP客户端的机器的主机名。
-   `client_name` ([String](../../sql-reference/data-types/string.md)) — [clickhouse-client](../../interfaces/cli.md) 或其他TCP客户端的名称。
-   `client_revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) 或其他TCP客户端的内部版本。
-   `client_version_major` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) 或其他TCP客户端的主要版本。
-   `client_version_minor` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) 或其他TCP客户端的次要版本。
-   `client_version_patch` ([UInt32](../../sql-reference/data-types/int-uint.md)) — [clickhouse-client](../../interfaces/cli.md) 或其他TCP客户端的修补组件。
-   `http_method` (UInt8) — 发起查询的HTTP方法。 可能值:
    -   0 — TCP接口的查询。
    -   1 — 使用 `GET` 方法。
    -   2 — 使用 `POST` 方法。
-   `http_user_agent` ([String](../../sql-reference/data-types/string.md)) — 在HTTP查询中传递的HTTP标头 `UserAgent`。
-   `http_referer` ([String](../../sql-reference/data-types/string.md)) — 在HTTP查询中传递的HTTP标头 `Referer` (包含进行查询的页面的绝对或部分地址)。
-   `forwarded_for` ([String](../../sql-reference/data-types/string.md)) — 在HTTP查询中传递的HTTP标头 `X-Forwarded-For`。
-   `quota_key` ([String](../../sql-reference/data-types/string.md)) — 在[quotas](../../operations/quotas.md) 配置里设置的 `quota key`（请参阅 `keyed`)。
-   `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ClickHouse版本。
-   `thread_numbers` ([Array(UInt32)](../../sql-reference/data-types/array.md)) — 参与查询执行的线程数。
-   `ProfileEvents.Names` ([Array（String)](../../sql-reference/data-types/array.md)) — 衡量不同指标的计数器。 可以在表[system.events](../../operations/system-tables/events.md#system_tables-events)中找到它们的描述。
-   `ProfileEvents.Values` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — `ProfileEvents.Names` 列中列出的指标的值。
-   `Settings.Names` ([Array（String)](../../sql-reference/data-types/array.md)) — 客户端运行查询时更改的设置的名称。 要启用对设置的日志记录更改，请将 `log_query_settings` 参数设置为1。
-   `Settings.Values` ([Array（String)](../../sql-reference/data-types/array.md)) — `Settings.Names` 列中列出的设置的值。
-   `used_aggregate_functions` ([Array(String)](../../sql-reference/data-types/array.md)) — `aggregate functions` 的规范名称，在查询执行期间使用。
-   `used_aggregate_function_combinators` ([Array(String)](../../sql-reference/data-types/array.md)) — `aggregate functions combinators` 的规范名称，在查询执行期间使用。
-   `used_database_engines` ([Array(String)](../../sql-reference/data-types/array.md)) — `database engines` 的规范名称，在查询执行期间使用。
-   `used_data_type_families` ([Array(String)](../../sql-reference/data-types/array.md)) — `data type families` 的规范名称，在查询执行期间使用。
-   `used_dictionaries` ([Array(String)](../../sql-reference/data-types/array.md)) — `dictionaries` 的规范名称，在查询执行期间使用。
-   `used_formats` ([Array(String)](../../sql-reference/data-types/array.md)) — `formats` 的规范名称，在查询执行期间使用。
-   `used_functions` ([Array(String)](../../sql-reference/data-types/array.md)) — `functions` 的规范名称，在查询执行期间使用。
-   `used_storages` ([Array(String)](../../sql-reference/data-types/array.md)) —  `storages` 的规范名称，在查询执行期间使用。
-   `used_table_functions` ([Array(String)](../../sql-reference/data-types/array.md)) — `table functions` 的规范名称，在查询执行期间使用。

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

-   [system.query_thread_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log) — 这个表包含了每个查询执行线程的信息

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/query_log) <!--hide-->
