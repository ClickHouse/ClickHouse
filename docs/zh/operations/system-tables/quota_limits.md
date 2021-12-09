# system.quota_limits {#system_tables-quota_limits}

包含关于所有配额的所有间隔的最大值的信息. 任何行数或0行都可以对应一个配额.

列信息:
- `quota_name` ([String](../../sql-reference/data-types/string.md)) — 配额名称.
- `duration` ([UInt32](../../sql-reference/data-types/int-uint.md)) — 计算资源消耗的时间间隔长度，单位为秒.
- `is_randomized_interval` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 逻辑值. 它显示了间隔是否是随机的. 如果间隔不是随机的, 它总是在同一时间开始. 例如, 1 分钟的间隔总是从整数分钟开始(即它可以从 11:20:00 开始, 但它永远不会从 11:20:01 开始), 一天的间隔总是从 UTC 午夜开始. 如果间隔是随机的, 则第一个间隔在随机时间开始, 随后的间隔一个接一个开始. 值:
- `0` — 区间不是随机的.
- `1` — 区间是随机的.
- `max_queries` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — 最大查询数.
- `max_query_selects` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — select 最大查询数.
- `max_query_inserts` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — insert 最大查询数.
- `max_errors` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — 最大错误数.
- `max_result_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — 最大结果行数.
- `max_result_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — 用于存储查询结果的最大RAM容量(以字节为单位).
- `max_read_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — 从参与查询的所有表和表函数中读取的最大行数.
- `max_read_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — 从参与查询的所有表和表函数中读取的最大字节数.
- `max_execution_time` ([Nullable](../../sql-reference/data-types/nullable.md)([Float64](../../sql-reference/data-types/float.md))) — 查询执行时间的最大值, 单位为秒.

[原始文章](https://clickhouse.com/docs/en/operations/system-tables/quota_limits) <!--hide-->
