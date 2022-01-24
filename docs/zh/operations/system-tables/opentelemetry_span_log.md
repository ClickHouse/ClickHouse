# system.opentelemetry_span_log {#system_tables-opentelemetry_span_log}

包含已执行查询的[跟踪范围](https://opentracing.io/docs/overview/spans/)的信息.

列信息:

-   `trace_id` ([UUID](../../sql-reference/data-types/uuid.md)) — 执行的查询的跟踪ID.

-   `span_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — `跟踪 跨度` ID.

-   `parent_span_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 父级`跟踪 跨度` ID.

-   `operation_name` ([String](../../sql-reference/data-types/string.md)) — 操作的名称.

-   `start_time_us` ([UInt64](../../sql-reference/data-types/int-uint.md)) — `跟踪 跨度` 开始时间 (微秒).

-   `finish_time_us` ([UInt64](../../sql-reference/data-types/int-uint.md)) — `跟踪 跨度 结束时间 (微秒).

-   `finish_date` ([Date](../../sql-reference/data-types/date.md)) — `跟踪 跨度` 完成日期.

-   `attribute.names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — [属性](https://opentelemetry.io/docs/go/instrumentation/#attributes) 名称取决于 `跟踪 跨度`. 它们是根据[OpenTelemetry](https://opentelemetry.io/)标准中的建议填写的.

-   `attribute.values` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 属性值取决于 `跟踪 跨度`. 它们是根据 `OpenTelemetry` 标准中的建议填写的.

**示例**

查询:

``` sql
SELECT * FROM system.opentelemetry_span_log LIMIT 1 FORMAT Vertical;
```

结果:

``` text
Row 1:
──────
trace_id:         cdab0847-0d62-61d5-4d38-dd65b19a1914
span_id:          701487461015578150
parent_span_id:   2991972114672045096
operation_name:   DB::Block DB::InterpreterSelectQuery::getSampleBlockImpl()
start_time_us:    1612374594529090
finish_time_us:   1612374594529108
finish_date:      2021-02-03
attribute.names:  []
attribute.values: []
```

**另请参阅**

-   [OpenTelemetry](../../operations/opentelemetry.md)

[原始文章](https://clickhouse.com/docs/en/operations/system_tables/opentelemetry_span_log) <!--hide-->
