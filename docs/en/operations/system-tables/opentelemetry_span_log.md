# system.opentelemetry_span_log {#system_tables-opentelemetry_span_log}

Contains information about [trace spans](https://opentracing.io/docs/overview/spans/) for executed queries.

Columns:

-   `trace_id` ([UUID](../../sql-reference/data-types/uuid.md)) — ID of the trace for executed query.

-   `span_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — ID of the `trace span`.

-   `parent_span_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — ID of the parent `trace span`.

-   `operation_name` ([String](../../sql-reference/data-types/string.md)) — The name of the operation.

-   `start_time_us` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The start time of the `trace span` (in microseconds).

-   `finish_time_us` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The finish time of the `trace span` (in microseconds).

-   `finish_date` ([Date](../../sql-reference/data-types/date.md)) — The finish date of the `trace span`.

-   `attribute.names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — [Attribute](https://opentelemetry.io/docs/go/instrumentation/#attributes) names depending on the `trace span`. They are filled in according to the recommendations in the [OpenTelemetry](https://opentelemetry.io/) standard.

-   `attribute.values` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Attribute values depending on the `trace span`. They are filled in according to the recommendations in the `OpenTelemetry` standard.

**Example**

Query:

``` sql
SELECT * FROM system.opentelemetry_span_log LIMIT 1 FORMAT Vertical;
```

Result:

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

**See Also**

-   [OpenTelemetry](../../operations/opentelemetry.md)

[Original article](https://clickhouse.com/docs/en/operations/system_tables/opentelemetry_span_log) <!--hide-->
