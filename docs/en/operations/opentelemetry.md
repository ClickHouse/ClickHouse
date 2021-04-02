---
toc_priority: 62
toc_title: OpenTelemetry Support
---

# [experimental] OpenTelemetry Support

[OpenTelemetry](https://opentelemetry.io/) is an open standard for collecting
traces and metrics from distributed application. ClickHouse has some support
for OpenTelemetry.

!!! warning "Warning"
This is an experimental feature that will change in backwards-incompatible ways in the future releases.


## Supplying Trace Context to ClickHouse

ClickHouse accepts trace context HTTP headers, as described by
the [W3C recommendation](https://www.w3.org/TR/trace-context/).
It also accepts trace context over native protocol that is used for
communication between ClickHouse servers or between the client and server.
For manual testing, trace context headers conforming to the Trace Context
recommendation can be supplied to `clickhouse-client` using
`--opentelemetry-traceparent` and `--opentelemetry-tracestate` flags.

If no parent trace context is supplied, ClickHouse can start a new trace, with
probability controlled by the `opentelemetry_start_trace_probability` setting.


## Propagating the Trace Context

The trace context is propagated to downstream services in the following cases:

* Queries to remote ClickHouse servers, such as when using `Distributed` table
  engine.

* `URL` table function. Trace context information is sent in HTTP headers.


## Tracing the ClickHouse Itself

ClickHouse creates _trace spans_ for each query and some of the query execution
stages, such as query planning or distributed queries.

To be useful, the tracing information has to be exported to a monitoring system
that supports OpenTelemetry, such as Jaeger or Prometheus. ClickHouse avoids
a dependency on a particular monitoring system, instead only providing the
tracing data through a system table. OpenTelemetry trace span information
[required by the standard](https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/overview.md#span)
is stored in the `system.opentelemetry_span_log` table.

The table must be enabled in the server configuration, see the `opentelemetry_span_log`
element in the default config file `config.xml`. It is enabled by default.

The table has the following columns:

- `trace_id` 
- `span_id`
- `parent_span_id`
- `operation_name`
- `start_time`
- `finish_time`
- `finish_date`
- `attribute.name`
- `attribute.values`

The tags or attributes are saved as two parallel arrays, containing the keys
and values. Use `ARRAY JOIN` to work with them.

## Integration with monitoring systems

At the moment, there is no ready tool that can export the tracing data from
ClickHouse to a monitoring system.

For testing, it is possible to setup the export using a materialized view with the URL engine over the `system.opentelemetry_span_log` table, which would push the arriving log data to an HTTP endpoint of a trace collector. For example, to push the minimal span data to a Zipkin instance running at `http://localhost:9411`, in Zipkin v2 JSON format:

```sql
CREATE MATERIALIZED VIEW default.zipkin_spans
ENGINE = URL('http://127.0.0.1:9411/api/v2/spans', 'JSONEachRow')
SETTINGS output_format_json_named_tuples_as_objects = 1,
    output_format_json_array_of_rows = 1 AS
SELECT
    lower(hex(reinterpretAsFixedString(trace_id))) AS traceId,
    lower(hex(parent_span_id)) AS parentId,
    lower(hex(span_id)) AS id,
    operation_name AS name,
    start_time_us AS timestamp,
    finish_time_us - start_time_us AS duration,
    cast(tuple('clickhouse'), 'Tuple(serviceName text)') AS localEndpoint,
    cast(tuple(
        attribute.values[indexOf(attribute.names, 'db.statement')]),
        'Tuple("db.statement" text)') AS tags
FROM system.opentelemetry_span_log
```

In case of any errors, the part of the log data for which the error has occurred will be silently lost. Check the server log for error messages if the data does not arrive.
