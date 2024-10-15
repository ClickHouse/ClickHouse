---
slug: /en/operations/opentelemetry
sidebar_position: 62
sidebar_label: Tracing ClickHouse with OpenTelemetry
title: "Tracing ClickHouse with OpenTelemetry"
---

[OpenTelemetry](https://opentelemetry.io/) is an open standard for collecting traces and metrics from the distributed application. ClickHouse has some support for OpenTelemetry.

## Supplying Trace Context to ClickHouse

ClickHouse accepts trace context HTTP headers, as described by the [W3C recommendation](https://www.w3.org/TR/trace-context/). It also accepts trace context over a native protocol that is used for communication between ClickHouse servers or between the client and server. For manual testing, trace context headers conforming to the Trace Context recommendation can be supplied to `clickhouse-client` using `--opentelemetry-traceparent` and `--opentelemetry-tracestate` flags.

If no parent trace context is supplied or the provided trace context does not comply with W3C standard above, ClickHouse can start a new trace, with probability controlled by the [opentelemetry_start_trace_probability](../operations/settings/settings.md#opentelemetry-start-trace-probability) setting.

## Propagating the Trace Context

The trace context is propagated to downstream services in the following cases:

* Queries to remote ClickHouse servers, such as when using [Distributed](../engines/table-engines/special/distributed.md) table engine.

* [url](../sql-reference/table-functions/url.md) table function. Trace context information is sent in HTTP headers.

## Tracing the ClickHouse Itself

ClickHouse creates `trace spans` for each query and some of the query execution stages, such as query planning or distributed queries.

To be useful, the tracing information has to be exported to a monitoring system that supports OpenTelemetry, such as [Jaeger](https://jaegertracing.io/) or [Prometheus](https://prometheus.io/). ClickHouse avoids a dependency on a particular monitoring system, instead only providing the tracing data through a system table. OpenTelemetry trace span information [required by the standard](https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/overview.md#span) is stored in the [system.opentelemetry_span_log](../operations/system-tables/opentelemetry_span_log.md) table.

The table must be enabled in the server configuration, see the `opentelemetry_span_log` element in the default config file `config.xml`. It is enabled by default.

The tags or attributes are saved as two parallel arrays, containing the keys and values. Use [ARRAY JOIN](../sql-reference/statements/select/array-join.md) to work with them.

## Log-query-settings

ClickHouse allows you to log changes to query settings during query execution. When enabled, any modifications made to query settings will be recorded in the OpenTelemetry span log. This feature is particularly useful in production environments for tracking configuration changes that may affect query performance.

## Integration with monitoring systems

At the moment, there is no ready tool that can export the tracing data from ClickHouse to a monitoring system.

For testing, it is possible to setup the export using a materialized view with the [URL](../engines/table-engines/special/url.md) engine over the [system.opentelemetry_span_log](../operations/system-tables/opentelemetry_span_log.md) table, which would push the arriving log data to an HTTP endpoint of a trace collector. For example, to push the minimal span data to a Zipkin instance running at `http://localhost:9411`, in Zipkin v2 JSON format:

```sql
CREATE MATERIALIZED VIEW default.zipkin_spans
ENGINE = URL('http://127.0.0.1:9411/api/v2/spans', 'JSONEachRow')
SETTINGS output_format_json_named_tuples_as_objects = 1,
    output_format_json_array_of_rows = 1 AS
SELECT
    lower(hex(trace_id)) AS traceId,
    case when parent_span_id = 0 then '' else lower(hex(parent_span_id)) end AS parentId,
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

## Related Content

- Blog: [Building an Observability Solution with ClickHouse - Part 2 - Traces](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)
