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
a dependency on a particular monitoring system, instead only
providing the tracing data conforming to the standard. A natural way to do so
in an SQL RDBMS is a system table. OpenTelemetry trace span information
[required by the standard](https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/overview.md#span)
is stored in the system table called `system.opentelemetry_span_log`.

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
