---
description: 'Documentation for the Apache Arrow Flight interface in ClickHouse, allowing Flight SQL clients to connect to ClickHouse'
sidebar_label: 'Arrow Flight Interface'
sidebar_position: 26
slug: /interfaces/arrowflight
title: 'Arrow Flight Interface'
doc_type: 'reference'
---

# Apache Arrow Flight Interface

ClickHouse supports integration with the [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) protocol — a high-performance RPC framework designed for efficient columnar data transport using the Arrow IPC format over gRPC.

This interface allows Flight SQL clients to query ClickHouse and retrieve results in the Arrow format, providing high throughput and low latency for analytical workloads.

## Features {#features}

* Execute SQL queries via the Arrow Flight SQL protocol
* Stream query results in Apache Arrow format
* Integration with BI tools and custom data applications that support Arrow Flight
* Lightweight and performant communication over gRPC

## Limitations {#limitations}

The Arrow Flight interface is currently experimental and under active development. Known limitations include:

* Limited support for complex ClickHouse-specific SQL features
* Not all Arrow Flight SQL metadata operations are implemented yet
* No built-in authentication or TLS configuration in the reference implementation

If you encounter compatibility issues or would like to contribute, please [create an issue](https://github.com/ClickHouse/ClickHouse/issues) in the ClickHouse repository.

## Running the Arrow Flight Server {#running-server}

To enable the Arrow Flight server in a self-managed ClickHouse instance, add the following configuration to your server config:

```xml
<clickhouse>
    <arrowflight_port>9005</arrowflight_port>
</clickhouse>
```

Restart the ClickHouse server. Upon successful startup, you should see a log message similar to:

```bash
{} <Information> Application: Arrow Flight compatibility protocol: 0.0.0.0:9005
```

## Connecting to ClickHouse via Arrow Flight SQL {#connecting-to-clickhouse}

You can use any client that supports Arrow Flight SQL. For example, using `pyarrow`:

```python
import pyarrow.flight

client = pyarrow.flight.FlightClient("grpc://localhost:9005")
ticket = pyarrow.flight.Ticket(b"SELECT number FROM system.numbers LIMIT 10")
reader = client.do_get(ticket)

for batch in reader:
    print(batch.to_pandas())
```

## Compatibility {#compatibility}

The Arrow Flight interface is compatible with tools that support Arrow Flight SQL including custom applications built with:

* Python (`pyarrow`)
* Java (`arrow-flight`)
* C++ and other gRPC-compatible languages

If a native ClickHouse connector is available for your tool (e.g. JDBC, ODBC), prefer using it unless Arrow Flight is specifically required for performance or format compatibility.

## Query Cancellation {#query-cancellation}

Long-running queries can be cancelled by closing the gRPC connection from the client. Support for more advanced cancellation features is planned.

---

For more details, see:

* [Apache Arrow Flight SQL specification](https://arrow.apache.org/docs/format/FlightSql.html)
* [ClickHouse GitHub Issue #7554](https://github.com/ClickHouse/ClickHouse/issues/7554)
