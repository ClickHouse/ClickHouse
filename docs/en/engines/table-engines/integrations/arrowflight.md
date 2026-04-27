---
description: 'Documentation for the Arrow Flight SQL interface and table engine in ClickHouse, a high-performance RPC protocol for columnar data transport.'
sidebar_label: 'ArrowFlight'
sidebar_position: 186
slug: /engines/table-engines/integrations/arrowflight
title: 'Arrow Flight SQL'
doc_type: 'reference'
---

# Arrow Flight SQL {#arrow-flight-sql}

[Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) is a high-performance RPC framework designed for efficient columnar data transport. It enables transferring large datasets in [Apache Arrow](https://arrow.apache.org/docs/format/Columnar.html) columnar format over gRPC with minimal serialization overhead.

[Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) extends the base Flight protocol with SQL semantics, allowing clients to execute queries, retrieve schemas, and manage database interactions using a standardized set of RPC commands.

ClickHouse exposes an Arrow Flight SQL server that clients can use to:

- Execute SQL queries and stream results in Arrow IPC format.
- Insert data into tables using `DoPut`.
- Manage sessions and cancel in-flight queries.
- Introspect schema metadata without executing queries.

## Enabling the Arrow Flight Server {#enabling-the-arrow-flight-server}

To enable the Arrow Flight server, add `arrowflight_port` to your ClickHouse server configuration:

```xml
<clickhouse>
    <arrowflight_port>9005</arrowflight_port>
</clickhouse>
```

## TLS Configuration {#tls-configuration}

To enable TLS for the Arrow Flight server:

```xml
<clickhouse>
    <arrowflight_port>9005</arrowflight_port>
    <arrowflight>
        <enable_ssl>true</enable_ssl>
        <ssl_cert_file>/path/to/server.crt</ssl_cert_file>
        <ssl_key_file>/path/to/server.key</ssl_key_file>
    </arrowflight>
</clickhouse>
```

When TLS is enabled, clients must connect using a TLS-enabled Flight client and provide the server certificate for verification.

## Authentication {#authentication}

### Basic Authentication {#basic-authentication}

Pass the username and password via the HTTP `Authorization` header using the `Basic` scheme. On successful authentication, the server returns a bearer token that can be reused for subsequent requests.

Python example without TLS:

```python
import pyarrow.flight as flight
import base64

location = flight.Location.for_grpc_tcp("localhost", 9005)
client = flight.FlightClient(location)

credentials = base64.b64encode(b"user:password").decode()
options = flight.FlightCallOptions(headers=[(b"authorization", f"Basic {credentials}".encode())])

info = client.get_flight_info(
    flight.FlightDescriptor.for_command(b"SELECT 1"),
    options
)
```

Python example with TLS:

```python
import pyarrow.flight as flight
import base64

with open("/path/to/server.crt", "rb") as f:
    tls_root_certs = f.read()

client = flight.FlightClient(
    "grpc+tls://localhost:9005",
    tls_root_certs=tls_root_certs
)

credentials = base64.b64encode(b"user:password").decode()
options = flight.FlightCallOptions(headers=[(b"authorization", f"Basic {credentials}".encode())])
```

### Bearer Token Authentication {#bearer-token-authentication}

After authenticating with Basic credentials, the server returns a bearer token in the response metadata. Use this token for subsequent requests to avoid resending credentials:

```python
# Authenticate once
token_pair = client.authenticate_basic_token("user", "password")
options = flight.FlightCallOptions(headers=[token_pair])

# Reuse the token
info = client.get_flight_info(
    flight.FlightDescriptor.for_command(b"SELECT 1"),
    options
)
```

## Session Management {#session-management}

ClickHouse sessions can be managed via gRPC metadata headers. All header names must be lowercase as required by RFC 9113 (HTTP/2).

| Header | Description |
|--------|-------------|
| `x-clickhouse-session-id` | Session identifier. If not provided, a new session is created per request. |
| `x-clickhouse-session-timeout` | Session timeout in seconds. |
| `x-clickhouse-session-check` | If set to `1`, validates that the session exists before processing the request. |
| `x-clickhouse-session-close` | If set to `1`, closes the session after the request completes. |

Example:

```python
options = flight.FlightCallOptions(headers=[
    token_pair,
    (b"x-clickhouse-session-id", b"my-session-123"),
    (b"x-clickhouse-session-timeout", b"300"),
])
```

## Server Configuration Reference {#server-configuration-reference}

| Setting | Description | Default |
|---------|-------------|---------|
| `arrowflight_port` | Port for the Arrow Flight server. | — |
| `arrowflight.enable_ssl` | Enable TLS for the Arrow Flight server. | `false` |
| `arrowflight.ssl_cert_file` | Path to the TLS certificate file. | — |
| `arrowflight.ssl_key_file` | Path to the TLS private key file. | — |
| `arrowflight.tickets_lifetime_seconds` | Lifetime of query tickets issued by `GetFlightInfo`. After expiration the ticket cannot be used with `DoGet`. | `300` |
| `arrowflight.cancel_ticket_after_do_get` | Cancel the query associated with a ticket after `DoGet` completes. | `true` |
| `arrowflight.poll_descriptors_lifetime_seconds` | Lifetime of flight descriptors created by `PollFlightInfo`. | `300` |
| `arrowflight.cancel_flight_descriptor_after_poll_flight_info` | Cancel the query associated with a descriptor after `PollFlightInfo` returns a final result. | `true` |
| `enable_arrow_close_session` | Allow clients to close ClickHouse sessions via `DoAction CloseSession`. | `true` |
| `default_session_timeout` | Default session timeout in seconds when none is specified by the client. | `60` |
| `max_session_timeout` | Maximum allowed session timeout in seconds. | `3600` |

## RPC Methods {#rpc-methods}

### GetFlightInfo {#getflightinfo}

Executes a query and returns a `FlightInfo` containing the result schema and a list of endpoints with tickets. Each ticket can be used with `DoGet` to retrieve the actual data.

```python
descriptor = flight.FlightDescriptor.for_command(b"SELECT number FROM system.numbers LIMIT 5")
info = client.get_flight_info(descriptor, options)
schema = info.schema
endpoints = info.endpoints
```

### PollFlightInfo {#pollflightinfo}

Submits a long-running query asynchronously and polls for completion. Returns a `PollInfo` with the current status and, once complete, endpoints for retrieving results. Useful for queries that take longer than a single RPC timeout.

```python
descriptor = flight.FlightDescriptor.for_command(b"SELECT sleep(2), number FROM system.numbers LIMIT 3")
poll_result = client.poll_flight_info(descriptor, options)
# poll_result.info contains the FlightInfo when ready
# poll_result.flight_descriptor is set while the query is still running
```

### GetSchema {#getschema}

Returns the Arrow schema for a query without executing it. Useful for schema introspection.

```python
descriptor = flight.FlightDescriptor.for_command(b"SELECT number, toString(number) AS s FROM system.numbers LIMIT 1")
schema_result = client.get_schema(descriptor, options)
print(schema_result.schema)
```

### DoGet {#doget}

Retrieves the result data for a ticket previously issued by `GetFlightInfo`. Data is streamed as Arrow IPC record batches.

```python
for endpoint in info.endpoints:
    reader = client.do_get(endpoint.ticket, options)
    table = reader.read_all()
    print(table)
```

### DoPut {#doput}

Sends data to ClickHouse. Supports two descriptor styles:

**Insert by table name** — use `for_path` with the table name:

```python
import pyarrow as pa

schema = pa.schema([("id", pa.int32()), ("name", pa.string())])
table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})

writer, _ = client.do_put(
    flight.FlightDescriptor.for_path("my_database", "my_table"),
    schema,
    options
)
writer.write_table(table)
writer.close()
```

**Insert by SQL** — use `for_command` with an `INSERT` statement:

```python
writer, _ = client.do_put(
    flight.FlightDescriptor.for_command(b"INSERT INTO my_table VALUES"),
    schema,
    options
)
writer.write_table(table)
writer.close()
```

### DoAction {#doaction}

Executes named server-side actions.

#### CancelFlightInfo {#cancelflightinfo}

Cancels a running query using the serialized `FlightInfo`:

```python
action = flight.Action(
    "CancelFlightInfo",
    info.serialize()
)
results = list(client.do_action(action, options))
```

#### SetSessionOptions {#setsessionoptions}

Sets per-session server settings:

```python
import json

action = flight.Action(
    "SetSessionOptions",
    json.dumps({"max_threads": "4", "enable_http_compression": "1"}).encode()
)
list(client.do_action(action, options))
```

#### GetSessionOptions {#getsessionoptions}

Retrieves current session settings:

```python
action = flight.Action("GetSessionOptions", b"")
results = list(client.do_action(action, options))
settings = json.loads(results[0].body.to_pybytes())
```

## Flight SQL Commands {#flight-sql-commands}

ClickHouse supports a subset of the Flight SQL command set.

**Supported via `GetFlightInfo` / `GetSchema`:**

| Command | Description |
|---------|-------------|
| `CommandStatementQuery` | Execute a SQL query and return results. |
| `CommandGetSqlInfo` | Retrieve server SQL capabilities. |
| `CommandGetCatalogs` | List available catalogs (databases). |
| `CommandGetDbSchemas` | List schemas within a catalog. |
| `CommandGetTables` | List tables in a schema. |
| `CommandGetTableTypes` | List supported table types. |
| `CommandGetPrimaryKeys` | Retrieve primary key information for a table. |

**Supported via `DoPut`:**

| Command | Description |
|---------|-------------|
| `CommandStatementUpdate` | Execute a DML statement (e.g., `INSERT`, `CREATE TABLE`). |
| `CommandStatementIngest` | Bulk ingest Arrow data into a table. |

**Not yet implemented:**

| Command |
|---------|
| `CommandPreparedStatementQuery` |
| `CommandPreparedStatementUpdate` |
| `CommandGetExportedKeys` |
| `CommandGetImportedKeys` |
| `CommandGetCrossReference` |
| `CommandGetXdbcTypeInfo` |

## Data Format {#data-format}

All data is transferred in [Apache Arrow IPC](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format) format. ClickHouse types are mapped to Arrow types automatically.

For ClickHouse types that have no direct Arrow equivalent (such as `LowCardinality`, `UUID`, or `Decimal256`), the server can serialize them as raw binary when the following settings are enabled:

| Setting | Description | Default |
|---------|-------------|---------|
| `output_format_arrow_unsupported_types_as_binary` | Serialize unsupported Arrow types as `Binary`. | `false` |
| `output_format_parquet_unsupported_types_as_binary` | Serialize unsupported Parquet types as `Binary`. | `false` |

These settings can be applied per session using `SetSessionOptions`.

## ArrowFlight Table Engine {#arrowflight-table-engine}

The `ArrowFlight` table engine enables ClickHouse to query remote datasets via the Arrow Flight protocol. This integration allows ClickHouse to fetch data from external Flight-enabled servers in a columnar Arrow format with high performance.

### Creating a Table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name (name1 [type1], name2 [type2], ...)
    ENGINE = ArrowFlight('host:port', 'dataset_name' [, 'username', 'password']);
```

**Engine Parameters**

* `host:port` — Address of the remote Arrow Flight server. [String](../../../sql-reference/data-types/string.md).
* `dataset_name` — Identifier of the dataset on the Flight server. [String](../../../sql-reference/data-types/string.md).
* `username` — Username for basic authentication.
* `password` — Password for basic authentication. If `username` and `password` are not specified, authentication is not used (only works if the server allows it).

### Usage Example {#usage-example}

Create a table that reads data from a remote Arrow Flight server:

```sql
CREATE TABLE remote_flight_data
(
    id UInt32,
    name String,
    value Float64
) ENGINE = ArrowFlight('127.0.0.1:9005', 'sample_dataset');
```

Query the remote data as if it were a local table:

```sql
SELECT * FROM remote_flight_data ORDER BY id;
```

```text
┌─id─┬─name────┬─value─┐
│  1 │ foo     │  42.1 │
│  2 │ bar     │  13.3 │
│  3 │ baz     │  77.0 │
└────┴─────────┴───────┘
```

## arrowFlight Table Function {#arrowflight-table-function}

The `arrowFlight` table function allows performing ad-hoc queries on data exposed via an Arrow Flight server without creating a table.

**Syntax**

```sql
arrowFlight('host:port', 'dataset_name' [, 'username', 'password'])
```

**Example**

```sql
SELECT * FROM arrowFlight('127.0.0.1:9005', 'sample_dataset') ORDER BY id;
```

## Complete Example {#complete-example}

Full Python example demonstrating connection, authentication, insert, and query:

```python
import pyarrow as pa
import pyarrow.flight as flight
import json

# Connect
client = flight.FlightClient(flight.Location.for_grpc_tcp("localhost", 9005))

# Authenticate and get a bearer token
token_pair = client.authenticate_basic_token("default", "")
options = flight.FlightCallOptions(headers=[token_pair])

# Create a table
writer, _ = client.do_put(
    flight.FlightDescriptor.for_command(
        b"CREATE TABLE IF NOT EXISTS test_flight (id UInt32, name String) ENGINE = Memory"
    ),
    pa.schema([]),
    options
)
writer.close()

# Insert data
schema = pa.schema([("id", pa.uint32()), ("name", pa.string())])
data = pa.table({"id": [1, 2, 3], "name": ["alpha", "beta", "gamma"]})

writer, _ = client.do_put(
    flight.FlightDescriptor.for_path("default", "test_flight"),
    schema,
    options
)
writer.write_table(data)
writer.close()

# Query data
descriptor = flight.FlightDescriptor.for_command(b"SELECT * FROM test_flight ORDER BY id")
info = client.get_flight_info(descriptor, options)

for endpoint in info.endpoints:
    reader = client.do_get(endpoint.ticket, options)
    result = reader.read_all()
    print(result.to_pydict())
# {'id': [1, 2, 3], 'name': ['alpha', 'beta', 'gamma']}
```

## Compatible Clients {#compatible-clients}

The following clients and tools are compatible with ClickHouse's Arrow Flight SQL interface:

- [PyArrow Flight](https://arrow.apache.org/docs/python/flight.html) (Python)
- [Arrow Flight Java](https://arrow.apache.org/docs/java/flight.html) (Java)
- [Arrow Flight C++](https://arrow.apache.org/docs/cpp/flight.html) (C++)
- [Arrow Flight Go](https://pkg.go.dev/github.com/apache/arrow/go/arrow/flight) (Go)
- [ADBC (Arrow Database Connectivity)](https://arrow.apache.org/adbc/) drivers
- [DBeaver](https://dbeaver.io/) (via ADBC/JDBC Flight SQL driver)

When a native ClickHouse connector is available for your language (e.g., `clickhouse-connect` for Python), prefer using it over the Arrow Flight SQL interface for full feature support.

## See Also {#see-also}

- [arrowFlight table function](../../../sql-reference/table-functions/arrowflight.md)
- [Apache Arrow Flight specification](https://arrow.apache.org/docs/format/Flight.html)
- [Apache Arrow Flight SQL specification](https://arrow.apache.org/docs/format/FlightSql.html)
- [Arrow format in ClickHouse](../../../interfaces/formats/Arrow/Arrow.md)
