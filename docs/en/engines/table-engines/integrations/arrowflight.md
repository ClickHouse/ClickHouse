---
description: 'The engine allows querying and inserting into remote datasets via Apache Arrow Flight.'
sidebar_label: 'ArrowFlight'
sidebar_position: 186
slug: /engines/table-engines/integrations/arrowflight
title: 'ArrowFlight table engine'
doc_type: 'reference'
---

The ArrowFlight table engine enables ClickHouse to read from and write to remote datasets via the [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) protocol.
This integration allows ClickHouse to interact with external Flight-enabled servers in a columnar Arrow format with high performance.

## Creating a Table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name (name1 [type1], name2 [type2], ...)
    ENGINE = ArrowFlight('host:port', 'dataset_name' [, 'username', 'password']);
```

**Engine Parameters**

- `host:port` — Address of the remote Arrow Flight server. If the port is omitted, the default port `8815` is used. [String](../../../sql-reference/data-types/string.md).
- `dataset_name` — Identifier of the dataset on the Flight server (used as a PATH descriptor or in a `SELECT *` query depending on the `arrow_flight_request_descriptor_type` setting). [String](../../../sql-reference/data-types/string.md).
- `username` — Username for basic HTTP authentication. [String](../../../sql-reference/data-types/string.md).
- `password` — Password for basic HTTP authentication. [String](../../../sql-reference/data-types/string.md).

If `username` and `password` are omitted, authentication is not used (this works only if the Arrow Flight server allows unauthenticated access).

The column list is optional — if omitted, the schema is inferred from the remote Arrow Flight server via `GetSchema`.

## Named Collections {#named-collections}

The engine supports [named collections](/operations/named-collections) for storing connection parameters:

```sql
CREATE TABLE remote_flight_data
    ENGINE = ArrowFlight(named_collection_name);
```

Named collection parameters:

| Parameter | Required | Default | Description |
|---|---|---|---|
| `host` or `hostname` | No | `""` | Server hostname. |
| `port` | Yes | — | Server port. |
| `dataset` | No | `""` | Dataset name or descriptor. |
| `use_basic_authentication` | No | `true` | Enable basic authentication. |
| `user` or `username` | If auth enabled | — | Username for authentication. |
| `password` | No | `""` | Password for authentication. |
| `enable_ssl` | No | `false` | Enable TLS encryption. |
| `ssl_ca` | No | `""` | Path to the CA certificate file for TLS verification. |
| `ssl_override_hostname` | No | `""` | Override the hostname checked during TLS verification. |

## Settings {#settings}

- `arrow_flight_request_descriptor_type` — Controls how the dataset name is sent to the Flight server. Possible values: `path` (default, sends as a PATH descriptor) or `command` (sends as a CMD descriptor with `SELECT * FROM <dataset>`). Use `command` for Flight servers that expect SQL commands (e.g., Dremio).

## Usage Example {#usage-example}

Reading data from a remote Arrow Flight server:

```sql
CREATE TABLE remote_flight_data
(
    id UInt32,
    name String,
    value Float64
) ENGINE = ArrowFlight('127.0.0.1:9005', 'sample_dataset');

SELECT * FROM remote_flight_data ORDER BY id;
```

```text
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

Inserting data into a remote Arrow Flight server:

```sql
INSERT INTO remote_flight_data VALUES (4, 'qux', 99.9);
```

## Notes {#notes}

- If columns are specified in the `CREATE TABLE` statement, they must match the schema returned by the Flight server.
- If columns are omitted, the schema is inferred automatically from the remote server.
- Both reading (`SELECT`) and writing (`INSERT`) are supported.
- The `arrow_flight_request_descriptor_type` setting controls whether the dataset name is sent as a PATH descriptor or as a CMD descriptor wrapping a `SELECT *` query.

## See Also {#see-also}

- [arrowFlight table function](/sql-reference/table-functions/arrowflight)
- [Arrow Flight Interface](/interfaces/arrowflight)
- [Apache Arrow Flight SQL specification](https://arrow.apache.org/docs/format/FlightSql.html)
- [Arrow format in ClickHouse](/interfaces/formats/Arrow)
