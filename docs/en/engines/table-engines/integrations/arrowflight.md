---
description: 'The engine allows querying remote datasets via Apache Arrow Flight.'
sidebar_label: 'ArrowFlight'
sidebar_position: 186
slug: /engines/table-engines/integrations/arrowflight
title: 'ArrowFlight table engine'
doc_type: 'reference'
---

# ArrowFlight table engine

The ArrowFlight table engine enables ClickHouse to query remote datasets via the [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) protocol.
This integration allows ClickHouse to fetch data from external Flight-enabled servers in a columnar Arrow format with high performance.

## Creating a Table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name (name1 [type1], name2 [type2], ...)
    ENGINE = ArrowFlight('host:port', 'dataset_name' [, 'username', 'password']);
```

**Engine Parameters**

* `host:port` — Address of the remote Arrow Flight server.
* `dataset_name` — Identifier of the dataset on the Flight server.
* `username` - Username to use with basic HTTP style authentication.
* `password` - Password to use with basic HTTP style authentication.
If `username` and `password` are not specified, it means that authentication is not used
(that will work only if the Arrow Flight server allows it).

## Usage Example {#usage-example}

This example shows how to create a table that reads data from a remote Arrow Flight server:

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
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

## Notes {#notes}

* The schema defined in ClickHouse must match the schema returned by the Flight server.
* This engine is suitable for federated queries, data virtualization, and decoupling storage from compute.

## See Also {#see-also}

* [Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
* [Arrow format integration in ClickHouse](/interfaces/formats/Arrow)
