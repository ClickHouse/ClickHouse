---
description: 'Allows reading from and writing to data exposed via an Apache Arrow Flight server.'
sidebar_label: 'arrowFlight'
sidebar_position: 186
slug: /sql-reference/table-functions/arrowflight
title: 'arrowFlight'
doc_type: 'reference'
---

Allows reading from and writing to data exposed via an [Apache Arrow Flight](/interfaces/arrowflight) server.

**Syntax**

```sql
arrowFlight('host:port', 'dataset_name' [, 'username', 'password'])
```

**Arguments**

- `host:port` — Address of the Arrow Flight server. If the port is omitted, the default port `8815` is used. [String](../../sql-reference/data-types/string.md).
- `dataset_name` — Name of the dataset or descriptor available on the Arrow Flight server. [String](../../sql-reference/data-types/string.md).
- `username` — Username for basic HTTP authentication. [String](../../sql-reference/data-types/string.md).
- `password` — Password for basic HTTP authentication. [String](../../sql-reference/data-types/string.md).

If `username` and `password` are not specified, authentication is not used (this works only if the Arrow Flight server allows unauthenticated access).

The function also supports [named collections](/operations/named-collections) — see the [ArrowFlight table engine](/engines/table-engines/integrations/arrowflight#named-collections) for the list of supported parameters.

**Returned value**

A table object representing the remote dataset. The schema is inferred from the Arrow Flight server.

**Settings**

- `arrow_flight_request_descriptor_type` — Controls how the dataset name is sent to the Flight server. Values: `path` (default) or `command`. See the [ArrowFlight table engine](/engines/table-engines/integrations/arrowflight#settings) for details.

**Examples**

Reading from a remote Arrow Flight server:

```sql title="Query"
SELECT * FROM arrowFlight('127.0.0.1:9005', 'sample_dataset') ORDER BY id;
```

```text title="Response"
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

Inserting data into a remote Arrow Flight server:

```sql
INSERT INTO FUNCTION arrowFlight('127.0.0.1:9005', 'sample_dataset') VALUES (4, 'qux', 99.9);
```

Using a named collection:

```sql
SELECT * FROM arrowFlight(named_collection_name);
```

**See Also**

- [ArrowFlight table engine](/engines/table-engines/integrations/arrowflight)
- [Arrow Flight Interface](/interfaces/arrowflight)
- [Apache Arrow Flight SQL specification](https://arrow.apache.org/docs/format/FlightSql.html)
