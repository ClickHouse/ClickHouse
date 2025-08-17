---
description: 'Allows to perform queries on data exposed via an Apache Arrow Flight server.'
sidebar_label: 'Arrow Flight'
sidebar_position: 186
slug: /sql-reference/table-functions/arrowflight
title: 'Arrow Flight'
---

# Arrow Flight Table Function

Allows to perform queries on data exposed via an [Apache Arrow Flight](../../interfaces/arrowflight.md) server.

**Syntax**

```sql
arrowFlight('host:port', 'dataset_name')
```

**Arguments**

* `host:port` — Address of the Arrow Flight server. [String](../../sql-reference/data-types/string.md).
* `dataset_name` — Name of the dataset or descriptor available on the Arrow Flight server. [String](../../sql-reference/data-types/string.md).

**Returned value**

* A table object representing the remote dataset. The schema is inferred from the Arrow Flight response.

**Example**

Query:

```sql
SELECT * FROM arrowFlight('127.0.0.1:9005', 'sample_dataset') ORDER BY id;
```

Result:

```text
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

**See Also**

* [Arrow Flight](../../engines/table-engines/integrations/arrowflight.md) table engine
* [Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
