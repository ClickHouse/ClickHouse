---
description: 'System table listing hypothetical (what-if) indexes defined in the current session'
keywords: ['system table', 'hypothetical_indexes', 'what-if']
slug: /operations/system-tables/hypothetical_indexes
title: 'system.hypothetical_indexes'
doc_type: 'reference'
---

# system.hypothetical_indexes {#system-hypothetical-indexes}

Lists every hypothetical (what-if) skip index defined in the current session. See [`CREATE HYPOTHETICAL INDEX`](/sql-reference/statements/hypothetical-index#create-hypothetical-index) and [`EXPLAIN WHATIF`](/sql-reference/statements/explain#explain-whatif).

The contents are session-scoped: each connection sees only its own hypothetical indexes, and the table is empty when no indexes have been created in the current session.

## Columns {#columns}

| Column        | Type     | Description                                                  |
|---------------|----------|--------------------------------------------------------------|
| `name`        | `String` | Index name.                                                  |
| `database`    | `String` | Target database.                                             |
| `table`       | `String` | Target table.                                                |
| `type`        | `String` | Index type (`minmax`, `set`, `bloom_filter`, etc.).          |
| `expression`  | `String` | Index expression as written in `CREATE HYPOTHETICAL INDEX`.  |
| `granularity` | `UInt64` | Number of data granules per index granule.                   |

## Example {#example}

```sql
CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;

SELECT name, table, type, granularity
FROM system.hypothetical_indexes;
```

```text
┌─name──┬─table─┬─type───┬─granularity─┐
│ idx_b │ t     │ minmax │           1 │
└───────┴───────┴────────┴─────────────┘
```

## See also {#see-also}

- [`CREATE HYPOTHETICAL INDEX`](/sql-reference/statements/hypothetical-index#create-hypothetical-index)
- [`EXPLAIN WHATIF`](/sql-reference/statements/explain#explain-whatif)
