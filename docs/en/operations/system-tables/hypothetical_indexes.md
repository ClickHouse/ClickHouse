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

The current `(database, table)` are resolved by UUID at query time, so they reflect `RENAME TABLE` and entries for dropped tables are hidden automatically.

## Columns {#columns}

| Column        | Type     | Description                                                                                  |
|---------------|----------|----------------------------------------------------------------------------------------------|
| `database`    | `String` | Target database.                                                                             |
| `table`       | `String` | Target table.                                                                                |
| `name`        | `String` | Index name.                                                                                  |
| `type`        | `String` | Index type (`minmax`, `set`, `bloom_filter`, etc.).                                          |
| `type_full`   | `String` | Index type expression including arguments, e.g. `bloom_filter(0.01)`.                        |
| `expression`  | `String` | Index expression as written in `CREATE HYPOTHETICAL INDEX`.                                  |
| `granularity` | `UInt64` | Number of data granules per index granule.                                                   |

## Example {#example}

```sql
CREATE HYPOTHETICAL INDEX i1 ON t (b) TYPE bloom_filter(0.01)  GRANULARITY 1;
CREATE HYPOTHETICAL INDEX i2 ON t (b) TYPE bloom_filter(0.001) GRANULARITY 1;

SELECT database, table, name, type, type_full, expression, granularity
FROM system.hypothetical_indexes;
```

```text
┌─database─┬─table─┬─name─┬─type─────────┬─type_full───────────┬─expression─┬─granularity─┐
│ default  │ t     │ i1   │ bloom_filter │ bloom_filter(0.01)  │ b          │           1 │
│ default  │ t     │ i2   │ bloom_filter │ bloom_filter(0.001) │ b          │           1 │
└──────────┴───────┴──────┴──────────────┴─────────────────────┴────────────┴─────────────┘
```

`type` is the base type name and `type_full` includes the arguments, so users can distinguish between parametrized variants like `bloom_filter(0.01)` and `bloom_filter(0.001)`.

## See also {#see-also}

- [`CREATE HYPOTHETICAL INDEX`](/sql-reference/statements/hypothetical-index#create-hypothetical-index)
- [`EXPLAIN WHATIF`](/sql-reference/statements/explain#explain-whatif)
