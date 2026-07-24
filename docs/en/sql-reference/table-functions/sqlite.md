---
description: 'Allows to perform queries on data stored in a SQLite database.'
sidebar_label: 'sqlite'
sidebar_position: 185
slug: /sql-reference/table-functions/sqlite
title: 'sqlite'
doc_type: 'reference'
---

Allows to perform queries on data stored in a [SQLite](../../engines/database-engines/sqlite.md) database.

## Syntax {#syntax}

```sql
sqlite('db_path', 'table_name')
```

## Arguments {#arguments}

- `db_path` — Path to a file with an SQLite database. [String](../../sql-reference/data-types/string.md).
- `table_name` — Name of a table in the SQLite database, or a query passed to SQLite as is (see [Passing a query instead of a table name](#passing-a-query)). [String](../../sql-reference/data-types/string.md).

## Returned value {#returned_value}

- A table object with the same columns as in the original `SQLite` table.

## Passing a query instead of a table name {#passing-a-query}

Instead of a table name, the second argument can be a `SELECT` query that is passed to SQLite as is. The structure of the resulting table is inferred from the query result. The query can be written either as a subquery, or wrapped into the `query` function:

```sql
SELECT * FROM sqlite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
SELECT * FROM sqlite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

Such a table is read-only: `INSERT` into it is not allowed. The same syntax is supported by the [`SQLite`](/engines/table-engines/integrations/sqlite) table engine.

:::note
The subquery form `(SELECT ...)` is parsed by ClickHouse and re-serialized before being sent to SQLite. It must therefore be valid ClickHouse SQL. To pass SQLite-specific syntax that ClickHouse does not parse, use the `query('...')` form, whose text is sent to SQLite verbatim.

Any outer `WHERE`, `LIMIT`, aggregation, etc. of the surrounding ClickHouse query is **not** pushed down into the passed query — it is applied in ClickHouse after the full query result is fetched. To restrict the data read from SQLite, put the filter inside the passed query. With [`external_table_strict_query = 1`](/operations/settings/settings#external_table_strict_query) an outer filter that cannot be pushed down is rejected with an exception instead of being applied locally.
:::

## Example {#example}

```sql title="Query"
SELECT * FROM sqlite('sqlite.db', 'table1') ORDER BY col2;
```

```text title="Response"
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

## Related {#related}

- [SQLite](../../engines/table-engines/integrations/sqlite.md) table engine
- [SQLite database engine](../../engines/database-engines/sqlite.md) — Data types support section
