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
