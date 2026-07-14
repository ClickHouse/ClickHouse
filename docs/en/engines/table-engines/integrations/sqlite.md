---
description: 'The engine allows to import and export data to SQLite and supports queries
  to SQLite tables directly from ClickHouse.'
sidebar_label: 'SQLite'
sidebar_position: 185
slug: /engines/table-engines/integrations/sqlite
title: 'SQLite table engine'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# SQLite table engine

<CloudNotSupportedBadge/>

The engine allows to import and export data to SQLite and supports queries to SQLite tables directly from ClickHouse.

## Creating a table {#creating-a-table}

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = SQLite('db_path', 'table')
```

**Engine Parameters**

- `db_path` — Path to SQLite file with a database.
- `table` — Name of a table in the SQLite database, or a query passed to SQLite as is (see [Passing a query instead of a table name](#passing-a-query)).

## Passing a query instead of a table name {#passing-a-query}

Instead of a table name, the `table` argument can be a `SELECT` query that is passed to SQLite as is. The structure of the table is inferred from the query result. The query can be written either as a subquery, or wrapped into the `query` function:

```sql
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

Such a table is read-only: `INSERT` into it is not allowed. The same syntax is supported by the [`sqlite`](/sql-reference/table-functions/sqlite) table function.

:::note
The subquery form `(SELECT ...)` is parsed by ClickHouse and re-serialized before being sent to SQLite. It must therefore be valid ClickHouse SQL. To pass SQLite-specific syntax that ClickHouse does not parse, use the `query('...')` form, whose text is sent to SQLite verbatim.

Any outer `WHERE`, `LIMIT`, aggregation, etc. of the surrounding ClickHouse query is **not** pushed down into the passed query — it is applied in ClickHouse after the full query result is fetched. To restrict the data read from SQLite, put the filter inside the passed query. With [`external_table_strict_query = 1`](/operations/settings/settings#external_table_strict_query) an outer filter that cannot be pushed down is rejected with an exception instead of being applied locally.
:::

## Data types support {#data-types-support}

When you explicitly specify ClickHouse column types in the table definition, the following ClickHouse types can be parsed from SQLite TEXT columns:

- [Date](../../../sql-reference/data-types/date.md), [Date32](../../../sql-reference/data-types/date32.md)
- [DateTime](../../../sql-reference/data-types/datetime.md), [DateTime64](../../../sql-reference/data-types/datetime64.md)
- [UUID](../../../sql-reference/data-types/uuid.md)
- [Enum8, Enum16](../../../sql-reference/data-types/enum.md)
- [Decimal32, Decimal64, Decimal128, Decimal256](../../../sql-reference/data-types/decimal.md)
- [FixedString](../../../sql-reference/data-types/fixedstring.md)
- All integer types ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../../sql-reference/data-types/int-uint.md))
- [Float32, Float64](../../../sql-reference/data-types/float.md)

See [SQLite database engine](../../../engines/database-engines/sqlite.md#data_types-support) for the default type mapping.

## Usage example {#usage-example}

Shows a query creating the SQLite table:

```sql
SHOW CREATE TABLE sqlite_db.table2;
```

```text
CREATE TABLE SQLite.table2
(
    `col1` Nullable(Int32),
    `col2` Nullable(String)
)
ENGINE = SQLite('sqlite.db','table2');
```

Returns the data from the table:

```sql
SELECT * FROM sqlite_db.table2 ORDER BY col1;
```

```text
┌─col1─┬─col2──┐
│    1 │ text1 │
│    2 │ text2 │
│    3 │ text3 │
└──────┴───────┘
```

**See Also**

- [SQLite](../../../engines/database-engines/sqlite.md) engine
- [sqlite](../../../sql-reference/table-functions/sqlite.md) table function
