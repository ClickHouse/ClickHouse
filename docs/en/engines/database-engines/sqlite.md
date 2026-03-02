---
description: 'Allows to connect to SQLite databases and perform `INSERT` and `SELECT`
  queries to exchange data between ClickHouse and SQLite.'
sidebar_label: 'SQLite'
sidebar_position: 55
slug: /engines/database-engines/sqlite
title: 'SQLite'
doc_type: 'reference'
---

# SQLite

Allows to connect to [SQLite](https://www.sqlite.org/index.html) database and perform `INSERT` and `SELECT` queries to exchange data between ClickHouse and SQLite.

## Creating a database {#creating-a-database}

```sql
    CREATE DATABASE sqlite_database
    ENGINE = SQLite('db_path')
```

**Engine Parameters**

- `db_path` — Path to a file with SQLite database.

## Data types support {#data_types-support}

The table below shows the default type mapping when ClickHouse automatically infers schema from SQLite:

|  SQLite   | ClickHouse                                              |
|---------------|---------------------------------------------------------|
| INTEGER       | [Int32](../../sql-reference/data-types/int-uint.md)     |
| REAL          | [Float32](../../sql-reference/data-types/float.md)      |
| TEXT          | [String](../../sql-reference/data-types/string.md)      |
| TEXT          | [UUID](../../sql-reference/data-types/uuid.md)          |
| BLOB          | [String](../../sql-reference/data-types/string.md)      |

When you explicitly define a table with specific ClickHouse types using the [SQLite table engine](../../engines/table-engines/integrations/sqlite.md), the following ClickHouse types can be parsed from SQLite TEXT columns:

- [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md)
- [DateTime](../../sql-reference/data-types/datetime.md), [DateTime64](../../sql-reference/data-types/datetime64.md)
- [UUID](../../sql-reference/data-types/uuid.md)
- [Enum8, Enum16](../../sql-reference/data-types/enum.md)
- [Decimal32, Decimal64, Decimal128, Decimal256](../../sql-reference/data-types/decimal.md)
- [FixedString](../../sql-reference/data-types/fixedstring.md)
- All integer types ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../sql-reference/data-types/int-uint.md))
- [Float32, Float64](../../sql-reference/data-types/float.md)

SQLite has dynamic typing, and its type access functions perform automatic type coercion. For example, reading a TEXT column as an integer will return 0 if the text cannot be parsed as a number. This means that if a ClickHouse table is defined with a different type than the underlying SQLite column, values may be silently coerced rather than causing an error.

## Specifics and recommendations {#specifics-and-recommendations}

SQLite stores the entire database (definitions, tables, indices, and the data itself) as a single cross-platform file on a host machine. During writing SQLite locks the entire database file, therefore write operations are performed sequentially. Read operations can be multi-tasked.
SQLite does not require service management (such as startup scripts) or access control based on `GRANT` and passwords. Access control is handled by means of file-system permissions given to the database file itself.

## Usage example {#usage-example}

Database in ClickHouse, connected to the SQLite:

```sql
CREATE DATABASE sqlite_db ENGINE = SQLite('sqlite.db');
SHOW TABLES FROM sqlite_db;
```

```text
┌──name───┐
│ table1  │
│ table2  │
└─────────┘
```

Shows the tables:

```sql
SELECT * FROM sqlite_db.table1;
```

```text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```
Inserting data into SQLite table from ClickHouse table:

```sql
CREATE TABLE clickhouse_table(`col1` String,`col2` Int16) ENGINE = MergeTree() ORDER BY col2;
INSERT INTO clickhouse_table VALUES ('text',10);
INSERT INTO sqlite_db.table1 SELECT * FROM clickhouse_table;
SELECT * FROM sqlite_db.table1;
```

```text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
│ text  │   10 │
└───────┴──────┘
```
