---
alias: []
description: 'Documentation for the SQLite format'
input_format: true
keywords: ['SQLite']
output_format: true
slug: /interfaces/formats/SQLite
title: 'SQLite'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `SQLite` format reads and writes a SQLite database file.

On output, ClickHouse writes the query result into a single table in the SQLite database. On input, ClickHouse reads a single table from the SQLite database.

For local regular files, ClickHouse opens the SQLite database file directly. For other input and output streams, ClickHouse materializes the SQLite database in memory.

On input, ClickHouse reads the first table from the SQLite database by default. You can change it with the `input_format_sqlite_table_name` setting. On output, the default table name is `table`; you can change it with the `output_format_sqlite_table_name` setting.

## Example usage {#example-usage}

```bash
clickhouse-client --query="SELECT number, toString(number) AS s FROM numbers(10) FORMAT SQLite" > data.sqlite
clickhouse-local --input-format SQLite --structure "number UInt64, s String" --query "SELECT * FROM table" < data.sqlite
```

## Data types matching {#data-types-matching}

When ClickHouse writes data in the `SQLite` format, it creates a SQLite table with the following declared types:

| ClickHouse data type | SQLite declared type |
|----------------------|----------------------|
| `UInt64`, `Int128`, `UInt128`, `Int256`, `UInt256` | `TEXT` |
| `Bool`, `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32` | `INTEGER` |
| Floating-point types | `REAL` |
| Other types | `TEXT` |

For `Float32` and `Float64` columns, non-`NaN` values are written using SQLite native storage classes, while `NaN` values are written using ClickHouse text serialization. `Bool` and ordinary integer values are also written using SQLite native storage classes. Wide integers and complex types are written using ClickHouse text serialization. `NULL` values are written as SQLite `NULL`.

When ClickHouse infers a schema from SQLite input, it uses the same mapping as the [SQLite database engine](../../engines/database-engines/sqlite.md#data_types-support):

| SQLite declared type | ClickHouse inferred type |
|----------------------|--------------------------|
| Type name contains `INT` | `Int64` |
| `REAL`, `FLOAT`, `DOUBLE` | `Float64` |
| Other types, including `TEXT` and `BLOB` | `String` |

If a column is nullable in SQLite, ClickHouse wraps the inferred type in `Nullable`.

Schema inference does not preserve the original ClickHouse data types. For example, `Bool` is written as SQLite `INTEGER` and inferred as `Int64`, while `Date`, `DateTime`, `Decimal`, `UUID`, `IPv4`, `IPv6`, `Enum`, `Array`, `Tuple`, and `Map` are written as SQLite `TEXT` and inferred as `String`.

When the ClickHouse table structure is specified explicitly, floating-point values are read as SQLite `REAL` values, with a text fallback for `NaN`. Other SQLite values are read as text and parsed into the requested ClickHouse types.

## Format settings {#format-settings}

| Setting                                                                                                                     | Description                                      | Default   |
|-----------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------|-----------|
| [`input_format_sqlite_table_name`](../../operations/settings/settings-formats.md/#input_format_sqlite_table_name)           | The name of the table to read from SQLite input. If empty, the first table is used. | `''` |
| [`output_format_sqlite_table_name`](../../operations/settings/settings-formats.md/#output_format_sqlite_table_name)         | The name of the table in SQLite output.          | `'table'` |
