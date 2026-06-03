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

| Input | Output  | Alias |
|-------|---------|-------|
| ✔     | ✔       |       |

## Description {#description}

The `SQLite` format allows reading from and writing to [SQLite](https://www.sqlite.org/) `.db` database files.

On input, ClickHouse reads all the data from a single table of the database file.
If the database contains more than one table, by default it reads data from the first one.
The table to read from can be chosen with the [`input_format_sqlite_table_name`](#format-settings) setting.

On output, ClickHouse creates a table named `result` in the database file and writes all the rows into it.
Writing is only supported when the output is a file, for example when using `INTO OUTFILE` or the [`file`](/sql-reference/table-functions/file) table function.

:::note
This format does not support schema inference, so the structure must be provided explicitly when reading.
To read or write tables stored in SQLite with full schema support, consider the [`SQLite`](/engines/database-engines/sqlite) database engine or the [`sqlite`](/sql-reference/table-functions/sqlite) table function instead.
:::

## Example usage {#example-usage}

### Writing a SQLite database {#writing-a-sqlite-database}

```sql title="Query"
INSERT INTO FUNCTION file('numbers.db', SQLite)
SELECT number AS id, toString(number) AS name
FROM numbers(3);
```

### Reading a SQLite database {#reading-a-sqlite-database}

```sql title="Query"
SELECT *
FROM file('numbers.db', SQLite, 'id UInt64, name String')
ORDER BY id;
```

```response title="Response"
┌─id─┬─name─┐
│  0 │ 0    │
│  1 │ 1    │
│  2 │ 2    │
└────┴──────┘
```

To read a specific table by name, use the `input_format_sqlite_table_name` setting:

```sql title="Query"
SELECT *
FROM file('database.db', SQLite, 'x UInt32')
SETTINGS input_format_sqlite_table_name = 'my_table';
```

## Format settings {#format-settings}

You can specify the name of the table from which to read data using the [`input_format_sqlite_table_name`](/operations/settings/settings-formats.md/#input_format_sqlite_table_name) setting.
When it is empty (the default), data is read from the first table in the database file.
