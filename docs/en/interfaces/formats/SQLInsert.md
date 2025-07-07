---
alias: []
description: 'Documentation for the SQLInsert format'
input_format: false
keywords: ['SQLInsert']
output_format: true
slug: /interfaces/formats/SQLInsert
title: 'SQLInsert'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

Outputs data as a sequence of `INSERT INTO table (columns...) VALUES (...), (...) ...;` statements.

## Example Usage {#example-usage}

Example:

```sql
SELECT number AS x, number + 1 AS y, 'Hello' AS z FROM numbers(10) FORMAT SQLInsert SETTINGS output_format_sql_insert_max_batch_size = 2
```

```sql
INSERT INTO table (x, y, z) VALUES (0, 1, 'Hello'), (1, 2, 'Hello');
INSERT INTO table (x, y, z) VALUES (2, 3, 'Hello'), (3, 4, 'Hello');
INSERT INTO table (x, y, z) VALUES (4, 5, 'Hello'), (5, 6, 'Hello');
INSERT INTO table (x, y, z) VALUES (6, 7, 'Hello'), (7, 8, 'Hello');
INSERT INTO table (x, y, z) VALUES (8, 9, 'Hello'), (9, 10, 'Hello');
```

To read data output by this format you can use [MySQLDump](../formats/MySQLDump.md) input format.

## Format Settings {#format-settings}

| Setting                                                                                                                                | Description                                         | Default   |
|----------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------|-----------|
| [`output_format_sql_insert_max_batch_size`](../../operations/settings/settings-formats.md/#output_format_sql_insert_max_batch_size)    | The maximum number of rows in one INSERT statement. | `65505`   |
| [`output_format_sql_insert_table_name`](../../operations/settings/settings-formats.md/#output_format_sql_insert_table_name)            | The name of the table in the output INSERT query.   | `'table'` |
| [`output_format_sql_insert_include_column_names`](../../operations/settings/settings-formats.md/#output_format_sql_insert_include_column_names) | Include column names in INSERT query.               | `true`    |
| [`output_format_sql_insert_use_replace`](../../operations/settings/settings-formats.md/#output_format_sql_insert_use_replace)          | Use REPLACE statement instead of INSERT.            | `false`   |
| [`output_format_sql_insert_quote_names`](../../operations/settings/settings-formats.md/#output_format_sql_insert_quote_names)          | Quote column names with "\`" characters.            | `true`    |

