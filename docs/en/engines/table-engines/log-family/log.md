---
description: 'Documentation for Log'
slug: /engines/table-engines/log-family/log
toc_priority: 33
toc_title: 'Log'
title: 'Log'
---

# Log

The engine belongs to the family of `Log` engines. See the common properties of `Log` engines and their differences in the [Log Engine Family](../../../engines/table-engines/log-family/index.md) article.

`Log` differs from [TinyLog](../../../engines/table-engines/log-family/tinylog.md) in that a small file of "marks" resides with the column files. These marks are written on every data block and contain offsets that indicate where to start reading the file in order to skip the specified number of rows. This makes it possible to read table data in multiple threads.
For concurrent data access, the read operations can be performed simultaneously, while write operations block reads and each other.
The `Log` engine does not support indexes. Similarly, if writing to a table failed, the table is broken, and reading from it returns an error. The `Log` engine is appropriate for temporary data, write-once tables, and for testing or demonstration purposes.

## Creating a Table {#table_engines-log-creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Log
```

See the detailed description of the [CREATE TABLE](/sql-reference/statements/create/table) query.

## Writing the Data {#table_engines-log-writing-the-data}

The `Log` engine efficiently stores data by writing each column to its own file.  For every table, the Log engine writes the following files to the specified storage path:

- `<column>.bin`: A data file for each column, containing the serialized and compressed data.
`__marks.mrk`: A marks file, storing offsets and row counts for each data block inserted. Marks are used to facilitate efficient query execution by allowing the engine to skip irrelevant data blocks during reads.

### Writing Process {#writing-process}

When data is written to a `Log` table:

1.    Data is serialized and compressed into blocks.
2.    For each column, the compressed data is appended to its respective `<column>.bin` file.
3.    Corresponding entries are added to the `__marks.mrk` file to record the offset and row count of the newly inserted data.

## Reading the Data {#table_engines-log-reading-the-data}

The file with marks allows ClickHouse to parallelize the reading of data. This means that a `SELECT` query returns rows in an unpredictable order. Use the `ORDER BY` clause to sort rows.

## Example of Use {#table_engines-log-example-of-use}

Creating a table:

```sql
CREATE TABLE log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = Log
```

Inserting data:

```sql
INSERT INTO log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

We used two `INSERT` queries to create two data blocks inside the `<column>.bin` files.

ClickHouse uses multiple threads when selecting data. Each thread reads a separate data block and returns resulting rows independently as it finishes. As a result, the order of blocks of rows in the output may not match the order of the same blocks in the input. For example:

```sql
SELECT * FROM log_table
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
┌───────────timestamp─┬─message_type─┬─message───────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message │
└─────────────────────┴──────────────┴───────────────────────────┘
```

Sorting the results (ascending order by default):

```sql
SELECT * FROM log_table ORDER BY timestamp
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```
