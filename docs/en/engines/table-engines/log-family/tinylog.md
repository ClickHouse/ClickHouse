---
description: 'Documentation for TinyLog'
slug: /engines/table-engines/log-family/tinylog
toc_priority: 34
toc_title: 'TinyLog'
title: 'TinyLog'
---

# TinyLog

The engine belongs to the log engine family. See [Log Engine Family](../../../engines/table-engines/log-family/index.md) for common properties of log engines and their differences.

This table engine is typically used with the write-once method: write data one time, then read it as many times as necessary. For example, you can use `TinyLog`-type tables for intermediary data that is processed in small batches. Note that storing data in a large number of small tables is inefficient.

Queries are executed in a single stream. In other words, this engine is intended for relatively small tables (up to about 1,000,000 rows). It makes sense to use this table engine if you have many small tables, since it's simpler than the [Log](../../../engines/table-engines/log-family/log.md) engine (fewer files need to be opened).

## Characteristics {#characteristics}

- **Simpler Structure**: Unlike the Log engine, TinyLog does not use mark files. This reduces complexity but also limits performance optimizations for large datasets.
- **Single Stream Queries**: Queries on TinyLog tables are executed in a single stream, making it suitable for relatively small tables, typically up to 1,000,000 rows.
- **Efficient for Small Table**: The simplicity of the TinyLog engine makes it advantageous when managing many small tables, as it requires fewer file operations compared to the Log engine.

Unlike the Log engine, TinyLog does not use mark files. This reduces complexity but also limits performance optimizations for larger datasets.

## Creating a Table {#table_engines-tinylog-creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = TinyLog
```

See the detailed description of the [CREATE TABLE](/sql-reference/statements/create/table) query.

## Writing the Data {#table_engines-tinylog-writing-the-data}

The `TinyLog` engine stores all the columns in one file. For each `INSERT` query, ClickHouse appends the data block to the end of a table file, writing columns one by one.

For each table ClickHouse writes the files:

- `<column>.bin`: A data file for each column, containing the serialized and compressed data.

The `TinyLog` engine does not support the `ALTER UPDATE` and `ALTER DELETE` operations.

## Example of Use {#table_engines-tinylog-example-of-use}

Creating a table:

```sql
CREATE TABLE tiny_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = TinyLog
```

Inserting data:

```sql
INSERT INTO tiny_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO tiny_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

We used two `INSERT` queries to create two data blocks inside the `<column>.bin` files.

ClickHouse uses a single stream selecting data. As a result, the order of blocks of rows in the output matches the order of the same blocks in the input. For example:

```sql
SELECT * FROM tiny_log_table
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2024-12-10 13:11:58 │ REGULAR      │ The first regular message  │
│ 2024-12-10 13:12:12 │ REGULAR      │ The second regular message │
│ 2024-12-10 13:12:12 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```
