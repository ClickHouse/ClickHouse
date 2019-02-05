# StripeLog

Engine belongs to the family of log engines. See the common properties of log engines and their differences in the [Log Engine Family](log_family.md) article.

Use this engine in scenarios, when you need to write many tables with the small amount of data (less than 1 million rows).

## Creating a Table {#table_engines-stripelog-creating-a-table}

```
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = StripeLog
```

See the detailed description of [CREATE TABLE](../../query_language/create.md#create-table-query) query.

## Writing the Data {#table_engines-stripelog-writing-the-data}

The `StripeLog` engine stores all the columns in one file. The `Log` and `TinyLog` engines store columns in separate files. For each `INSERT` query, ClickHouse appends data block to the end of a table file, writing columns one by one.

For each table ClickHouse writes two files:

- `data.bin` — Data file.
- `index.mrk` — File with marks. Marks contain offsets for each column of each data block inserted.

The `StripeLog` engine does not support the `ALTER UPDATE` and `ALTER DELETE` operations.

## Reading the Data {#table_engines-stripelog-reading-the-data}

File with marks allows ClickHouse parallelize the reading of data. This means that `SELECT` query returns rows in an unpredictable order. Use the `ORDER BY` clause to sort rows.

## Example of Use {#table_engines-stripelog-example-of-use}

Creating a table:

```sql
CREATE TABLE stripe_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = StripeLog
```

Inserting data:

```sql
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

We used two `INSERT` queries to create two data block inside the `data.bin` file.

When selecting data, ClickHouse uses multiple threads. Each thread reads the separate data block and returns resulting rows independently as it finished. It causes that the order of blocks of rows in the output does not match the order of the same blocks in the input in the most cases. For example:

```sql
SELECT * FROM stripe_log_table
```
```
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
SELECT * FROM stripe_log_table ORDER BY timestamp
```
```
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```

[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/stripelog/) <!--hide-->
