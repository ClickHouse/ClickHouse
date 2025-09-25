---
description: 'Documentation for Table Functions'
sidebar_label: 'Table Functions'
sidebar_position: 1
slug: /sql-reference/table-functions/
title: 'Table Functions'
---

# Table Functions

Table functions are methods for constructing tables.

## Usage {#usage}

Table functions can be used in the [`FROM`](../../sql-reference/statements/select/from.md) 
clause of a `SELECT` query. For example, you can `SELECT` data from a file on your local
machine using the `file` table function.

```bash
echo "1, 2, 3" > example.csv
```
```text
./clickhouse client
:) SELECT * FROM file('example.csv')
┌─c1─┬─c2─┬─c3─┐
│  1 │  2 │  3 │
└────┴────┴────┘
```

You can also use table functions for creating a temporary table that is available
only in the current query. For example:

```sql title="Query"
SELECT * FROM generateSeries(1,5);
```
```response title="Response"
┌─generate_series─┐
│               1 │
│               2 │
│               3 │
│               4 │
│               5 │
└─────────────────┘
```

The table is deleted when the query finishes.

Table functions can be used as a way to create tables, using the following syntax:

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

For example:

```sql title="Query"
CREATE TABLE series AS generateSeries(1, 5);
SELECT * FROM series;
```

```response
┌─generate_series─┐
│               1 │
│               2 │
│               3 │
│               4 │
│               5 │
└─────────────────┘
```

Finally, table functions can be used to `INSERT` data into a table. For example,
we could write out the contents of the table we created in the previous example
to a file on disk using the `file` table function again:

```sql
INSERT INTO FUNCTION file('numbers.csv', 'CSV') SELECT * FROM series;
```

```bash
cat numbers.csv
1
2
3
4
5
```

:::note
You can't use table functions if the [allow_ddl](/operations/settings/settings#allow_ddl) setting is disabled.
:::
