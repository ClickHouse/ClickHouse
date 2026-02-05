---
description: 'CoalescingMergeTree inherits from the MergeTree engine. Its key feature
  is the ability to automatically store last non-null value of each column during part merges.'
sidebar_label: 'CoalescingMergeTree'
sidebar_position: 50
slug: /engines/table-engines/mergetree-family/coalescingmergetree
title: 'CoalescingMergeTree table engine'
keywords: ['CoalescingMergeTree']
show_related_blogs: true
doc_type: 'reference'
---

# CoalescingMergeTree table engine

:::note Available from version 25.6
This table engine is available from version 25.6 and higher in both OSS and Cloud.
:::

This engine inherits from [MergeTree](/engines/table-engines/mergetree-family/mergetree). The key difference is in how data parts are merged: for `CoalescingMergeTree` tables, ClickHouse replaces all rows with the same primary key (or more precisely, the same [sorting key](../../../engines/table-engines/mergetree-family/mergetree.md)) with a single row that contains the latest non-NULL values for each column.

This enables column-level upserts, meaning you can update only specific columns rather than entire rows.

`CoalescingMergeTree` is intended for use with Nullable types in non-key columns. If the columns are not Nullable, the behavior is the same as with [ReplacingMergeTree](/engines/table-engines/mergetree-family/replacingmergetree).

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = CoalescingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

For a description of request parameters, see [request description](../../../sql-reference/statements/create/table.md).

### Parameters of CoalescingMergeTree {#parameters-of-coalescingmergetree}

#### Columns {#columns}

`columns` - a tuple with the names of columns where values will be united. Optional parameter.
    The columns must be of a numeric type and must not be in the partition or sorting key.

 If `columns` is not specified, ClickHouse unites the values in all columns that are not in the sorting key.

### Query clauses {#query-clauses}

When creating a `CoalescingMergeTree` table the same [clauses](../../../engines/table-engines/mergetree-family/mergetree.md) are required, as when creating a `MergeTree` table.

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

:::note
Do not use this method in new projects and, if possible, switch the old projects to the method described above.
:::

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] CoalescingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
```

All of the parameters excepting `columns` have the same meaning as in `MergeTree`.

- `columns` — tuple with names of columns values of which will be summed. Optional parameter. For a description, see the text above.

</details>

## Usage example {#usage-example}

Consider the following table:

```sql
CREATE TABLE test_table
(
    key UInt64,
    value_int Nullable(UInt32),
    value_string Nullable(String),
    value_date Nullable(Date)
)
ENGINE = CoalescingMergeTree()
ORDER BY key
```

Insert data to it:

```sql
INSERT INTO test_table VALUES(1, NULL, NULL, '2025-01-01'), (2, 10, 'test', NULL);
INSERT INTO test_table VALUES(1, 42, 'win', '2025-02-01');
INSERT INTO test_table(key, value_date) VALUES(2, '2025-02-01');
```

The result will looks like this:

```sql
SELECT * FROM test_table ORDER BY key;
```

```text
┌─key─┬─value_int─┬─value_string─┬─value_date─┐
│   1 │        42 │ win          │ 2025-02-01 │
│   1 │      ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │ 2025-01-01 │
│   2 │      ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │ 2025-02-01 │
│   2 │        10 │ test         │       ᴺᵁᴸᴸ │
└─────┴───────────┴──────────────┴────────────┘
```

Recommended query for correct and final result:

```sql
SELECT * FROM test_table FINAL ORDER BY key;
```

```text
┌─key─┬─value_int─┬─value_string─┬─value_date─┐
│   1 │        42 │ win          │ 2025-02-01 │
│   2 │        10 │ test         │ 2025-02-01 │
└─────┴───────────┴──────────────┴────────────┘
```

Using the `FINAL` modifier forces ClickHouse to apply merge logic at query time, ensuring you get the correct, coalesced "latest" value for each column. This is the safest and most accurate method when querying from a CoalescingMergeTree table.

:::note

An approach with `GROUP BY` may return incorrect results if the underlying parts have not been fully merged.

```sql
SELECT key, last_value(value_int), last_value(value_string), last_value(value_date)  FROM test_table GROUP BY key; -- Not recommended.
```

:::
