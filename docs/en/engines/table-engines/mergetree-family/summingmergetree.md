---
description: 'SummingMergeTree inherits from the MergeTree engine. Its key feature
  is the ability to automatically sum numeric data during part merges.'
sidebar_label: 'SummingMergeTree'
sidebar_position: 50
slug: /engines/table-engines/mergetree-family/summingmergetree
title: 'SummingMergeTree'
---

# SummingMergeTree

The engine inherits from [MergeTree](/engines/table-engines/mergetree-family/versionedcollapsingmergetree). The difference is that when merging data parts for `SummingMergeTree` tables ClickHouse replaces all the rows with the same primary key (or more accurately, with the same [sorting key](../../../engines/table-engines/mergetree-family/mergetree.md)) with one row which contains summed values for the columns with the numeric data type. If the sorting key is composed in a way that a single key value corresponds to large number of rows, this significantly reduces storage volume and speeds up data selection.

We recommend using the engine together with `MergeTree`. Store complete data in `MergeTree` table, and use `SummingMergeTree` for aggregated data storing, for example, when preparing reports. Such an approach will prevent you from losing valuable data due to an incorrectly composed primary key.

## Creating a Table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = SummingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

For a description of request parameters, see [request description](../../../sql-reference/statements/create/table.md).

### Parameters of SummingMergeTree {#parameters-of-summingmergetree}

#### columns {#columns}

`columns` - a tuple with the names of columns where values will be summed. Optional parameter.
    The columns must be of a numeric type and must not be in the partition or sorting key.

 If `columns` is not specified, ClickHouse summarizes the values in all columns with a numeric data type that are not in the sorting key.

### Query clauses {#query-clauses}

When creating a `SummingMergeTree` table the same [clauses](../../../engines/table-engines/mergetree-family/mergetree.md) are required, as when creating a `MergeTree` table.

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
) ENGINE [=] SummingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
```

All of the parameters excepting `columns` have the same meaning as in `MergeTree`.

- `columns` — tuple with names of columns values of which will be summed. Optional parameter. For a description, see the text above.

</details>

## Usage Example {#usage-example}

Consider the following table:

```sql
CREATE TABLE summtt
(
    key UInt32,
    value UInt32
)
ENGINE = SummingMergeTree()
ORDER BY key
```

Insert data to it:

```sql
INSERT INTO summtt VALUES(1,1),(1,2),(2,1)
```

ClickHouse may sum all the rows not completely ([see below](#data-processing)), so we use an aggregate function `sum` and `GROUP BY` clause in the query.

```sql
SELECT key, sum(value) FROM summtt GROUP BY key
```

```text
┌─key─┬─sum(value)─┐
│   2 │          1 │
│   1 │          3 │
└─────┴────────────┘
```

## Data Processing {#data-processing}

When data are inserted into a table, they are saved as-is. ClickHouse merges the inserted parts of data periodically and this is when rows with the same primary key are summed and replaced with one for each resulting part of data.

ClickHouse can merge the data parts so that different resulting parts of data can consist rows with the same primary key, i.e. the summation will be incomplete. Therefore (`SELECT`) an aggregate function [sum()](/sql-reference/aggregate-functions/reference/sum) and `GROUP BY` clause should be used in a query as described in the example above.

### Common Rules for Summation {#common-rules-for-summation}

The values in the columns with the numeric data type are summed. The set of columns is defined by the parameter `columns`.

If the values were 0 in all of the columns for summation, the row is deleted.

If column is not in the primary key and is not summed, an arbitrary value is selected from the existing ones.

The values are not summed for columns in the primary key.

### The Summation in the Aggregatefunction Columns {#the-summation-in-the-aggregatefunction-columns}

For columns of [AggregateFunction type](../../../sql-reference/data-types/aggregatefunction.md) ClickHouse behaves as [AggregatingMergeTree](../../../engines/table-engines/mergetree-family/aggregatingmergetree.md) engine aggregating according to the function.

### Nested Structures {#nested-structures}

Table can have nested data structures that are processed in a special way.

If the name of a nested table ends with `Map` and it contains at least two columns that meet the following criteria:

- the first column is numeric `(*Int*, Date, DateTime)` or a string `(String, FixedString)`, let's call it `key`,
- the other columns are arithmetic `(*Int*, Float32/64)`, let's call it `(values...)`,

then this nested table is interpreted as a mapping of `key => (values...)`, and when merging its rows, the elements of two data sets are merged by `key` with a summation of the corresponding `(values...)`.

Examples:

```text
DROP TABLE IF EXISTS nested_sum;
CREATE TABLE nested_sum
(
    date Date,
    site UInt32,
    hitsMap Nested(
        browser String,
        imps UInt32,
        clicks UInt32
    )
) ENGINE = SummingMergeTree
PRIMARY KEY (date, site);

INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['Firefox', 'Opera'], [10, 5], [2, 1]);
INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['Chrome', 'Firefox'], [20, 1], [1, 1]);
INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['IE'], [22], [0]);
INSERT INTO nested_sum VALUES ('2020-01-01', 10, ['Chrome'], [4], [3]);

OPTIMIZE TABLE nested_sum FINAL; -- emulate merge 

SELECT * FROM nested_sum;
┌───────date─┬─site─┬─hitsMap.browser───────────────────┬─hitsMap.imps─┬─hitsMap.clicks─┐
│ 2020-01-01 │   10 │ ['Chrome']                        │ [4]          │ [3]            │
│ 2020-01-01 │   12 │ ['Chrome','Firefox','IE','Opera'] │ [20,11,22,5] │ [1,3,0,1]      │
└────────────┴──────┴───────────────────────────────────┴──────────────┴────────────────┘

SELECT
    site,
    browser,
    impressions,
    clicks
FROM
(
    SELECT
        site,
        sumMap(hitsMap.browser, hitsMap.imps, hitsMap.clicks) AS imps_map
    FROM nested_sum
    GROUP BY site
)
ARRAY JOIN
    imps_map.1 AS browser,
    imps_map.2 AS impressions,
    imps_map.3 AS clicks;

┌─site─┬─browser─┬─impressions─┬─clicks─┐
│   12 │ Chrome  │          20 │      1 │
│   12 │ Firefox │          11 │      3 │
│   12 │ IE      │          22 │      0 │
│   12 │ Opera   │           5 │      1 │
│   10 │ Chrome  │           4 │      3 │
└──────┴─────────┴─────────────┴────────┘
```

When requesting data, use the [sumMap(key, value)](../../../sql-reference/aggregate-functions/reference/summap.md) function for aggregation of `Map`.

For nested data structure, you do not need to specify its columns in the tuple of columns for summation.

## Related Content {#related-content}

- Blog: [Using Aggregate Combinators in ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
