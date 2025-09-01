---
description: 'Inherits from MergeTree but adds logic for collapsing rows during the
  merge process.'
keywords: ['updates', 'collapsing']
sidebar_label: 'CollapsingMergeTree'
sidebar_position: 70
slug: /engines/table-engines/mergetree-family/collapsingmergetree
title: 'CollapsingMergeTree'
---

# CollapsingMergeTree

## Description {#description}

The `CollapsingMergeTree` engine inherits from [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)
and adds logic for collapsing rows during the merge process.
The `CollapsingMergeTree` table engine asynchronously deletes (collapses) 
pairs of rows if all the fields in a sorting key (`ORDER BY`) are equivalent except for the special field `Sign`, 
which can have values of either `1` or `-1`. 
Rows without a pair of opposite valued `Sign` are kept. 

For more details, see the [Collapsing](#table_engine-collapsingmergetree-collapsing) section of the document.

:::note
This engine may significantly reduce the volume of storage,
increasing the efficiency of `SELECT` queries as a consequence.
:::

## Parameters {#parameters}

All parameters of this table engine, with the exception of the `Sign` parameter,
have the same meaning as in [`MergeTree`](/engines/table-engines/mergetree-family/mergetree).

- `Sign` — The name given to a column with the type of row where `1` is a "state" row and `-1` is a "cancel" row. Type: [Int8](/sql-reference/data-types/int-uint).

## Creating a Table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) 
ENGINE = CollapsingMergeTree(Sign)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

:::note
The method below is not recommended for use in new projects. 
We advise, if possible, to update old projects to use the new method.
:::

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) 
ENGINE [=] CollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, Sign)
```

`Sign` — The name given to a column with the type of row where `1` is a "state" row and `-1` is a "cancel" row. [Int8](/sql-reference/data-types/int-uint).

</details>

- For a description of query parameters, see [query description](../../../sql-reference/statements/create/table.md).
- When creating a `CollapsingMergeTree` table, the same [query clauses](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) are required, as when creating a `MergeTree` table.

## Collapsing {#table_engine-collapsingmergetree-collapsing}

### Data {#data}

Consider the situation where you need to save continually changing data for some given object.
It may sound logical to have one row per object and update it anytime something changes,
however, update operations are expensive and slow for the DBMS because they require rewriting the data in storage. 
If we need to write data quickly, performing large numbers of updates is not an acceptable approach,
but we can always write the changes of an object sequentially.
To do so, we make use of the special column `Sign`.

- If `Sign` = `1` it means that the row is a "state" row: _a row containing fields which represent a current valid state_. 
- If `Sign` = `-1` it means that the row is a "cancel" row: _a row used for the cancellation of state of an object with the same attributes_.

For example, we want to calculate how many pages users checked on some website and how long they visited them for. 
At some given moment in time, we write the following row with the state of user activity:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

At a later moment in time, we register the change of user activity and write it with the following two rows:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

The first row cancels the previous state of the object (representing a user in this case). 
It should copy all the sorting key fields for the "canceled" row except for `Sign`. 
The second row above contains the current state.

As we need only the last state of user activity, the original "state" row and the "cancel" 
row that we inserted can be deleted as shown below, collapsing the invalid (old) state of an object:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │ -- old "state" row can be deleted
│ 4324182021466249494 │         5 │      146 │   -1 │ -- "cancel" row can be deleted
│ 4324182021466249494 │         6 │      185 │    1 │ -- new "state" row remains
└─────────────────────┴───────────┴──────────┴──────┘
```

`CollapsingMergeTree` carries out precisely this _collapsing_ behavior while merging of the data parts takes place.

:::note
The reason for why two rows are needed for each change 
is further discussed in the [Algorithm](#table_engine-collapsingmergetree-collapsing-algorithm) paragraph.
:::

**The peculiarities of such an approach**

1.  The program that writes the data should remember the state of an object to be able to cancel it. The "cancel" row should contain copies of sorting key fields of the "state" and the opposite `Sign`. This increases the initial size of storage but allows us to write the data quickly.
2.  Long growing arrays in columns reduce the efficiency of the engine due to the increased load for writing. The more straightforward the data, the higher the efficiency.
3.  The `SELECT` results depend strongly on the consistency of the object change history. Be accurate when preparing data for inserting. You can get unpredictable results with inconsistent data. For example, negative values for non-negative metrics such as session depth.

### Algorithm {#table_engine-collapsingmergetree-collapsing-algorithm}

When ClickHouse merges data [parts](/concepts/glossary#parts), 
each group of consecutive rows with the same sorting key (`ORDER BY`) is reduced to no more than two rows,
the "state" row with `Sign` = `1` and the "cancel" row with `Sign` = `-1`. 
In other words, in ClickHouse entries collapse.

For each resulting data part ClickHouse saves:

|  |                                                                                                                                     |
|--|-------------------------------------------------------------------------------------------------------------------------------------|
|1.| The first "cancel" and the last "state" rows, if the number of "state" and "cancel" rows matches and the last row is a "state" row. |
|2.| The last "state" row, if there are more "state" rows than "cancel" rows.                                                            |
|3.| The first "cancel" row, if there are more "cancel" rows than "state" rows.                                                          |
|4.| None of the rows, in all other cases.                                                                                               |

Additionally, when there are at least two more "state" rows than "cancel" 
rows, or at least two more "cancel" rows than "state" rows, the merge continues.
ClickHouse, however, treats this situation as a logical error and records it in the server log. 
This error can occur if the same data is inserted more than once. 
Thus, collapsing should not change the results of calculating statistics.
Changes are gradually collapsed so that in the end only the last state of almost every object is left.

The `Sign` column is required because the merging algorithm does not guarantee 
that all the rows with the same sorting key will be in the same resulting data part and even on the same physical server. 
ClickHouse processes `SELECT` queries with multiple threads, and it cannot predict the order of rows in the result. 

Aggregation is required if there is a need to get completely "collapsed" data from the `CollapsingMergeTree` table.
To finalize collapsing, write a query with the `GROUP BY` clause and aggregate functions that account for the sign. 
For example, to calculate quantity, use `sum(Sign)` instead of `count()`. 
To calculate the sum of something, use `sum(Sign * x)` together `HAVING sum(Sign) > 0` instead of `sum(x)`
as in the [example](#example-of-use) below.

The aggregates `count`, `sum` and `avg` could be calculated this way. 
The aggregate `uniq` could be calculated if an object has at least one non-collapsed state. 
The aggregates `min` and `max` could not be calculated 
because `CollapsingMergeTree` does not save the history of the collapsed states.

:::note
If you need to extract data without aggregation 
(for example, to check whether rows whose newest values match certain conditions are present), 
you can use the [`FINAL`](../../../sql-reference/statements/select/from.md#final-modifier) modifier for the `FROM` clause. It will merge the data before returning the result.
For CollapsingMergeTree, only the latest state row for each key is returned.
:::

## Examples {#examples}

### Example of Use {#example-of-use}

Given the following example data:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Let's create a table `UAct` using the `CollapsingMergeTree`:

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
```

Next we will insert some data:

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1)
```

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1),(4324182021466249494, 6, 185, 1)
```

We use two `INSERT` queries to create two different data parts. 

:::note
If we insert the data with a single query, ClickHouse creates only one data part and will not perform any merge ever.
:::

We can select the data using:

```sql
SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Let's take a look at the returned data above and see if collapsing occurred...
With two `INSERT` queries, we created two data parts. 
The `SELECT` query was performed in two threads, and we got a random order of rows. 
However, collapsing **did not occur** because there was no merge of the data parts yet 
and ClickHouse merges data parts in the background at an unknown moment which we cannot predict.

We therefore need an aggregation 
which we perform with the [`sum`](/sql-reference/aggregate-functions/reference/sum) 
aggregate function and the [`HAVING`](/sql-reference/statements/select/having) clause:

```sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration
FROM UAct
GROUP BY UserID
HAVING sum(Sign) > 0
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

If we do not need aggregation and want to force collapsing, we can also use the `FINAL` modifier for `FROM` clause.

```sql
SELECT * FROM UAct FINAL
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```
:::note
This way of selecting the data is less efficient and is not recommended for use with large amounts of scanned data (millions of rows).
:::

### Example of Another Approach {#example-of-another-approach}

The idea with this approach is that merges take into account only key fields.
In the "cancel" row, we can therefore specify negative values
that equalize the previous version of the row when summing without using the `Sign` column.

For this example, we will make use of the sample data below:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │        -5 │     -146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

For this approach, it is necessary to change the data types of `PageViews` and `Duration` to store negative values. 
We therefore change the values of these columns from `UInt8` to `Int16` when we create our table `UAct` using the
`collapsingMergeTree`:

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews Int16,
    Duration Int16,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
```

Let's test the approach by inserting data into our table. 

For examples or small tables, it is, however, acceptable:

```sql
INSERT INTO UAct VALUES(4324182021466249494,  5,  146,  1);
INSERT INTO UAct VALUES(4324182021466249494, -5, -146, -1);
INSERT INTO UAct VALUES(4324182021466249494,  6,  185,  1);

SELECT * FROM UAct FINAL;
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

```sql
SELECT
    UserID,
    sum(PageViews) AS PageViews,
    sum(Duration) AS Duration
FROM UAct
GROUP BY UserID
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

```sql
SELECT COUNT() FROM UAct
```

```text
┌─count()─┐
│       3 │
└─────────┘
```

```sql
OPTIMIZE TABLE UAct FINAL;

SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```
