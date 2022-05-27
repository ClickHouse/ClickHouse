---
sidebar_position: 70
sidebar_label: CollapsingMergeTree
---

# CollapsingMergeTree {#table_engine-collapsingmergetree}

The engine inherits from [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) and adds the logic of rows collapsing to data parts merge algorithm.

`CollapsingMergeTree` asynchronously deletes (collapses) pairs of rows if all of the fields in a sorting key (`ORDER BY`) are equivalent excepting the particular field `Sign` which can have `1` and `-1` values. Rows without a pair are kept. For more details see the [Collapsing](#table_engine-collapsingmergetree-collapsing) section of the document.

The engine may significantly reduce the volume of storage and increase the efficiency of `SELECT` query as a consequence.

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = CollapsingMergeTree(sign)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

For a description of query parameters, see [query description](../../../sql-reference/statements/create/table.md).

**CollapsingMergeTree Parameters**

-   `sign` — Name of the column with the type of row: `1` is a “state” row, `-1` is a “cancel” row.

    Column data type — `Int8`.

**Query clauses**

When creating a `CollapsingMergeTree` table, the same [query clauses](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) are required, as when creating a `MergeTree` table.

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

:::warning    
Do not use this method in new projects and, if possible, switch old projects to the method described above.
:::

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] CollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, sign)
```

All of the parameters excepting `sign` have the same meaning as in `MergeTree`.

-   `sign` — Name of the column with the type of row: `1` — “state” row, `-1` — “cancel” row.

    Column Data Type — `Int8`.

</details>

## Collapsing {#table_engine-collapsingmergetree-collapsing}

### Data {#data}

Consider the situation where you need to save continually changing data for some object. It sounds logical to have one row for an object and update it at any change, but update operation is expensive and slow for DBMS because it requires rewriting of the data in the storage. If you need to write data quickly, update not acceptable, but you can write the changes of an object sequentially as follows.

Use the particular column `Sign`. If `Sign = 1` it means that the row is a state of an object, let’s call it “state” row. If `Sign = -1` it means the cancellation of the state of an object with the same attributes, let’s call it “cancel” row.

For example, we want to calculate how much pages users checked at some site and how long they were there. At some moment we write the following row with the state of user activity:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

At some moment later we register the change of user activity and write it with the following two rows.

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

The first row cancels the previous state of the object (user). It should copy the sorting key fields of the cancelled state excepting `Sign`.

The second row contains the current state.

As we need only the last state of user activity, the rows

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

can be deleted collapsing the invalid (old) state of an object. `CollapsingMergeTree` does this while merging of the data parts.

Why we need 2 rows for each change read in the [Algorithm](#table_engine-collapsingmergetree-collapsing-algorithm) paragraph.

**Peculiar properties of such approach**

1.  The program that writes the data should remember the state of an object to be able to cancel it. “Cancel” string should contain copies of the sorting key fields of the “state” string and the opposite `Sign`. It increases the initial size of storage but allows to write the data quickly.
2.  Long growing arrays in columns reduce the efficiency of the engine due to load for writing. The more straightforward data, the higher the efficiency.
3.  The `SELECT` results depend strongly on the consistency of object changes history. Be accurate when preparing data for inserting. You can get unpredictable results in inconsistent data, for example, negative values for non-negative metrics such as session depth.

### Algorithm {#table_engine-collapsingmergetree-collapsing-algorithm}

When ClickHouse merges data parts, each group of consecutive rows with the same sorting key (`ORDER BY`) is reduced to not more than two rows, one with `Sign = 1` (“state” row) and another with `Sign = -1` (“cancel” row). In other words, entries collapse.

For each resulting data part ClickHouse saves:

1.  The first “cancel” and the last “state” rows, if the number of “state” and “cancel” rows matches and the last row is a “state” row.
2.  The last “state” row, if there are more “state” rows than “cancel” rows.
3.  The first “cancel” row, if there are more “cancel” rows than “state” rows.
4.  None of the rows, in all other cases.

Also when there are at least 2 more “state” rows than “cancel” rows, or at least 2 more “cancel” rows then “state” rows, the merge continues, but ClickHouse treats this situation as a logical error and records it in the server log. This error can occur if the same data were inserted more than once.

Thus, collapsing should not change the results of calculating statistics.
Changes gradually collapsed so that in the end only the last state of almost every object left.

The `Sign` is required because the merging algorithm does not guarantee that all of the rows with the same sorting key will be in the same resulting data part and even on the same physical server. ClickHouse process `SELECT` queries with multiple threads, and it can not predict the order of rows in the result. The aggregation is required if there is a need to get completely “collapsed” data from `CollapsingMergeTree` table.

To finalize collapsing, write a query with `GROUP BY` clause and aggregate functions that account for the sign. For example, to calculate quantity, use `sum(Sign)` instead of `count()`. To calculate the sum of something, use `sum(Sign * x)` instead of `sum(x)`, and so on, and also add `HAVING sum(Sign) > 0`.

The aggregates `count`, `sum` and `avg` could be calculated this way. The aggregate `uniq` could be calculated if an object has at least one state not collapsed. The aggregates `min` and `max` could not be calculated because `CollapsingMergeTree` does not save the values history of the collapsed states.

If you need to extract data without aggregation (for example, to check whether rows are present whose newest values match certain conditions), you can use the `FINAL` modifier for the `FROM` clause. This approach is significantly less efficient.

## Example of Use {#example-of-use}

Example data:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Creation of the table:

``` sql
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

Insertion of the data:

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1)
```

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1),(4324182021466249494, 6, 185, 1)
```

We use two `INSERT` queries to create two different data parts. If we insert the data with one query ClickHouse creates one data part and will not perform any merge ever.

Getting the data:

``` sql
SELECT * FROM UAct
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

What do we see and where is collapsing?

With two `INSERT` queries, we created 2 data parts. The `SELECT` query was performed in 2 threads, and we got a random order of rows. Collapsing not occurred because there was no merge of the data parts yet. ClickHouse merges data part in an unknown moment which we can not predict.

Thus we need aggregation:

``` sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration
FROM UAct
GROUP BY UserID
HAVING sum(Sign) > 0
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

If we do not need aggregation and want to force collapsing, we can use `FINAL` modifier for `FROM` clause.

``` sql
SELECT * FROM UAct FINAL
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

This way of selecting the data is very inefficient. Don’t use it for big tables.

## Example of Another Approach {#example-of-another-approach}

Example data:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │        -5 │     -146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

The idea is that merges take into account only key fields. And in the “Cancel” line we can specify negative values that equalize the previous version of the row when summing without using the Sign column. For this approach, it is necessary to change the data type `PageViews`,`Duration` to store negative values of UInt8 -\> Int16.

``` sql
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

Let’s test the approach:

``` sql
insert into UAct values(4324182021466249494,  5,  146,  1);
insert into UAct values(4324182021466249494, -5, -146, -1);
insert into UAct values(4324182021466249494,  6,  185,  1);

select * from UAct final; // avoid using final in production (just for a test or small tables)
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

``` sql
SELECT
    UserID,
    sum(PageViews) AS PageViews,
    sum(Duration) AS Duration
FROM UAct
GROUP BY UserID
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

``` sql
select count() FROM UAct
```

``` text
┌─count()─┐
│       3 │
└─────────┘
```

``` sql
optimize table UAct final;

select * FROM UAct
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

[Original article](https://clickhouse.com/docs/en/operations/table_engines/collapsingmergetree/) <!--hide-->
