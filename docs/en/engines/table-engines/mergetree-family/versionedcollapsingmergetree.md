---
toc_priority: 37
toc_title: VersionedCollapsingMergeTree
---

# VersionedCollapsingMergeTree {#versionedcollapsingmergetree}

This engine:

-   Allows quick writing of object states that are continually changing.
-   Deletes old object states in the background. This significantly reduces the volume of storage.

See the section [Collapsing](#table_engines_versionedcollapsingmergetree) for details.

The engine inherits from [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engines-mergetree) and adds the logic for collapsing rows to the algorithm for merging data parts. `VersionedCollapsingMergeTree` serves the same purpose as [CollapsingMergeTree](../../../engines/table-engines/mergetree-family/collapsingmergetree.md) but uses a different collapsing algorithm that allows inserting the data in any order with multiple threads. In particular, the `Version` column helps to collapse the rows properly even if they are inserted in the wrong order. In contrast, `CollapsingMergeTree` allows only strictly consecutive insertion.

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = VersionedCollapsingMergeTree(sign, version)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

For a description of query parameters, see the [query description](../../../sql-reference/statements/create/table.md).

**Engine Parameters**

``` sql
VersionedCollapsingMergeTree(sign, version)
```

-   `sign` — Name of the column with the type of row: `1` is a “state” row, `-1` is a “cancel” row.

    The column data type should be `Int8`.

-   `version` — Name of the column with the version of the object state.

    The column data type should be `UInt*`.

**Query Clauses**

When creating a `VersionedCollapsingMergeTree` table, the same [clauses](../../../engines/table-engines/mergetree-family/mergetree.md) are required as when creating a `MergeTree` table.

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

!!! attention "Attention"
    Do not use this method in new projects. If possible, switch the old projects to the method described above.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] VersionedCollapsingMergeTree(date-column [, samp#table_engines_versionedcollapsingmergetreeling_expression], (primary, key), index_granularity, sign, version)
```

All of the parameters except `sign` and `version` have the same meaning as in `MergeTree`.

-   `sign` — Name of the column with the type of row: `1` is a “state” row, `-1` is a “cancel” row.

    Column Data Type — `Int8`.

-   `version` — Name of the column with the version of the object state.

    The column data type should be `UInt*`.

</details>

## Collapsing {#table_engines_versionedcollapsingmergetree}

### Data {#data}

Consider a situation where you need to save continually changing data for some object. It is reasonable to have one row for an object and update the row whenever there are changes. However, the update operation is expensive and slow for a DBMS because it requires rewriting the data in the storage. Update is not acceptable if you need to write data quickly, but you can write the changes to an object sequentially as follows.

Use the `Sign` column when writing the row. If `Sign = 1` it means that the row is a state of an object (let’s call it the “state” row). If `Sign = -1` it indicates the cancellation of the state of an object with the same attributes (let’s call it the “cancel” row). Also use the `Version` column, which should identify each state of an object with a separate number.

For example, we want to calculate how many pages users visited on some site and how long they were there. At some point in time we write the following row with the state of user activity:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

At some point later we register the change of user activity and write it with the following two rows.

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

The first row cancels the previous state of the object (user). It should copy all of the fields of the canceled state except `Sign`.

The second row contains the current state.

Because we need only the last state of user activity, the rows

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

can be deleted, collapsing the invalid (old) state of the object. `VersionedCollapsingMergeTree` does this while merging the data parts.

To find out why we need two rows for each change, see [Algorithm](#table_engines-versionedcollapsingmergetree-algorithm).

**Notes on Usage**

1.  The program that writes the data should remember the state of an object in order to cancel it. The “cancel” string should be a copy of the “state” string with the opposite `Sign`. This increases the initial size of storage but allows to write the data quickly.
2.  Long growing arrays in columns reduce the efficiency of the engine due to the load for writing. The more straightforward the data, the better the efficiency.
3.  `SELECT` results depend strongly on the consistency of the history of object changes. Be accurate when preparing data for inserting. You can get unpredictable results with inconsistent data, such as negative values for non-negative metrics like session depth.

### Algorithm {#table_engines-versionedcollapsingmergetree-algorithm}

When ClickHouse merges data parts, it deletes each pair of rows that have the same primary key and version and different `Sign`. The order of rows does not matter.

When ClickHouse inserts data, it orders rows by the primary key. If the `Version` column is not in the primary key, ClickHouse adds it to the primary key implicitly as the last field and uses it for ordering.

## Selecting Data {#selecting-data}

ClickHouse doesn’t guarantee that all of the rows with the same primary key will be in the same resulting data part or even on the same physical server. This is true both for writing the data and for subsequent merging of the data parts. In addition, ClickHouse processes `SELECT` queries with multiple threads, and it cannot predict the order of rows in the result. This means that aggregation is required if there is a need to get completely “collapsed” data from a `VersionedCollapsingMergeTree` table.

To finalize collapsing, write a query with a `GROUP BY` clause and aggregate functions that account for the sign. For example, to calculate quantity, use `sum(Sign)` instead of `count()`. To calculate the sum of something, use `sum(Sign * x)` instead of `sum(x)`, and add `HAVING sum(Sign) > 0`.

The aggregates `count`, `sum` and `avg` can be calculated this way. The aggregate `uniq` can be calculated if an object has at least one non-collapsed state. The aggregates `min` and `max` can’t be calculated because `VersionedCollapsingMergeTree` does not save the history of values of collapsed states.

If you need to extract the data with “collapsing” but without aggregation (for example, to check whether rows are present whose newest values match certain conditions), you can use the `FINAL` modifier for the `FROM` clause. This approach is inefficient and should not be used with large tables.

## Example of Use {#example-of-use}

Example data:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Creating the table:

``` sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8,
    Version UInt8
)
ENGINE = VersionedCollapsingMergeTree(Sign, Version)
ORDER BY UserID
```

Inserting the data:

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1, 1)
```

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1, 1),(4324182021466249494, 6, 185, 1, 2)
```

We use two `INSERT` queries to create two different data parts. If we insert the data with a single query, ClickHouse creates one data part and will never perform any merge.

Getting the data:

``` sql
SELECT * FROM UAct
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

What do we see here and where are the collapsed parts?
We created two data parts using two `INSERT` queries. The `SELECT` query was performed in two threads, and the result is a random order of rows.
Collapsing did not occur because the data parts have not been merged yet. ClickHouse merges data parts at an unknown point in time which we cannot predict.

This is why we need aggregation:

``` sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration,
    Version
FROM UAct
GROUP BY UserID, Version
HAVING sum(Sign) > 0
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │       2 │
└─────────────────────┴───────────┴──────────┴─────────┘
```

If we don’t need aggregation and want to force collapsing, we can use the `FINAL` modifier for the `FROM` clause.

``` sql
SELECT * FROM UAct FINAL
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

This is a very inefficient way to select data. Don’t use it for large tables.

[Original article](https://clickhouse.tech/docs/en/operations/table_engines/versionedcollapsingmergetree/) <!--hide-->
