<a name="table_engine-versionedcollapsingmergetree"></a>

# VersionedCollapsingMergeTree

This engine:

- Allows quick writing of continually changing states of objects.
- Deletes old states of objects in the background. It causes to significant reduction of the volume of storage.

See the section [Collapsing](#versionedcollapsingmergetree-collapsing) for details.

The engine inherits from [MergeTree](mergetree.md#table_engines-mergetree) and adds the logic of rows collapsing to data parts merge algorithm. `VersionedCollapsingMergeTree` solves the same problem as the [CollapsingMergeTree](collapsingmergetree.md#table_engine-collapsingmergetree) but uses another algorithm of collapsing. It allows inserting the data in any order with multiple threads. The particular `Version` column helps to collapse the rows properly even if they are inserted in the wrong order. `CollapsingMergeTree` allows only strictly consecutive insertion.

## Creating a Table

```sql
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

For a description of query parameters, see [query description](../../query_language/create.md#query_language-queries-create_table).

**Engine Parameters**

```
VersionedCollapsingMergeTree(sign, version)
```

- `sign` — Name of the column with the type of row: `1` is a "state" row, `-1` is a "cancel" row.

    Column data type should be `Int8`.

- `version` — Name of the column with the version of object state.

    Column data type should be `UInt*`.

**Query Clauses**

When creating a `VersionedCollapsingMergeTree` table, the same [clauses](mergetree.md#table_engines-mergetree-configuring) are required, as when creating a `MergeTree` table.

<details markdown="1"><summary>Deprecated Method for Creating a Table</summary>

!!! attention
    Do not use this method in new projects and, if possible, switch the old projects to the method described above.

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] VersionedCollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, sign, version)
```

All of the parameters excepting `sign` and `version` have the same meaning as in `MergeTree`.

- `sign` — Name of the column with the type of row: `1` — "state" row, `-1` — "cancel" row.

    Column Data Type — `Int8`.

- `version` — Name of the column with the version of object state.

    Column data type should be `UInt*`.
</details>

<a name="versionedcollapsingmergetree-collapsing"></a>

## Collapsing

### Data

Consider the situation where you need to save continually changing data for some object, it is reasonable to have one row for an object and update it at any change. Update operation is expensive and slow for DBMS because it requires rewriting of the data in the storage. If you need to write data quickly, update not acceptable, but you can write the changes of an object sequentially as follows.

Use the particular column `Sign` when writing row. If `Sign = 1` it means that the row is a state of an object, let's call it "state" row. If `Sign = -1` it means the cancellation of the state of an object with the same attributes, let's call it "cancel" row. Also, use the particular column `Version` which should identify each state of an object with a separate number.

For example, we want to calculate how much pages users checked at some site and how long they were there. At some moment of time we write the following row with the state of user activity:

```
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

At some moment later we register the change of user activity and write it with the following two rows.

```
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

The first row cancels the previous state of the object (user). It should copy all of the fields of the canceled state excepting `Sign`.

The second row contains the current state.

As we need only the last state of user activity, the rows

```
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

can be deleted collapsing the invalid (old) state of an object. `VesionedCollapsingMergeTree` does this while merging of the data parts.

Why we need 2 rows for each change read in the "Algorithm" paragraph.

**Peculiar properties of such approach**

1. The program that writes the data should remember the state of an object to be able to cancel it. "Cancel" string should be the copy of "state" string with the opposite `Sign`. It increases the initial size of storage but allows to write the data quickly.
2. Long growing arrays in columns reduce the efficiency of the engine due to load for writing. The more straightforward data, the higher efficiency.
3. `SELECT` results depend strongly on the consistency of object changes history. Be accurate when preparing data for inserting. You can get unpredictable results in inconsistent data, for example, negative values for non-negative metrics such as session depth.

### Algorithm

When ClickHouse merges data parts, it deletes each pair of rows having the same primary key and version and different `Sign`. The order of rows does not matter.

When ClickHouse merges data parts, it orders rows by the primary key. If the `Version` column is not in the primary key, ClickHouse adds it to the primary key implicitly as the last field and use for ordering.

## Selecting Data

ClickHouse doesn't guarantee that all of the rows with the same primary key will be in the same resulting data part and even on the same physical server. It's true for writing the data and for subsequent merging of the data parts. Also, ClickHouse process `SELECT` queries with multiple threads, and it can not predict the order of rows in the result. So the aggregation is required if there is a need to get completely "collapsed" data from `VersionedCollapsingMergeTree` table.

To finalize collapsing write a query with `GROUP BY` clause and aggregate functions that account for the sign. For example, to calculate quantity, use `sum(Sign)` instead of `count()`. To calculate the sum of something, use `sum(Sign * x)` instead of `sum(x)`, and so on, and also add `HAVING sum(Sign) > 0`.

The aggregates `count`, `sum` and `avg` could be calculated this way. The aggregate `uniq` could be calculated if an object has at list one state not collapsed. The aggregates `min` and `max` could not be calculated because `VersionedCollapsingMergeTree` does not save values history of the collapsed states.

If you need to extract the data with "collapsing" but without of aggregation (for example, to check whether rows are present whose newest values match certain conditions), you can use the `FINAL` modifier for the `FROM` clause. This approach is inefficient and should not be used with big tables.

## Example of Use

Example data:

```
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Creation of the table:

```sql
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

Insertion of the data:

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1, 1)
```
```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1, 1),(4324182021466249494, 6, 185, 1, 2)
```

We use two `INSERT` queries to create two different data parts. If we insert the data with one query ClickHouse creates one data part and will not perform any merge ever.

Getting the data:

```
SELECT * FROM UAct
```

```
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

What do we see and where is collapsing?
With two `INSERT` queries, we created 2 data parts. The `SELECT` query have performed in 2 threads, and we got a random order of rows.
Collapsing not occurred because there was no merge of the data parts yet. ClickHouse merges data part in an unknown moment of time which we can not predict.

Thus we need aggregation:

```sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration,
    Version
FROM UAct
GROUP BY UserID, Version
HAVING sum(Sign) > 0
```
```
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │       2 │
└─────────────────────┴───────────┴──────────┴─────────┘
```

If we do not need aggregation and want to force collapsing, we can use `FINAL` modifier for `FROM` clause.

```sql
SELECT * FROM UAct FINAL
```
```
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

This way of selecting the data is very inefficient. Don't use it for big tables.

[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/versionedcollapsingmergetree/) <!--hide-->
