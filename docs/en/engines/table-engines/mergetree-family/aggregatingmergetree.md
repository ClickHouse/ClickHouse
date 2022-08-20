---
sidebar_position: 60
sidebar_label:  AggregatingMergeTree
---

# AggregatingMergeTree

The engine inherits from [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md#table_engines-mergetree), altering the logic for data parts merging. ClickHouse replaces all rows with the same primary key (or more accurately, with the same [sorting key](../../../engines/table-engines/mergetree-family/mergetree.md)) with a single row (within a one data part) that stores a combination of states of aggregate functions.

You can use `AggregatingMergeTree` tables for incremental data aggregation, including for aggregated materialized views.

The engine processes all columns with the following types:

## [AggregateFunction](../../../sql-reference/data-types/aggregatefunction.md)
## [SimpleAggregateFunction](../../../sql-reference/data-types/simpleaggregatefunction.md)

It is appropriate to use `AggregatingMergeTree` if it reduces the number of rows by orders.

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = AggregatingMergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[TTL expr]
[SETTINGS name=value, ...]
```

For a description of request parameters, see [request description](../../../sql-reference/statements/create/table.md).

**Query clauses**

When creating an `AggregatingMergeTree` table the same [clauses](../../../engines/table-engines/mergetree-family/mergetree.md) are required, as when creating a `MergeTree` table.

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

:::warning
Do not use this method in new projects and, if possible, switch the old projects to the method described above.
:::

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] AggregatingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

All of the parameters have the same meaning as in `MergeTree`.
</details>

## SELECT and INSERT {#select-and-insert}

To insert data, use [INSERT SELECT](../../../sql-reference/statements/insert-into.md) query with aggregate -State- functions.
When selecting data from `AggregatingMergeTree` table, use `GROUP BY` clause and the same aggregate functions as when inserting data, but using `-Merge` suffix.

In the results of `SELECT` query, the values of `AggregateFunction` type have implementation-specific binary representation for all of the ClickHouse output formats. If dump data into, for example, `TabSeparated` format with `SELECT` query then this dump can be loaded back using `INSERT` query.

## Example of an Aggregated Materialized View {#example-of-an-aggregated-materialized-view}

`AggregatingMergeTree` materialized view that watches the `test.visits` table:

``` sql
CREATE MATERIALIZED VIEW test.basic
ENGINE = AggregatingMergeTree() PARTITION BY toYYYYMM(StartDate) ORDER BY (CounterID, StartDate)
AS SELECT
    CounterID,
    StartDate,
    sumState(Sign)    AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY CounterID, StartDate;
```

Inserting data into the `test.visits` table.

``` sql
INSERT INTO test.visits ...
```

The data are inserted in both the table and view `test.basic` that will perform the aggregation.

To get the aggregated data, we need to execute a query such as `SELECT ... GROUP BY ...` from the view `test.basic`:

``` sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.basic
GROUP BY StartDate
ORDER BY StartDate;
```

[Original article](https://clickhouse.com/docs/en/operations/table_engines/aggregatingmergetree/) <!--hide-->
