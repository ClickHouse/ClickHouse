---
toc_priority: 45
toc_title: AggregatingMemory
---

# AggregatingMemory {#aggregatingmemory}

The `AggregatingMemory` is a special engine, which allows incrementally aggregate data, keeping it all in the memory. When writing to this table, incoming data is aggregated as specified in the AS `SELECT` query, and internal memory state is updated. Every `INSERT` to this table is processed incrementally, and updates internal state without iterating over previously aggregated data. `SELECT` from the table returns current aggregation state, and can be called any amount of times.

When restarting a server, data disappears from the table and the table becomes empty. 

You can use `AggregatingMemory` tables for incremental data aggregation, including for aggregated materialized views.

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
ENGINE = AggregatingMemory()
AS SELECT
    expr1 [AS column1],
    expr2 [AS column2],
    ...
FROM [src_db.]src_table
[GROUP BY key1 [, key2, ...]]
[SETTINGS name=value, ...]
```

`AS SELECT` query not used to fill the table at the creation, and is used to specify aggregation query. `FROM` table is used only for the structure, and the table allows `INSERT` of any data with matching structure.

For a description of request parameters, see [request description](../../../sql-reference/statements/create/table.md).

## SELECT and INSERT {#select-and-insert}

To insert data, you can use [INSERT SELECT](../../../sql-reference/statements/insert-into.md) query with the data from the table matching structure before aggreagtion.

The results of `SELECT` query matches the result of executing provided `SELECT` query on the all inserted data.

## Example of an Aggregated Materialized View {#example-of-an-aggregated-materialized-view}

`AggregatingMemory` materialized view that watches the `test.visits` table:

``` sql
CREATE MATERIALIZED VIEW test.basic
ENGINE = AggregatingMemory()
AS SELECT
    CounterID,
    StartDate,
    sum(Sign)    AS Visits,
    uniq(UserID) AS Users
FROM test.visits
GROUP BY CounterID, StartDate;
```

Inserting data into the `test.visits` table.

``` sql
INSERT INTO test.visits ...
```

The data are inserted in both the table and view `test.basic` that will perform the aggregation.

To get the aggregated data, we need to execute a simple `SELECT` query from the view `test.basic`:

``` sql
SELECT
    CounterID,
    StartDate,
    Visits,
    Users
FROM test.basic
ORDER BY CounterID, StartDate;
```

[Original article](https://clickhouse.tech/docs/en/operations/special/aggregatingmemory/) <!--hide-->
