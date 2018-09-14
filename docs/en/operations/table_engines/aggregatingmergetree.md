# AggregatingMergeTree

This engine differs from `MergeTree` in that the merge combines the states of aggregate functions stored in the table for rows with the same primary key value.

For this to work, it uses the `AggregateFunction` data type, as well as `-State` and `-Merge` modifiers for aggregate functions. Let's examine it more closely.

There is an `AggregateFunction` data type. It is a parametric data type. As parameters, the name of the aggregate function is passed, then the types of its arguments.

Examples:

```sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

This type of column stores the state of an aggregate function.

To get this type of value, use aggregate functions with the `State` suffix.

Example:`uniqState(UserID), quantilesState(0.5, 0.9)(SendTiming)`

In contrast to the corresponding `uniq` and `quantiles` functions, these functions return the state, rather than the prepared value. In other words, they return an `AggregateFunction` type value.

An `AggregateFunction` type value can't be output in Pretty formats. In other formats, these types of values are output as implementation-specific binary data. The `AggregateFunction` type values are not intended for output or saving in a dump.

The only useful thing you can do with `AggregateFunction` type values is to combine the states and get a result, which essentially means to finish aggregation. Aggregate functions with the 'Merge' suffix are used for this purpose.
Example: `uniqMerge(UserIDState)`, where `UserIDState` has the `AggregateFunction` type.

In other words, an aggregate function with the 'Merge' suffix takes a set of states, combines them, and returns the result.
As an example, these two queries return the same result:

```sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

There is an `AggregatingMergeTree` engine. Its job during a merge is to combine the states of aggregate functions from different table rows with the same primary key value.

You can't use a normal INSERT to insert a row in a table containing `AggregateFunction` columns, because you can't explicitly define the `AggregateFunction` value. Instead, use `INSERT SELECT` with `-State` aggregate functions for inserting data.

With SELECT from an `AggregatingMergeTree` table, use GROUP BY and aggregate functions with the '-Merge' modifier in order to complete data aggregation.

You can use `AggregatingMergeTree` tables for incremental data aggregation, including for aggregated materialized views.

Example:

Create an `AggregatingMergeTree` materialized view that watches the `test.visits` table:

```sql
CREATE MATERIALIZED VIEW test.basic
ENGINE = AggregatingMergeTree(StartDate, (CounterID, StartDate), 8192)
AS SELECT
    CounterID,
    StartDate,
    sumState(Sign)    AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY CounterID, StartDate;
```

Insert data in the `test.visits` table. Data will also be inserted in the view, where it will be aggregated:

```sql
INSERT INTO test.visits ...
```

Perform `SELECT` from the view using `GROUP BY` in order to complete data aggregation:

```sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.basic
GROUP BY StartDate
ORDER BY StartDate;
```

You can create a materialized view like this and assign a normal view to it that finishes data aggregation.

Note that in most cases, using `AggregatingMergeTree` is not justified, since queries can be run efficiently enough on non-aggregated data.

