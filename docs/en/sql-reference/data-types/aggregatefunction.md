---
slug: /en/sql-reference/data-types/aggregatefunction
sidebar_position: 46
sidebar_label: AggregateFunction
---

# AggregateFunction

Aggregate functions can have an implementation-defined intermediate state that can be serialized to an `AggregateFunction(...)` data type and stored in a table, usually, by means of [a materialized view](../../sql-reference/statements/create/view.md). The common way to produce an aggregate function state is by calling the aggregate function with the `-State` suffix. To get the final result of aggregation in the future, you must use the same aggregate function with the `-Merge`suffix.

`AggregateFunction(name, types_of_arguments...)` — parametric data type.

**Parameters**

- Name of the aggregate function. If the function is parametric, specify its parameters too.

- Types of the aggregate function arguments.

**Example**

``` sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

[uniq](../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq), anyIf ([any](../../sql-reference/aggregate-functions/reference/any.md#agg_function-any)+[If](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-if)) and [quantiles](../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) are the aggregate functions supported in ClickHouse.

## Usage

### Data Insertion

To insert data, use `INSERT SELECT` with aggregate `-State`- functions.

**Function examples**

``` sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

In contrast to the corresponding functions `uniq` and `quantiles`, `-State`- functions return the state, instead of the final value. In other words, they return a value of `AggregateFunction` type.

In the results of `SELECT` query, the values of `AggregateFunction` type have implementation-specific binary representation for all of the ClickHouse output formats. If dump data into, for example, `TabSeparated` format with `SELECT` query, then this dump can be loaded back using `INSERT` query.

### Data Selection

When selecting data from `AggregatingMergeTree` table, use `GROUP BY` clause and the same aggregate functions as when inserting data, but using `-Merge`suffix.

An aggregate function with `-Merge` suffix takes a set of states, combines them, and returns the result of complete data aggregation.

For example, the following two queries return the same result:

``` sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

## Usage Example

See [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) engine description.


## Related Content

- Blog: [Using Aggregate Combinators in ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
