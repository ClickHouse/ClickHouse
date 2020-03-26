# AggregateFunction(name, types\_of\_arguments…) {#data-type-aggregatefunction}

The intermediate state of an aggregate function. To get it, use aggregate functions with the `-State` suffix. To get aggregated data in the future, you must use the same aggregate functions with the `-Merge`suffix.

`AggregateFunction` — parametric data type.

**Parameters**

-   Name of the aggregate function.

        If the function is parametric specify its parameters too.

-   Types of the aggregate function arguments.

**Example**

``` sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

[uniq](../../query_language/agg_functions/reference.md#agg_function-uniq), anyIf ([any](../../query_language/agg_functions/reference.md#agg_function-any)+[If](../../query_language/agg_functions/combinators.md#agg-functions-combinator-if)) and [quantiles](../../query_language/agg_functions/reference.md) are the aggregate functions supported in ClickHouse.

## Usage {#usage}

### Data Insertion {#data-insertion}

To insert data, use `INSERT SELECT` with aggregate `-State`- functions.

**Function examples**

``` sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

In contrast to the corresponding functions `uniq` and `quantiles`, `-State`- functions return the state, instead of the final value. In other words, they return a value of `AggregateFunction` type.

In the results of `SELECT` query, the values of `AggregateFunction` type have implementation-specific binary representation for all of the ClickHouse output formats. If dump data into, for example, `TabSeparated` format with `SELECT` query then this dump can be loaded back using `INSERT` query.

### Data Selection {#data-selection}

When selecting data from `AggregatingMergeTree` table, use `GROUP BY` clause and the same aggregate functions as when inserting data, but using `-Merge`suffix.

An aggregate function with `-Merge` suffix takes a set of states, combines them, and returns the result of complete data aggregation.

For example, the following two queries return the same result:

``` sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

## Usage Example {#usage-example}

See [AggregatingMergeTree](../../operations/table_engines/aggregatingmergetree.md) engine description.

[Original article](https://clickhouse.tech/docs/en/data_types/nested_data_structures/aggregatefunction/) <!--hide-->
