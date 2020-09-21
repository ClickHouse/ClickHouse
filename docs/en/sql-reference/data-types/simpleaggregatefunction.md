# SimpleAggregateFunction {#data-type-simpleaggregatefunction}

`SimpleAggregateFunction(name, types_of_arguments…)` data type stores current value of the aggregate function, and does not store its full state as [`AggregateFunction`](../../sql-reference/data-types/aggregatefunction.md) does. This optimization can be applied to functions for which the following property holds: the result of applying a function `f` to a row set `S1 UNION ALL S2` can be obtained by applying `f` to parts of the row set separately, and then again applying `f` to the results: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`. This property guarantees that partial aggregation results are enough to compute the combined one, so we don’t have to store and process any extra data.

The following aggregate functions are supported:

-   [`any`](../../sql-reference/aggregate-functions/reference/any.md#agg_function-any)
-   [`anyLast`](../../sql-reference/aggregate-functions/reference/anylast.md#anylastx)
-   [`min`](../../sql-reference/aggregate-functions/reference/min.md#agg_function-min)
-   [`max`](../../sql-reference/aggregate-functions/reference/max.md#agg_function-max)
-   [`sum`](../../sql-reference/aggregate-functions/reference/sum.md#agg_function-sum)
-   [`sumWithOverflow`](../../sql-reference/aggregate-functions/reference/sumwithoverflow.md#sumwithoverflowx)
-   [`groupBitAnd`](../../sql-reference/aggregate-functions/reference/groupbitand.md#groupbitand)
-   [`groupBitOr`](../../sql-reference/aggregate-functions/reference/groupbitor.md#groupbitor)
-   [`groupBitXor`](../../sql-reference/aggregate-functions/reference/groupbitxor.md#groupbitxor)
-   [`groupArrayArray`](../../sql-reference/aggregate-functions/reference/grouparray.md#agg_function-grouparray)
-   [`groupUniqArrayArray`](../../sql-reference/aggregate-functions/reference/groupuniqarray.md)

Values of the `SimpleAggregateFunction(func, Type)` look and stored the same way as `Type`, so you do not need to apply functions with `-Merge`/`-State` suffixes. `SimpleAggregateFunction` has better performance than `AggregateFunction` with same aggregation function.

**Parameters**

-   Name of the aggregate function.
-   Types of the aggregate function arguments.

**Example**

``` sql
CREATE TABLE t
(
    column1 SimpleAggregateFunction(sum, UInt64),
    column2 SimpleAggregateFunction(any, String)
) ENGINE = ...
```

[Original article](https://clickhouse.tech/docs/en/data_types/simpleaggregatefunction/) <!--hide-->
