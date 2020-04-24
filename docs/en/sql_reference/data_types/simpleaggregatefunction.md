# SimpleAggregateFunction(name, types\_of\_arguments…) {#data-type-simpleaggregatefunction}

`SimpleAggregateFunction` data type stores current value of the aggregate function, and does not store its full state as [`AggregateFunction`](aggregatefunction.md) does. This optimization can be applied to functions for which the following property holds: the result of applying a function `f` to a row set `S1 UNION ALL S2` can be obtained by applying `f` to parts of the row set separately, and then again applying `f` to the results: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`. This property guarantees that partial aggregation results are enough to compute the combined one, so we don't have to store and process any extra data.

The following aggregate functions are supported:

- [`any`](../../sql_reference/aggregate_functions/reference.md#agg_function-any)
- [`anyLast`](../../sql_reference/aggregate_functions/reference.md#anylastx)
- [`min`](../../sql_reference/aggregate_functions/reference.md#agg_function-min)
- [`max`](../../sql_reference/aggregate_functions/reference.md#agg_function-max)
- [`sum`](../../sql_reference/aggregate_functions/reference.md#agg_function-sum)
- [`groupBitAnd`](../../sql_reference/aggregate_functions/reference.md#groupbitand)
- [`groupBitOr`](../../sql_reference/aggregate_functions/reference.md#groupbitor)
- [`groupBitXor`](../../sql_reference/aggregate_functions/reference.md#groupbitxor)


Values of the `SimpleAggregateFunction(func, Type)` look and stored the same way as `Type`, so you do not need to apply functions with `-Merge`/`-State` suffixes. `SimpleAggregateFunction` has better performance than `AggregateFunction` with same aggregation function.

**Parameters**

- Name of the aggregate function.
- Types of the aggregate function arguments.

**Example**

``` sql
CREATE TABLE t
(
    column1 SimpleAggregateFunction(sum, UInt64),
    column2 SimpleAggregateFunction(any, String)
) ENGINE = ...
```

[Original article](https://clickhouse.tech/docs/en/data_types/simpleaggregatefunction/) <!--hide-->
