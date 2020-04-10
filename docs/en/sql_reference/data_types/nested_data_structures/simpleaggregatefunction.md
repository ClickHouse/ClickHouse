# SimpleAggregateFunction(name, types\_of\_arguments…) {#data-type-simpleaggregatefunction}

Unlike [`AggregateFunction`](../aggregatefunction.md), which stores not the value of the aggregate function but it's state:

- `SimpleAggregateFunction` data type stores current value of aggregation and does not store aggregation state as [`AggregateFunction`](../aggregatefunction.md).
.
It supports simple stateless aggregate functions, including:

    - [`any`](../../query_language/agg_functions/reference.md#agg_function-any)
    - [`anyLast`](../../query_language/agg_functions/reference.md#anylastx)
    - [`min`](../../query_language/agg_functions/reference.md#agg_function-min)
    - [`max`](../../query_language/agg_functions/reference.md#agg_function-max)
    - [`sum`](../../query_language/agg_functions/reference.md#agg_function-sum)
    - [`groupBitAnd`](../../query_language/agg_functions/reference.md#groupbitand)
    - [`groupBitOr`](../../query_language/agg_functions/reference.md#groupbitor)
    - [`groupBitXor`](../../query_language/agg_functions/reference.md#groupbitxor)

- Type of the `SimpleAggregateFunction(func, Type)` is `Type` itself, so you do not need to apply functions with `-Merge`/`-State` suffixes.
- `SimpleAggregateFunction` has better performance than `AggregateFunction` with same aggregation function.

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

[Original article](https://clickhouse.tech/docs/en/data_types/nested_data_structures/simpleaggregatefunction/) <!--hide-->
