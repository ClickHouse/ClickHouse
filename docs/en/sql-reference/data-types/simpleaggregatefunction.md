---
slug: /sql-reference/data-types/simpleaggregatefunction
sidebar_position: 48
sidebar_label: SimpleAggregateFunction
---
# SimpleAggregateFunction

`SimpleAggregateFunction(name, types_of_arguments...)` data type stores current value (intermediate state) of the aggregate function, but not its full state as [`AggregateFunction`](../../sql-reference/data-types/aggregatefunction.md) does.
This optimization can be applied to functions for which the following property holds: the result of applying a function `f` to a row set `S1 UNION ALL S2` can be obtained by applying `f` to parts of the row set separately, and then again applying `f` to the results: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`.
This property guarantees that partial aggregation results are enough to compute the combined one, so we do not have to store and process any extra data.

The common way to produce an aggregate function value is by calling the aggregate function with the [-SimpleState](/sql-reference/aggregate-functions/combinators#-simplestate) suffix.

The following aggregate functions are supported:

- [`any`](/sql-reference/aggregate-functions/reference/any)
- [`anyLast`](/sql-reference/aggregate-functions/reference/anylast)
- [`min`](/sql-reference/aggregate-functions/reference/min)
- [`max`](/sql-reference/aggregate-functions/reference/max)
- [`sum`](/sql-reference/aggregate-functions/reference/sum)
- [`sumWithOverflow`](/sql-reference/aggregate-functions/reference/sumwithoverflow)
- [`groupBitAnd`](/sql-reference/aggregate-functions/reference/groupbitand)
- [`groupBitOr`](/sql-reference/aggregate-functions/reference/groupbitor)
- [`groupBitXor`](/sql-reference/aggregate-functions/reference/groupbitxor)
- [`groupArrayArray`](/sql-reference/aggregate-functions/reference/grouparray)
- [`groupUniqArrayArray`](../../sql-reference/aggregate-functions/reference/groupuniqarray.md)
- [`groupUniqArrayArrayMap`](../../sql-reference/aggregate-functions/combinators#-map)
- [`sumMap`](/sql-reference/aggregate-functions/reference/summap)
- [`minMap`](/sql-reference/aggregate-functions/reference/minmap)
- [`maxMap`](/sql-reference/aggregate-functions/reference/maxmap)


:::note
Values of the `SimpleAggregateFunction(func, Type)` look and stored the same way as `Type`, so you do not need to apply functions with `-Merge`/`-State` suffixes.

`SimpleAggregateFunction` has better performance than `AggregateFunction` with same aggregation function.
:::

**Parameters**

- Name of the aggregate function.
- Types of the aggregate function arguments.

**Example**

``` sql
CREATE TABLE simple (id UInt64, val SimpleAggregateFunction(sum, Double)) ENGINE=AggregatingMergeTree ORDER BY id;
```
