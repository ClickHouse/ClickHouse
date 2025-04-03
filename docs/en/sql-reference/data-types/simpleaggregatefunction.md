---
description: 'Documentation for the SimpleAggregateFunction data type'
sidebar_label: 'SimpleAggregateFunction'
sidebar_position: 48
slug: /sql-reference/data-types/simpleaggregatefunction
title: 'SimpleAggregateFunction Type'
---

# SimpleAggregateFunction Type

## Description {#description}

The `SimpleAggregateFunction` data type stores the intermediate state of an 
aggregate function, but not its full state as the [`AggregateFunction`](../../sql-reference/data-types/aggregatefunction.md) 
type does.

This optimization can be applied to functions for which the following property 
holds: 

> the result of applying a function `f` to a row set `S1 UNION ALL S2` can 
be obtained by applying `f` to parts of the row set separately, and then again 
applying `f` to the results: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`.

This property guarantees that partial aggregation results are enough to compute
the combined one, so we do not have to store and process any extra data. For
example, the result of the `min` or `max` functions require no extra steps to
calculate the final result from the intermediate steps, whereas the `avg` function
requires keeping track of a sum and a count, which will be divided to get the
average in a final `Merge` step which combines the intermediate states.

Aggregate function values are commonly produced by calling an aggregate function
with the [`-SimpleState`](/sql-reference/aggregate-functions/combinators#-simplestate) combinator appended to the function name.

## Syntax {#syntax}

```sql
SimpleAggregateFunction(aggregate_function_name, types_of_arguments...)
```

**Parameters**

- `aggregate_function_name` - The name of an aggregate function.
- `Type` - Types of the aggregate function arguments.

## Supported functions {#supported-functions}

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
Values of the `SimpleAggregateFunction(func, Type)` have the same `Type`, 
so unlike with the `AggregateFunction` type there is no need to apply 
`-Merge`/`-State` combinators.

The `SimpleAggregateFunction` type has better performance than the `AggregateFunction`
for the same aggregate functions.
:::

## Example {#example}

```sql
CREATE TABLE simple (id UInt64, val SimpleAggregateFunction(sum, Double)) ENGINE=AggregatingMergeTree ORDER BY id;
```
## Related Content {#related-content}

- Blog: [Using Aggregate Combinators in ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)    - Blog: [Using Aggregate Combinators in ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
- [AggregateFunction](/sql-reference/data-types/aggregatefunction) type.