---
description: 'Documentation for the SimpleAggregateFunction data type'
sidebar_label: 'SimpleAggregateFunction'
sidebar_position: 48
slug: /sql-reference/data-types/simpleaggregatefunction
title: 'SimpleAggregateFunction Type'
doc_type: 'reference'
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

- [`any`](/sql-reference/aggregate-functions/reference/any.md)
- [`any_respect_nulls`](/sql-reference/aggregate-functions/reference/any.md)
- [`anyLast`](/sql-reference/aggregate-functions/reference/anyLast.md)
- [`anyLast_respect_nulls`](/sql-reference/aggregate-functions/reference/anyLast.md)
- [`min`](/sql-reference/aggregate-functions/reference/min.md)
- [`max`](/sql-reference/aggregate-functions/reference/max.md)
- [`sum`](/sql-reference/aggregate-functions/reference/sum.md)
- [`sumWithOverflow`](/sql-reference/aggregate-functions/reference/sumWithOverflow.md)
- [`groupBitAnd`](/sql-reference/aggregate-functions/reference/groupBitAnd.md)
- [`groupBitOr`](/sql-reference/aggregate-functions/reference/groupBitOr.md)
- [`groupBitXor`](/sql-reference/aggregate-functions/reference/groupBitXor.md)
- [`groupArrayArray`](/sql-reference/aggregate-functions/reference/groupArrayArray.md)
- [`groupUniqArrayArray`](../../sql-reference/aggregate-functions/reference/groupUniqArray.md)
- [`groupUniqArrayArrayMap`](../../sql-reference/aggregate-functions/combinators#-map)
- [`sumMap`](/sql-reference/aggregate-functions/reference/sumMap.md)
- [`minMap`](/sql-reference/aggregate-functions/reference/minMap.md)
- [`maxMap`](/sql-reference/aggregate-functions/reference/maxMap.md)

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