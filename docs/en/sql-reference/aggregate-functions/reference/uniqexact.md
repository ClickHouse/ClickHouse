---
slug: /sql-reference/aggregate-functions/reference/uniqexact
sidebar_position: 207
title: "uniqExact"
description: "Calculates the exact number of different argument values."
---

# uniqExact

Calculates the exact number of different argument values.

``` sql
uniqExact(x[, ...])
```

Use the `uniqExact` function if you absolutely need an exact result. Otherwise use the [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) function.

The `uniqExact` function uses more memory than `uniq`, because the size of the state has unbounded growth as the number of different values increases.

**Arguments**

The function takes a variable number of parameters. Parameters can be `Tuple`, `Array`, `Date`, `DateTime`, `String`, or numeric types.

**Example**

In this example we'll use the `uniqExact` function to count the number of unique type codes (a short identifier for the type of aircraft) in the [opensky data set](https://sql.clickhouse.com?query=U0VMRUNUIHVuaXFFeGFjdCh0eXBlY29kZSkgRlJPTSBvcGVuc2t5Lm9wZW5za3k&).

```sql title="Query"
SELECT uniqExact(typecode) FROM opensky.opensky
```

```response title="Response"
1106
```

**See Also**

- [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)
- [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniqcombined)
- [uniqHLL12](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniqhll12)
- [uniqTheta](../../../sql-reference/aggregate-functions/reference/uniqthetasketch.md#agg_function-uniqthetasketch)

## Combinators

The following combinators can be applied to the `uniqExact` function:

### uniqExactIf
Calculates the exact number of different values only from rows that match the given condition.

### uniqExactArray
Calculates the exact number of different elements in the array.

### uniqExactMap
Calculates the exact number of different values for each key in the map separately.

### uniqExactSimpleState
Returns the unique values state with SimpleAggregateFunction type.

### uniqExactState
Returns the intermediate state of unique values calculation.

### uniqExactMerge
Combines intermediate states to get the final count of unique values.

### uniqExactMergeState
Combines intermediate states but returns an intermediate state.

### uniqExactForEach
Calculates exact unique counts for corresponding elements in multiple arrays.

### uniqExactDistinct
Same as uniqExact since it already calculates distinct values.

### uniqExactOrDefault
Returns 0 if there are no rows to calculate unique values.

### uniqExactOrNull
Returns NULL if there are no rows to calculate unique values.
