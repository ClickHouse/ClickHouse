---
slug: /sql-reference/aggregate-functions/reference/quantile
sidebar_position: 170
title: "quantile"
description: "Computes an approximate quantile of a numeric data sequence."
---

# quantile

Computes an approximate [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

This function applies [reservoir sampling](https://en.wikipedia.org/wiki/Reservoir_sampling) with a reservoir size up to 8192 and a random number generator for sampling. The result is non-deterministic. To get an exact quantile, use the [quantileExact](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexact) function.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) function.

Note that for an empty numeric sequence, `quantile` will return NaN, but its `quantile*` variants will return either NaN or a default value for the sequence type, depending on the variant.

**Syntax**

``` sql
quantile(level)(expr)
```

Alias: `median`.

**Arguments**

- `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).
- `expr` — Expression over the column values resulting in numeric [data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) or [DateTime](../../../sql-reference/data-types/datetime.md).

**Returned value**

- Approximate quantile of the specified level.

Type:

- [Float64](../../../sql-reference/data-types/float.md) for numeric data type input.
- [Date](../../../sql-reference/data-types/date.md) if input values have the `Date` type.
- [DateTime](../../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Input table:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

Query:

``` sql
SELECT quantile(val) FROM t
```

Result:

``` text
┌─quantile(val)─┐
│           1.5 │
└───────────────┘
```

**See Also**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)

## Combinators

The following combinators can be applied to the `quantile` function:

### quantileIf
Calculates the approximate quantile only for rows that match the given condition.

### quantileArray
Calculates the approximate quantile of elements in the array.

### quantileMap
Calculates the approximate quantile for each key in the map separately.

### quantileSimpleState
Returns the approximate quantile value with SimpleAggregateFunction type.

### quantileState
Returns the intermediate state of quantile calculation.

### quantileMerge
Combines intermediate quantile states to get the final quantile.

### quantileMergeState
Combines intermediate quantile states but returns an intermediate state.

### quantileForEach
Calculates the approximate quantile for corresponding elements in multiple arrays.

### quantileDistinct
Calculates the approximate quantile of distinct values only.

### quantileOrDefault
Returns 0 if there are no rows to calculate quantile.

### quantileOrNull
Returns NULL if there are no rows to calculate quantile.
