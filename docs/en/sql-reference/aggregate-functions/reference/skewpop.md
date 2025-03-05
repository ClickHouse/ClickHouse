---
slug: /sql-reference/aggregate-functions/reference/skewpop
sidebar_position: 185
title: "skewPop"
description: "Computes the skewness of a sequence."
---

# skewPop

Computes the [skewness](https://en.wikipedia.org/wiki/Skewness) of a sequence.

``` sql
skewPop(expr)
```

**Arguments**

`expr` — [Expression](../../../sql-reference/syntax.md#syntax-expressions) returning a number.

**Returned value**

The skewness of the given distribution. Type — [Float64](../../../sql-reference/data-types/float.md)

**Example**

``` sql
SELECT skewPop(value) FROM series_with_value_column;
```

## Combinators

The following combinators can be applied to the `skewPop` function:

### skewPopIf
Calculates the population skewness only for rows that match the given condition.

### skewPopArray
Calculates the population skewness of elements in the array.

### skewPopMap
Calculates the population skewness for each key in the map separately.

### skewPopSimpleState
Returns the skewness value with SimpleAggregateFunction type.

### skewPopState
Returns the intermediate state of skewness calculation.

### skewPopMerge
Combines intermediate skewness states to get the final result.

### skewPopMergeState
Combines intermediate skewness states but returns an intermediate state.

### skewPopForEach
Calculates the population skewness for corresponding elements in multiple arrays.

### skewPopDistinct
Calculates the population skewness of distinct values only.

### skewPopOrDefault
Returns 0 if there are no rows to calculate skewness.

### skewPopOrNull
Returns NULL if there are no rows to calculate skewness.
