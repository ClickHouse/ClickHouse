---
slug: /sql-reference/aggregate-functions/reference/kurtpop
sidebar_position: 157
title: "kurtPop"
description: "Computes the kurtosis of a sequence."
---

# kurtPop

Computes the [kurtosis](https://en.wikipedia.org/wiki/Kurtosis) of a sequence.

``` sql
kurtPop(expr)
```

**Arguments**

`expr` — [Expression](/sql-reference/syntax#expressions) returning a number.

**Returned value**

The kurtosis of the given distribution. Type — [Float64](../../../sql-reference/data-types/float.md)

**Example**

``` sql
SELECT kurtPop(value) FROM series_with_value_column;
```

## Combinators

The following combinators can be applied to the `kurtPop` function:

### kurtPopIf
Calculates the population kurtosis only for rows that match the given condition.

### kurtPopArray
Calculates the population kurtosis of elements in the array.

### kurtPopMap
Calculates the population kurtosis for each key in the map separately.

### kurtPopSimpleState
Returns the kurtosis value with SimpleAggregateFunction type.

### kurtPopState
Returns the intermediate state of kurtosis calculation.

### kurtPopMerge
Combines intermediate kurtosis states to get the final result.

### kurtPopMergeState
Combines intermediate kurtosis states but returns an intermediate state.

### kurtPopForEach
Calculates the population kurtosis for corresponding elements in multiple arrays.

### kurtPopDistinct
Calculates the population kurtosis of distinct values only.

### kurtPopOrDefault
Returns 0 if there are no rows to calculate kurtosis.

### kurtPopOrNull
Returns NULL if there are no rows to calculate kurtosis.
