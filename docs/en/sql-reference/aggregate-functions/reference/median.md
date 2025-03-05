---
slug: /sql-reference/aggregate-functions/reference/median
sidebar_position: 167
title: "median"
description: "The `median*` functions are the aliases for the corresponding `quantile*` functions. They calculate median of a numeric data sample."
---

# median

The `median*` functions are the aliases for the corresponding `quantile*` functions. They calculate median of a numeric data sample.

Functions:

- `median` — Alias for [quantile](../../../sql-reference/aggregate-functions/reference/quantile.md#quantile).
- `medianDeterministic` — Alias for [quantileDeterministic](../../../sql-reference/aggregate-functions/reference/quantiledeterministic.md#quantiledeterministic).
- `medianExact` — Alias for [quantileExact](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexact).
- `medianExactWeighted` — Alias for [quantileExactWeighted](../../../sql-reference/aggregate-functions/reference/quantileexactweighted.md#quantileexactweighted).
- `medianTiming` — Alias for [quantileTiming](../../../sql-reference/aggregate-functions/reference/quantiletiming.md#quantiletiming).
- `medianTimingWeighted` — Alias for [quantileTimingWeighted](../../../sql-reference/aggregate-functions/reference/quantiletimingweighted.md#quantiletimingweighted).
- `medianTDigest` — Alias for [quantileTDigest](../../../sql-reference/aggregate-functions/reference/quantiletdigest.md#quantiletdigest).
- `medianTDigestWeighted` — Alias for [quantileTDigestWeighted](../../../sql-reference/aggregate-functions/reference/quantiletdigestweighted.md#quantiletdigestweighted).
- `medianBFloat16` — Alias for [quantileBFloat16](../../../sql-reference/aggregate-functions/reference/quantilebfloat16.md#quantilebfloat16).
- `medianDD` — Alias for [quantileDD](../../../sql-reference/aggregate-functions/reference/quantileddsketch.md#quantileddsketch).

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
SELECT medianDeterministic(val, 1) FROM t;
```

Result:

``` text
┌─medianDeterministic(val, 1)─┐
│                         1.5 │
└─────────────────────────────┘
```

## Combinators

The following combinators can be applied to the `median` function:

### medianIf
Calculates the median only for rows that match the given condition.

### medianArray
Calculates the median of elements in the array.

### medianMap
Calculates the median for each key in the map separately.

### medianSimpleState
Returns the median value with SimpleAggregateFunction type.

### medianState
Returns the intermediate state of median calculation.

### medianMerge
Combines intermediate median states to get the final median.

### medianMergeState
Combines intermediate median states but returns an intermediate state.

### medianForEach
Calculates the median for corresponding elements in multiple arrays.

### medianDistinct
Calculates the median of distinct values only.

### medianOrDefault
Returns 0 if there are no rows to calculate median.

### medianOrNull
Returns NULL if there are no rows to calculate median.

Note: These combinators can also be applied to all median variants (medianExact, medianTiming, etc.) with their respective prefixes.
