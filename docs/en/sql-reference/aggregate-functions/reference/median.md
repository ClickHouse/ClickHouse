---
slug: /en/sql-reference/aggregate-functions/reference/median
sidebar_position: 167
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
