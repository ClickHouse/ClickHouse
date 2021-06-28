---
toc_priority: 212
---

# median {#median}

The `median*` functions are the aliases for the corresponding `quantile*` functions. They calculate median of a numeric data sample.

Functions:

-   `median` — Alias for [quantile](quantile.md).
-   `medianDeterministic` — Alias for [quantileDeterministic](quantiledeterministic.md).
-   `medianExact` — Alias for [quantileExact](quantileexact.md).
-   `medianExactWeighted` — Alias for [quantileExactWeighted](quantileexactweighted.md).
-   `medianTiming` — Alias for [quantileTiming](quantiletiming.md).
-   `medianTimingWeighted` — Alias for [quantileTimingWeighted](quantiletimingweighted.md).
-   `medianTDigest` — Alias for [quantileTDigest](quantiletdigest.md).
-   `medianTDigestWeighted` — Alias for [quantileTDigestWeighted](quantiletdigestweighted.md).
-   `medianBFloat16` — Alias for [quantileBFloat16](quantilebfloat16.md).

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
SELECT medianDeterministic(val, 1) FROM t
```

Result:

``` text
┌─medianDeterministic(val, 1)─┐
│                         1.5 │
└─────────────────────────────┘
```
