# median {#median}

The `median*` functions are the aliases for the corresponding `quantile*` functions. They calculate median of a numeric data sample.

Functions:

-   `median` — Alias for [quantile](../../../sql-reference/aggregate-functions/reference/quantile#quantile).
-   `medianDeterministic` — Alias for [quantileDeterministic](../../../sql-reference/aggregate-functions/reference/quantiledeterministic#quantiledeterministic).
-   `medianExact` — Alias for [quantileExact](../../../sql-reference/aggregate-functions/reference/quantileexact#quantileexact).
-   `medianExactWeighted` — Alias for [quantileExactWeighted](../../../sql-reference/aggregate-functions/reference/quantileexactweighted#quantileexactweighted).
-   `medianTiming` — Alias for [quantileTiming](../../../sql-reference/aggregate-functions/reference/quantiletiming#quantiletiming).
-   `medianTimingWeighted` — Alias for [quantileTimingWeighted](../../../sql-reference/aggregate-functions/reference/quantiletimingweighted#quantiletimingweighted).
-   `medianTDigest` — Alias for [quantileTDigest](../../../sql-reference/aggregate-functions/reference/quantiletdigest#quantiletdigest).
-   `medianTDigestWeighted` — Alias for [quantileTDigestWeighted](../../../sql-reference/aggregate-functions/reference/quantiletdigestweighted#quantiletdigestweighted).

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
