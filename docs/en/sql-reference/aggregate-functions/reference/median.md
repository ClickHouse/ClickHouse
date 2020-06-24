# median {#median}

The `median*` functions are the aliases for the corresponding `quantile*` functions. They calculate median of a numeric data sample.

Functions:

-   `median` — Alias for [quantile](#quantile).
-   `medianDeterministic` — Alias for [quantileDeterministic](#quantiledeterministic).
-   `medianExact` — Alias for [quantileExact](#quantileexact).
-   `medianExactWeighted` — Alias for [quantileExactWeighted](#quantileexactweighted).
-   `medianTiming` — Alias for [quantileTiming](#quantiletiming).
-   `medianTimingWeighted` — Alias for [quantileTimingWeighted](#quantiletimingweighted).
-   `medianTDigest` — Alias for [quantileTDigest](#quantiletdigest).
-   `medianTDigestWeighted` — Alias for [quantileTDigestWeighted](#quantiletdigestweighted).

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
