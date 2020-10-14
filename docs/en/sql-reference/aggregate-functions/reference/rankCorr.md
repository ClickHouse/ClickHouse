## rankCorr {#agg_function-rankcorr}

The aggregate function which computes a rank correlation coefficient.

**Syntax**

``` sql
rankCorr(x, y)
```

**Parameters**

-   `x` — Floating-point number. [Float64](../../../sql-reference/data-types/float.md#float32-float64-float32-float64).
-   `y` — Floating-point number. [Float64](../../../sql-reference/data-types/float.md#float32-float64-float32-float64).

**Returned value(s)**

-   Returns a floating-point number.

Type: [Float64](../../../sql-reference/data-types/float.md).

**Example**

Query:

``` sql
SELECT rankCorr(number, number) FROM numbers(100);
```

Result:

``` text
┌─rankCorr(number, number)─┐
│                        1 │
└──────────────────────────┘
```

``` sql
SELECT roundBankers(rankCorr(exp(number), sin(number)), 3) FROM numbers(100);
```

Result:

``` text
┌─roundBankers(rankCorr(exp(number), sin(number)), 3)─┐
│                                              -0.037 │
└─────────────────────────────────────────────────────┘
```
**See Also**

-   [Spearman's rank correlation coefficient](https://en.wikipedia.org/wiki/Spearman%27s_rank_correlation_coefficient)