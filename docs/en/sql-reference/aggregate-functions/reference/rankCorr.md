---
sidebar_position: 145
---

# rankCorr

Computes a rank correlation coefficient.

**Syntax**

``` sql
rankCorr(x, y)
```

**Arguments**

-   `x` — Arbitrary value. [Float32](../../../sql-reference/data-types/float.md#float32-float64) or [Float64](../../../sql-reference/data-types/float.md#float32-float64).
-   `y` — Arbitrary value. [Float32](../../../sql-reference/data-types/float.md#float32-float64) or [Float64](../../../sql-reference/data-types/float.md#float32-float64).

**Returned value(s)**

-   Returns a rank correlation coefficient of the ranks of x and y. The value of the correlation coefficient ranges from -1 to +1. If less than two arguments are passed, the function will return an exception. The value close to +1 denotes a high linear relationship, and with an increase of one random variable, the second random variable also increases. The value close to -1 denotes a high linear relationship, and with an increase of one random variable, the second random variable decreases. The value close or equal to 0 denotes no relationship between the two random variables.

Type: [Float64](../../../sql-reference/data-types/float.md#float32-float64).

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

Query:

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