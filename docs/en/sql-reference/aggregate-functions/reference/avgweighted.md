---
toc_priority: 107
---

# avgWeighted {#avgweighted}

Calculates the [weighted arithmetic mean](https://en.wikipedia.org/wiki/Weighted_arithmetic_mean).

**Syntax**

``` sql
avgWeighted(x, weight)
```

**Parameters**

-   `x` — Values. [Integer](../../../sql-reference/data-types/int-uint.md) or [floating-point](../../../sql-reference/data-types/float.md).
-   `weight` — Weights of the values. [Integer](../../../sql-reference/data-types/int-uint.md) or [floating-point](../../../sql-reference/data-types/float.md).

Type of `x` and `weight` must be the same.

**Returned value**

-   Weighted mean.
-   `NaN`. If all the weights are equal to 0.

Type: [Float64](../../../sql-reference/data-types/float.md).

**Example**

Query:

``` sql
SELECT avgWeighted(x, w)
FROM values('x Int8, w Int8', (4, 1), (1, 0), (10, 2))
```

Result:

``` text
┌─avgWeighted(x, weight)─┐
│                      8 │
└────────────────────────┘
```
