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

-   `x` — Values.
-   `weight` — Weights of the values.

`x` and `weight` must both be
[Integer](../../../sql-reference/data-types/int-uint.md),
[floating-point](../../../sql-reference/data-types/float.md), or 
[Decimal](../../../sql-reference/data-types/decimal.md),
but may have different types.

**Returned value**

-   `NaN`. If all the weights are equal to 0.
-   Weighted mean otherwise.

**Return type**

- `Decimal` if both types are [Decimal](../../../sql-reference/data-types/decimal.md) (largest type taken).
(depending on the largest type).
- [Float64](../../../sql-reference/data-types/float.md) otherwise.

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

**Example**

Query:

``` sql
SELECT avgWeighted(x, w)
FROM values('x Int8, w Float64', (4, 1), (1, 0), (10, 2))
```

Result:

``` text
┌─avgWeighted(x, weight)─┐
│                      8 │
└────────────────────────┘
```
