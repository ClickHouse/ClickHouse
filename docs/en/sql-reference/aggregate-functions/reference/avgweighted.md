---
sidebar_position: 107
---

# avgWeighted

Calculates the [weighted arithmetic mean](https://en.wikipedia.org/wiki/Weighted_arithmetic_mean).

**Syntax**

``` sql
avgWeighted(x, weight)
```

**Arguments**

-   `x` — Values.
-   `weight` — Weights of the values.

`x` and `weight` must both be
[Integer](../../../sql-reference/data-types/int-uint.md),
[floating-point](../../../sql-reference/data-types/float.md), or
[Decimal](../../../sql-reference/data-types/decimal.md),
but may have different types.

**Returned value**

-   `NaN` if all the weights are equal to 0 or the supplied weights parameter is empty.
-   Weighted mean otherwise.

**Return type** is always [Float64](../../../sql-reference/data-types/float.md).

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

**Example**

Query:

``` sql
SELECT avgWeighted(x, w)
FROM values('x Int8, w Int8', (0, 0), (1, 0), (10, 0))
```

Result:

``` text
┌─avgWeighted(x, weight)─┐
│                    nan │
└────────────────────────┘
```

**Example**

Query:

``` sql
CREATE table test (t UInt8) ENGINE = Memory;
SELECT avgWeighted(t) FROM test
```

Result:

``` text
┌─avgWeighted(x, weight)─┐
│                    nan │
└────────────────────────┘
```
