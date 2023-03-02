---
sidebar_position: 153
---

# kurtPop

Computes the [kurtosis](https://en.wikipedia.org/wiki/Kurtosis) of a sequence.

``` sql
kurtPop(expr)
```

**Arguments**

`expr` — [Expression](../../../sql-reference/syntax.md#syntax-expressions) returning a number.

**Returned value**

The kurtosis of the given distribution. Type — [Float64](../../../sql-reference/data-types/float.md)

**Example**

``` sql
SELECT kurtPop(value) FROM series_with_value_column;
```
