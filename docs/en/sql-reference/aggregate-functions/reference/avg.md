---
toc_priority: 5
---

# avg {#agg_function-avg}

Calculates the arithmetic mean.

**Syntax**

``` sql
avgWeighted(x)
```

**Parameter**

-   `x` — Values.

`x` must be
[Integer](../../../sql-reference/data-types/int-uint.md),
[floating-point](../../../sql-reference/data-types/float.md), or 
[Decimal](../../../sql-reference/data-types/decimal.md).

**Returned value**

- `NaN` if the supplied parameter is empty.
- Mean otherwise.

**Return type** is always [Float64](../../../sql-reference/data-types/float.md).

**Example**

Query:

``` sql
SELECT avg(x) FROM values('x Int8', 0, 1, 2, 3, 4, 5)
```

Result:

``` text
┌─avg(x)─┐
│    2.5 │
└────────┘
```

**Example**

Query:

``` sql
CREATE table test (t UInt8) ENGINE = Memory;
SELECT avg(t) FROM test
```

Result:

``` text
┌─avg(x)─┐
│    nan │
└────────┘
```
