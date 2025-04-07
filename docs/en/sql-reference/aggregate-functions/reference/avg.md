---
slug: /en/sql-reference/aggregate-functions/reference/avg
sidebar_position: 112
---

# avg

Calculates the arithmetic mean.

**Syntax**

``` sql
avg(x)
```

**Arguments**

- `x` — input values, must be [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md), or [Decimal](../../../sql-reference/data-types/decimal.md).

**Returned value**

- The arithmetic mean, always as [Float64](../../../sql-reference/data-types/float.md).
- `NaN` if the input parameter `x` is empty.

**Example**

Query:

``` sql
SELECT avg(x) FROM values('x Int8', 0, 1, 2, 3, 4, 5);
```

Result:

``` text
┌─avg(x)─┐
│    2.5 │
└────────┘
```

**Example**

Create a temp table:

Query:

``` sql
CREATE table test (t UInt8) ENGINE = Memory;
```

Get the arithmetic mean:

Query:

```
SELECT avg(t) FROM test;
```

Result:

``` text
┌─avg(x)─┐
│    nan │
└────────┘
```
