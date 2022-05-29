---
sidebar_position: 145
---

# sumKahan {#agg_function-sumKahan}

Calculates the sum of the numbers with [Kahan compensated summation algorithm](https://en.wikipedia.org/wiki/Kahan_summation_algorithm)
Slower than [sum](./sum.md) function.
The compensation works only for [Float](../../../sql-reference/data-types/float.md) types.


**Syntax**

``` sql
sumKahan(x)
```

**Arguments**

-   `x` — Input value, must be [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md), or [Decimal](../../../sql-reference/data-types/decimal.md).

**Returned value**

-  the sum of numbers, with type [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md), or [Decimal](../../../sql-reference/data-types/decimal.md) depends on type of input arguments

**Example**

Query:

``` sql
SELECT sum(0.1), sumKahan(0.1) FROM numbers(10);
```

Result:

``` text
┌───────────sum(0.1)─┬─sumKahan(0.1)─┐
│ 0.9999999999999999 │             1 │
└────────────────────┴───────────────┘
```