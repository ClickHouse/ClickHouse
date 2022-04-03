---
sidebar_position: 209
---

# quantileBFloat16 {#quantilebfloat16}

Computes an approximate [quantile](https://en.wikipedia.org/wiki/Quantile) of a sample consisting of [bfloat16](https://en.wikipedia.org/wiki/Bfloat16_floating-point_format) numbers. `bfloat16` is a floating-point data type with 1 sign bit, 8 exponent bits and 7 fraction bits.
The function converts input values to 32-bit floats and takes the most significant 16 bits. Then it calculates `bfloat16` quantile value and converts the result to a 64-bit float by appending zero bits.
The function is a fast quantile estimator with a relative error no more than 0.390625%.

**Syntax**

``` sql
quantileBFloat16[(level)](expr)
```

Alias: `medianBFloat16`

**Arguments**

-   `expr` — Column with numeric data. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md).

**Parameters**

-   `level` — Level of quantile. Optional. Possible values are in the range from 0 to 1. Default value: 0.5. [Float](../../../sql-reference/data-types/float.md).

**Returned value**

-   Approximate quantile of the specified level.

Type: [Float64](../../../sql-reference/data-types/float.md#float32-float64).

**Example**

Input table has an integer and a float columns:

``` text
┌─a─┬─────b─┐
│ 1 │ 1.001 │
│ 2 │ 1.002 │
│ 3 │ 1.003 │
│ 4 │ 1.004 │
└───┴───────┘
```

Query to calculate 0.75-quantile (third quartile):

``` sql
SELECT quantileBFloat16(0.75)(a), quantileBFloat16(0.75)(b) FROM example_table;
```

Result:

``` text
┌─quantileBFloat16(0.75)(a)─┬─quantileBFloat16(0.75)(b)─┐
│                         3 │                         1 │
└───────────────────────────┴───────────────────────────┘
```
Note that all floating point values in the example are truncated to 1.0 when converting to `bfloat16`.

# quantileBFloat16Weighted {#quantilebfloat16weighted}

Like `quantileBFloat16` but takes into account the weight of each sequence member.

**See Also**

-   [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
-   [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
