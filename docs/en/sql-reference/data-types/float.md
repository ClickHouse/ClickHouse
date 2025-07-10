---
description: 'Documentation for floating-point data types in ClickHouse: Float32,
  Float64, and BFloat16'
sidebar_label: 'Float32 | Float64 | BFloat16'
sidebar_position: 4
slug: /sql-reference/data-types/float
title: 'Float32 | Float64 | BFloat16 Types'
---

:::note
If you need accurate calculations, in particular if you work with financial or business data requiring a high precision, you should consider using [Decimal](../data-types/decimal.md) instead. 

[Floating Point Numbers](https://en.wikipedia.org/wiki/IEEE_754) might lead to inaccurate results as illustrated below:

```sql
CREATE TABLE IF NOT EXISTS float_vs_decimal
(
   my_float Float64,
   my_decimal Decimal64(3)
)
ENGINE=MergeTree
ORDER BY tuple();

# Generate 1 000 000 random numbers with 2 decimal places and store them as a float and as a decimal
INSERT INTO float_vs_decimal SELECT round(randCanonical(), 3) AS res, res FROM system.numbers LIMIT 1000000;
```
```sql
SELECT sum(my_float), sum(my_decimal) FROM float_vs_decimal;

┌──────sum(my_float)─┬─sum(my_decimal)─┐
│ 499693.60500000004 │      499693.605 │
└────────────────────┴─────────────────┘

SELECT sumKahan(my_float), sumKahan(my_decimal) FROM float_vs_decimal;

┌─sumKahan(my_float)─┬─sumKahan(my_decimal)─┐
│         499693.605 │           499693.605 │
└────────────────────┴──────────────────────┘
```
:::

The equivalent types in ClickHouse and in C are given below:

- `Float32` — `float`.
- `Float64` — `double`.

Float types in ClickHouse have the following aliases:

- `Float32` — `FLOAT`, `REAL`, `SINGLE`.
- `Float64` — `DOUBLE`, `DOUBLE PRECISION`.

When creating tables, numeric parameters for floating point numbers can be set (e.g. `FLOAT(12)`, `FLOAT(15, 22)`, `DOUBLE(12)`, `DOUBLE(4, 18)`), but ClickHouse ignores them.

## Using Floating-point Numbers {#using-floating-point-numbers}

- Computations with floating-point numbers might produce a rounding error.

<!-- -->

```sql
SELECT 1 - 0.9

┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

- The result of the calculation depends on the calculation method (the processor type and architecture of the computer system).
- Floating-point calculations might result in numbers such as infinity (`Inf`) and "not-a-number" (`NaN`). This should be taken into account when processing the results of calculations.
- When parsing floating-point numbers from text, the result might not be the nearest machine-representable number.

## NaN and Inf {#nan-and-inf}

In contrast to standard SQL, ClickHouse supports the following categories of floating-point numbers:

- `Inf` – Infinity.

<!-- -->

```sql
SELECT 0.5 / 0

┌─divide(0.5, 0)─┐
│            inf │
└────────────────┘
```

- `-Inf` — Negative infinity.

<!-- -->

```sql
SELECT -0.5 / 0

┌─divide(-0.5, 0)─┐
│            -inf │
└─────────────────┘
```

- `NaN` — Not a number.

<!-- -->

```sql
SELECT 0 / 0

┌─divide(0, 0)─┐
│          nan │
└──────────────┘
```

See the rules for `NaN` sorting in the section [ORDER BY clause](../../sql-reference/statements/select/order-by.md).

## BFloat16 {#bfloat16}

`BFloat16` is a 16-bit floating point data type with 8-bit exponent, sign, and 7-bit mantissa. 
It is useful for machine learning and AI applications.

ClickHouse supports conversions between `Float32` and `BFloat16` which 
can be done using the [`toFloat32()`](../functions/type-conversion-functions.md/#tofloat32) or [`toBFloat16`](../functions/type-conversion-functions.md/#tobfloat16) functions.

:::note
Most other operations are not supported.
:::
