---
description: 'Documentation for floating-point data types in ClickHouse: Float32,
  Float64, and BFloat16'
sidebar_label: 'Float32 | Float64 | BFloat16'
sidebar_position: 4
slug: /sql-reference/data-types/float
title: 'Float32 | Float64 | BFloat16 Types'
doc_type: 'reference'
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

## Using floating-point numbers {#using-floating-point-numbers}

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

## NaN values in set semantics {#nan-values-in-set-semantics}

The IEEE 754 standard defines `NaN` such that the scalar comparison `NaN = NaN` returns `false`.
ClickHouse follows that rule for the `=` operator.

However, `NaN` is not a single value; it is any bit pattern whose exponent is all ones and whose
mantissa is non-zero. Different operations and different CPU architectures can produce `NaN`
values with different sign bits or different mantissa payloads. For example:

- `0./0.` produces a `NaN` whose sign bit is 1 on most x86 platforms.
- The literal `nan` produces a `NaN` whose sign bit is 0.
- After [PR #98230](https://github.com/ClickHouse/ClickHouse/pull/98230), the AArch64 NEON path of
  `log` returns a `NaN` whose sign bit differs from glibc's scalar `log` on negative inputs.

Hash tables in ClickHouse compare keys byte-wise, so different `NaN` bit patterns hash to
different buckets and are treated as distinct values by set-semantics operations including
`DISTINCT`, `GROUP BY`, `uniqExact`, `countDistinct`, and equi-`JOIN` on a `Float` key:

```sql
SELECT countDistinct(arrayJoin([0./0., nan, log(-1.)]));
-- May return 2 or 3 depending on architecture and build, even though all three inputs are NaN.
```

This is consistent with IEEE 754 (every `NaN` is unequal to every other value, including itself)
but can be surprising. If you need set-semantics operations to treat all `NaN` values as equal,
canonicalize them in the query:

```sql
-- Replace every NaN with a single canonical NaN value
SELECT countDistinct(if(isNaN(x), CAST('nan' AS Float64), x))
FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x);
-- Returns 1.

-- Or exclude NaN values from the set entirely
SELECT countDistinct(if(isNaN(x), NULL, x))
FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x);
-- Returns 0.
```

The same approach works for `DISTINCT`, `GROUP BY`, and `JOIN` keys.

## BFloat16 {#bfloat16}

`BFloat16` is a 16-bit floating point data type with 8-bit exponent, sign, and 7-bit mantissa. 
It is useful for machine learning and AI applications.

ClickHouse supports conversions between `Float32` and `BFloat16` which 
can be done using the [`toFloat32()`](../functions/type-conversion-functions.md/#toFloat32) or [`toBFloat16`](../functions/type-conversion-functions.md/#toBFloat16) functions.

:::note
Most other operations are not supported.
:::
