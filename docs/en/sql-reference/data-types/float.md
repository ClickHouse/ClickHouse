---
slug: /en/sql-reference/data-types/float
sidebar_position: 41
sidebar_label: Float32, Float64
---

# Float32, Float64

:::note
If you need accurate calculations, in particular if you work with financial or business data requiring a high precision you should consider using Decimal instead. Floats might lead to inaccurate results as illustrated below:

```
CREATE TABLE IF NOT EXISTS float_vs_decimal
(
   my_float Float64,
   my_decimal Decimal64(3)
)Engine=MergeTree ORDER BY tuple()

INSERT INTO float_vs_decimal SELECT round(canonicalRand(), 3) AS res, res FROM system.numbers LIMIT 1000000; # Generate 1 000 000 random number with 2 decimal places and store them as a float and as a decimal

SELECT sum(my_float), sum(my_decimal) FROM float_vs_decimal;
> 500279.56300000014	500279.563

SELECT sumKahan(my_float), sumKahan(my_decimal) FROM float_vs_decimal;
> 500279.563	500279.563
```
:::

[Floating point numbers](https://en.wikipedia.org/wiki/IEEE_754).

Types are equivalent to types of C:

- `Float32` — `float`.
- `Float64` — `double`.

Aliases:

- `Float32` — `FLOAT`.
- `Float64` — `DOUBLE`.

When creating tables, numeric parameters for floating point numbers can be set (e.g. `FLOAT(12)`, `FLOAT(15, 22)`, `DOUBLE(12)`, `DOUBLE(4, 18)`), but ClickHouse ignores them.

## Using Floating-point Numbers

- Computations with floating-point numbers might produce a rounding error.

<!-- -->

``` sql
SELECT 1 - 0.9
```

``` text
┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

- The result of the calculation depends on the calculation method (the processor type and architecture of the computer system).
- Floating-point calculations might result in numbers such as infinity (`Inf`) and “not-a-number” (`NaN`). This should be taken into account when processing the results of calculations.
- When parsing floating-point numbers from text, the result might not be the nearest machine-representable number.

## NaN and Inf

In contrast to standard SQL, ClickHouse supports the following categories of floating-point numbers:

- `Inf` – Infinity.

<!-- -->

``` sql
SELECT 0.5 / 0
```

``` text
┌─divide(0.5, 0)─┐
│            inf │
└────────────────┘
```

- `-Inf` — Negative infinity.

<!-- -->

``` sql
SELECT -0.5 / 0
```

``` text
┌─divide(-0.5, 0)─┐
│            -inf │
└─────────────────┘
```

- `NaN` — Not a number.

<!-- -->

``` sql
SELECT 0 / 0
```

``` text
┌─divide(0, 0)─┐
│          nan │
└──────────────┘
```

See the rules for `NaN` sorting in the section [ORDER BY clause](../../sql-reference/statements/select/order-by.md).
