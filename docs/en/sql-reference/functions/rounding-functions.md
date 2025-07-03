---
description: 'Documentation for Rounding Functions'
sidebar_label: 'Rounding'
sidebar_position: 155
slug: /sql-reference/functions/rounding-functions
title: 'Rounding Functions'
---

# Rounding Functions

## floor {#floor}

Returns the largest rounded number less than or equal `x`.
A rounded number is a multiple of 1 / 10 * N, or the nearest number of the appropriate data type if 1 / 10 * N isn't exact.

Integer arguments may be rounded with negative `N` argument, with non-negative `N` the function returns `x`, i.e. does nothing.

If rounding causes an overflow (for example, `floor(-128, -1)`), the result is undefined.

**Syntax**

```sql
floor(x[, N])
```

**Parameters**

- `x` - The value to round. [Float*](../data-types/float.md), [Decimal*](../data-types/decimal.md), or [(U)Int*](../data-types/int-uint.md).
- `N` . [(U)Int*](../data-types/int-uint.md). The default is zero, which means rounding to an integer. Can be negative.

**Returned value**

A rounded number of the same type as `x`.

**Examples**

Query:

```sql
SELECT floor(123.45, 1) AS rounded
```

Result:

```response
┌─rounded─┐
│   123.4 │
└─────────┘
```

Query:

```sql
SELECT floor(123.45, -1)
```

Result:

```response
┌─rounded─┐
│     120 │
└─────────┘
```

## ceiling {#ceiling}

Like `floor` but returns the smallest rounded number greater than or equal `x`.

**Syntax**

```sql
ceiling(x[, N])
```

Alias: `ceil`

## truncate {#truncate}

Like `floor` but returns the rounded number with largest absolute value that has an absolute value less than or equal to `x`'s.

**Syntax**

```sql
truncate(x[, N])
```

Alias: `trunc`.

**Example**

Query:

```sql
SELECT truncate(123.499, 1) AS res;
```

```response
┌───res─┐
│ 123.4 │
└───────┘
```

## round {#round}

Rounds a value to a specified number of decimal places.

The function returns the nearest number of the specified order.
If the input value has equal distance to two neighboring numbers, the function uses banker's rounding for [Float*](../data-types/float.md) inputs and rounds away from zero for the other number types ([Decimal*](../data-types/decimal.md).

**Syntax**

```sql
round(x[, N])
```

**Arguments**

- `x` — A number to round. [Float*](../data-types/float.md), [Decimal*](../data-types/decimal.md), or [(U)Int*](../data-types/int-uint.md).
- `N` — The number of decimal places to round to. Integer. Defaults to `0`.
    - If `N > 0`, the function rounds to the right of the decimal point.
    - If `N < 0`, the function rounds to the left of the decimal point.
    - If `N = 0`, the function rounds to the next integer.

**Returned value:**

A rounded number of the same type as `x`.

**Examples**

Example with `Float` inputs:

```sql
SELECT number / 2 AS x, round(x) FROM system.numbers LIMIT 3;
```

```response
┌───x─┬─round(divide(number, 2))─┐
│   0 │                        0 │
│ 0.5 │                        0 │
│   1 │                        1 │
└─────┴──────────────────────────┘
```

Example with `Decimal` inputs:

```sql
SELECT cast(number / 2 AS  Decimal(10,4)) AS x, round(x) FROM system.numbers LIMIT 3;
```

```sql
┌───x─┬─round(CAST(divide(number, 2), 'Decimal(10, 4)'))─┐
│   0 │                                                0 │
│ 0.5 │                                                1 │
│   1 │                                                1 │
└─────┴──────────────────────────────────────────────────┘
```

To retain trailing zeros, enable setting `output_format_decimal_trailing_zeros`:

```sql
SELECT cast(number / 2 AS  Decimal(10,4)) AS x, round(x) FROM system.numbers LIMIT 3 settings output_format_decimal_trailing_zeros=1;

```

```sql
┌──────x─┬─round(CAST(divide(number, 2), 'Decimal(10, 4)'))─┐
│ 0.0000 │                                           0.0000 │
│ 0.5000 │                                           1.0000 │
│ 1.0000 │                                           1.0000 │
└────────┴──────────────────────────────────────────────────┘
```

Examples of rounding to the nearest number:

```text
round(3.2, 0) = 3
round(4.1267, 2) = 4.13
round(22,-1) = 20
round(467,-2) = 500
round(-467,-2) = -500
```

Banker's rounding.

```text
round(3.5) = 4
round(4.5) = 4
round(3.55, 1) = 3.6
round(3.65, 1) = 3.6
```

**See Also**

- [roundBankers](#roundbankers)

## roundBankers {#roundbankers}

Rounds a number to a specified decimal position.

If the rounding number is halfway between two numbers, the function uses banker's rounding.
Banker's rounding is a method of rounding fractional numbers
When the rounding number is halfway between two numbers, it's rounded to the nearest even digit at the specified decimal position.
For example: 3.5 rounds up to 4, 2.5 rounds down to 2.
It's the default rounding method for floating point numbers defined in [IEEE 754](https://en.wikipedia.org/wiki/IEEE_754#Roundings_to_nearest).
The [round](#round) function performs the same rounding for floating point numbers.
The `roundBankers` function also rounds integers the same way, for example, `roundBankers(45, -1) = 40`.

In other cases, the function rounds numbers to the nearest integer.

Using banker's rounding, you can reduce the effect that rounding numbers has on the results of summing or subtracting these numbers.

For example, sum numbers 1.5, 2.5, 3.5, 4.5 with different rounding:

- No rounding: 1.5 + 2.5 + 3.5 + 4.5 = 12.
- Banker's rounding: 2 + 2 + 4 + 4 = 12.
- Rounding to the nearest integer: 2 + 3 + 4 + 5 = 14.

**Syntax**

```sql
roundBankers(x [, N])
```

**Arguments**

    - `N > 0` — The function rounds the number to the given position right of the decimal point. Example: `roundBankers(3.55, 1) = 3.6`.
    - `N < 0` — The function rounds the number to the given position left of the decimal point. Example: `roundBankers(24.55, -1) = 20`.
    - `N = 0` — The function rounds the number to an integer. In this case the argument can be omitted. Example: `roundBankers(2.5) = 2`.

- `x` — A number to round. [Float*](../data-types/float.md), [Decimal*](../data-types/decimal.md), or [(U)Int*](../data-types/int-uint.md).
- `N` — The number of decimal places to round to. Integer. Defaults to `0`.
    - If `N > 0`, the function rounds to the right of the decimal point.
    - If `N < 0`, the function rounds to the left of the decimal point.
    - If `N = 0`, the function rounds to the next integer.

**Returned value**

A value rounded by the banker's rounding method.

**Examples**

Query:

```sql
 SELECT number / 2 AS x, roundBankers(x, 0) AS b FROM system.numbers LIMIT 10
```

Result:

```response
┌───x─┬─b─┐
│   0 │ 0 │
│ 0.5 │ 0 │
│   1 │ 1 │
│ 1.5 │ 2 │
│   2 │ 2 │
│ 2.5 │ 2 │
│   3 │ 3 │
│ 3.5 │ 4 │
│   4 │ 4 │
│ 4.5 │ 4 │
└─────┴───┘
```

Examples of Banker's rounding:

```response
roundBankers(0.4) = 0
roundBankers(-3.5) = -4
roundBankers(4.5) = 4
roundBankers(3.55, 1) = 3.6
roundBankers(3.65, 1) = 3.6
roundBankers(10.35, 1) = 10.4
roundBankers(10.755, 2) = 10.76
```

**See Also**

- [round](#round)

## roundToExp2 {#roundtoexp2}

Accepts a number. If the number is less than one, it returns `0`. Otherwise, it rounds the number down to the nearest (whole non-negative) degree of two.

**Syntax**

```sql
roundToExp2(num)
```

**Parameters**

- `num`: A number to round. [UInt](../data-types/int-uint.md)/[Float](../data-types/float.md).

**Returned value**

- `0`, for `num` $\lt 1$. [UInt8](../data-types/int-uint.md).
- `num` rounded down to the nearest (whole non-negative) degree of two. [UInt](../data-types/int-uint.md)/[Float](../data-types/float.md) equivalent to the input type.

**Example**

Query:

```sql
SELECT *, roundToExp2(*) FROM system.numbers WHERE number IN (0, 2, 5, 10, 19, 50)
```

Result:

```response
┌─number─┬─roundToExp2(number)─┐
│      0 │                   0 │
│      2 │                   2 │
│      5 │                   4 │
│     10 │                   8 │
│     19 │                  16 │
│     50 │                  32 │
└────────┴─────────────────────┘
```

## roundDuration {#roundduration}

Accepts a number. If the number is less than one, it returns `0`. Otherwise, it rounds the number down to numbers from the set of commonly used durations: `1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000`.

**Syntax**

```sql
roundDuration(num)
```

**Parameters**

- `num`: A number to round to one of the numbers in the set of common durations. [UInt](../data-types/int-uint.md)/[Float](../data-types/float.md).

**Returned value**

- `0`, for `num` $\lt 1$.
- Otherwise, one of: `1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000`. [UInt16](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT *, roundDuration(*) FROM system.numbers WHERE number IN (0, 9, 19, 47, 101, 149, 205, 271, 421, 789, 1423, 2345, 4567, 9876, 24680, 42573)
```

Result:

```response
┌─number─┬─roundDuration(number)─┐
│      0 │                     0 │
│      9 │                     1 │
│     19 │                    10 │
│     47 │                    30 │
│    101 │                    60 │
│    149 │                   120 │
│    205 │                   180 │
│    271 │                   240 │
│    421 │                   300 │
│    789 │                   600 │
│   1423 │                  1200 │
│   2345 │                  1800 │
│   4567 │                  3600 │
│   9876 │                  7200 │
│  24680 │                 18000 │
│  42573 │                 36000 │
└────────┴───────────────────────┘
```

## roundAge {#roundage}

Accepts a number within various commonly used ranges of human age and returns either a maximum or a minimum within that range.

**Syntax**

```sql
roundAge(num)
```

**Parameters**

- `age`: A number representing an age in years. [UInt](../data-types/int-uint.md)/[Float](../data-types/float.md).

**Returned value**

- Returns `0`, for $age \lt 1$.
- Returns `17`, for $1 \leq age \leq 17$.
- Returns `18`, for $18 \leq age \leq 24$.
- Returns `25`, for $25 \leq age \leq 34$.
- Returns `35`, for $35 \leq age \leq 44$.
- Returns `45`, for $45 \leq age \leq 54$.
- Returns `55`, for $age \geq 55$.

Type: [UInt8](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT *, roundAge(*) FROM system.numbers WHERE number IN (0, 5, 20, 31, 37, 54, 72);
```

Result:

```response
┌─number─┬─roundAge(number)─┐
│      0 │                0 │
│      5 │               17 │
│     20 │               18 │
│     31 │               25 │
│     37 │               35 │
│     54 │               45 │
│     72 │               55 │
└────────┴──────────────────┘
```

## roundDown {#rounddown}

Accepts a number and rounds it down to an element in the specified array. If the value is less than the lowest bound, the lowest bound is returned.

**Syntax**

```sql
roundDown(num, arr)
```

**Parameters**

- `num`: A number to round down. [Numeric](../data-types/int-uint.md).
- `arr`: Array of elements to round `age` down to. [Array](../data-types/array.md) of [UInt](../data-types/int-uint.md)/[Float](../data-types/float.md) type.

**Returned value**

- Number rounded down to an element in `arr`. If the value is less than the lowest bound, the lowest bound is returned. [UInt](../data-types/int-uint.md)/[Float](../data-types/float.md) type deduced from the type of `arr`.

**Example**

Query:

```sql
SELECT *, roundDown(*, [3, 4, 5]) FROM system.numbers WHERE number IN (0, 1, 2, 3, 4, 5)
```

Result:

```response
┌─number─┬─roundDown(number, [3, 4, 5])─┐
│      0 │                            3 │
│      1 │                            3 │
│      2 │                            3 │
│      3 │                            3 │
│      4 │                            4 │
│      5 │                            5 │
└────────┴──────────────────────────────┘
```

<!-- 
The inner content of the tags below are replaced at doc framework build time with 
docs generated from system.functions. Please do not modify or remove the tags.
See: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
-->

<!--AUTOGENERATED_START-->
<!--AUTOGENERATED_END-->
