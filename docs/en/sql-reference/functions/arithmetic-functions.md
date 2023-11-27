---
slug: /en/sql-reference/functions/arithmetic-functions
sidebar_position: 5
sidebar_label: Arithmetic
---

# Arithmetic Functions

Arithmetic functions work for any two operands of type `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32`, `Int64`, `Float32`, or `Float64`.

Before performing the operation, both operands are casted to the result type. The result type is determined as follows (unless specified
differently in the function documentation below):
- If both operands are up to 32 bits wide, the size of the result type will be the size of the next bigger type following the bigger of the
  two operands (integer size promotion). For example, `UInt8 + UInt16 = UInt32` or `Float32 * Float32 = Float64`.
- If one of the operands has 64 or more bits, the size of the result type will be the same size as the bigger of the two operands. For
  example, `UInt32 + UInt128 = UInt128` or `Float32 * Float64 = Float64`.
- If one of the operands is signed, the result type will also be signed, otherwise it will be signed. For example, `UInt32 * Int32 = Int64`.

These rules make sure that the result type will be the smallest type which can represent all possible results. While this introduces a risk
of overflows around the value range boundary, it ensures that calculations are performed quickly using the maximum native integer width of
64 bit. This behavior also guarantees compatibility with many other databases which provide 64 bit integers (BIGINT) as the biggest integer
type.

Example:

``` sql
SELECT toTypeName(0), toTypeName(0 + 0), toTypeName(0 + 0 + 0), toTypeName(0 + 0 + 0 + 0)
```

``` text
┌─toTypeName(0)─┬─toTypeName(plus(0, 0))─┬─toTypeName(plus(plus(0, 0), 0))─┬─toTypeName(plus(plus(plus(0, 0), 0), 0))─┐
│ UInt8         │ UInt16                 │ UInt32                          │ UInt64                                   │
└───────────────┴────────────────────────┴─────────────────────────────────┴──────────────────────────────────────────┘
```

Overflows are produced the same way as in C++.

## plus

Calculates the sum of two values `a` and `b`.

**Syntax**

```sql
plus(a, b)
```

It is possible to add an integer and a date or date with time. The former operation increments the number of days in the date, the latter operation increments the number of seconds in the date with time.

Alias: `a + b` (operator)

## minus

Calculates the difference of two values `a` and `b`. The result is always signed.

Similar to `plus`, it is possible to subtract an integer from a date or date with time.

**Syntax**

```sql
minus(a, b)
```

Alias: `a - b` (operator)

## multiply

Calculates the product of two values `a` and `b`.

**Syntax**

```sql
multiply(a, b)
```

Alias: `a \* b` (operator)

## divide

Calculates the quotient of two values `a` and `b`. The result type is always [Float64](../../sql-reference/data-types/float.md). Integer division is provided by the `intDiv` function.

Division by 0 returns `inf`, `-inf`, or `nan`.

**Syntax**

```sql
divide(a, b)
```

Alias: `a / b` (operator)

## intDiv

Performs an integer division of two values `a` by `b`, i.e. computes the quotient rounded down to the next smallest integer.

The result has the same width as the dividend (the first parameter).

An exception is thrown when dividing by zero, when the quotient does not fit in the range of the dividend, or when dividing a minimal negative number by minus one.

**Syntax**

```sql
intDiv(a, b)
```

**Example**

Query:

```sql
SELECT
    intDiv(toFloat64(1), 0.001) AS res,
    toTypeName(res)
```

```response
┌──res─┬─toTypeName(intDiv(toFloat64(1), 0.001))─┐
│ 1000 │ Int64                                   │
└──────┴─────────────────────────────────────────┘
```

```sql
SELECT
    intDiv(1, 0.001) AS res,
    toTypeName(res)
```

```response
Received exception from server (version 23.2.1):
Code: 153. DB::Exception: Received from localhost:9000. DB::Exception: Cannot perform integer division, because it will produce infinite or too large number: While processing intDiv(1, 0.001) AS res, toTypeName(res). (ILLEGAL_DIVISION)
```

## intDivOrZero

Same as `intDiv` but returns zero when dividing by zero or when dividing a minimal negative number by minus one.

**Syntax**

```sql
intDivOrZero(a, b)
```

## modulo

Calculates the remainder of the division of two values `a` by `b`.

The result type is an integer if both inputs are integers. If one of the inputs is a floating-point number, the result type is [Float64](../../sql-reference/data-types/float.md).

The remainder is computed like in C++. Truncated division is used for negative numbers.

An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.

**Syntax**

```sql
modulo(a, b)
```

Alias: `a % b` (operator)

## moduloOrZero

Like [modulo](#modulo) but returns zero when the divisor is zero.

**Syntax**

```sql
moduloOrZero(a, b)
```

## positiveModulo(a, b)

Like [modulo](#modulo) but always returns a non-negative number.

This function is 4-5 times slower than `modulo`.

**Syntax**

```sql
positiveModulo(a, b)
```

Alias:
- `positive_modulo(a, b)`
- `pmod(a, b)`

**Example**

Query:

```sql
SELECT positiveModulo(-1, 10)
```

Result:

```result
┌─positiveModulo(-1, 10)─┐
│                      9 │
└────────────────────────┘
```

## negate

Negates a value `a`. The result is always signed.

**Syntax**

```sql
negate(a)
```

Alias: `-a`

## abs

Calculates the absolute value of `a`. Has no effect if `a` is of an unsigned type. If `a` is of a signed type, it returns an unsigned number.

**Syntax**

```sql
abs(a)
```

## gcd

Returns the greatest common divisor of two values `a` and `b`.

An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.

**Syntax**

```sql
gcd(a, b)
```

## lcm(a, b)

Returns the least common multiple of two values `a` and `b`.

An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.

**Syntax**

```sql
lcm(a, b)
```

## max2

Returns the bigger of two values `a` and `b`. The returned value is of type [Float64](../../sql-reference/data-types/float.md).

**Syntax**

```sql
max2(a, b)
```

**Example**

Query:

```sql
SELECT max2(-1, 2);
```

Result:

```result
┌─max2(-1, 2)─┐
│           2 │
└─────────────┘
```

## min2

Returns the smaller of two values `a` and `b`. The returned value is of type [Float64](../../sql-reference/data-types/float.md).

**Syntax**

```sql
min2(a, b)
```

**Example**

Query:

```sql
SELECT min2(-1, 2);
```

Result:

```result
┌─min2(-1, 2)─┐
│          -1 │
└─────────────┘
```

## multiplyDecimal

Multiplies two decimals `a` and `b`. The result value will be of type [Decimal256](../../sql-reference/data-types/decimal.md).

The scale of the result can be explicitly specified by `result_scale`. If `result_scale` is not specified, it is assumed to be the maximum scale of the input values.

This function work significantly slower than usual `multiply`. In case no control over the result precision is needed and/or fast computation is desired, consider using `multiply`.

**Syntax**

```sql
multiplyDecimal(a, b[, result_scale])
```

**Arguments**

- `a` — First value: [Decimal](../../sql-reference/data-types/decimal.md).
- `b` — Second value: [Decimal](../../sql-reference/data-types/decimal.md).
- `result_scale` — Scale of result: [Int/UInt](../../sql-reference/data-types/int-uint.md).

**Returned value**

- The result of multiplication with given scale.

Type: [Decimal256](../../sql-reference/data-types/decimal.md).

**Example**

```result
┌─multiplyDecimal(toDecimal256(-12, 0), toDecimal32(-2.1, 1), 1)─┐
│                                                           25.2 │
└────────────────────────────────────────────────────────────────┘
```

**Differences compared to regular multiplication:**

```sql
SELECT toDecimal64(-12.647, 3) * toDecimal32(2.1239, 4);
SELECT toDecimal64(-12.647, 3) as a, toDecimal32(2.1239, 4) as b, multiplyDecimal(a, b);
```

Result:

```result
┌─multiply(toDecimal64(-12.647, 3), toDecimal32(2.1239, 4))─┐
│                                               -26.8609633 │
└───────────────────────────────────────────────────────────┘
┌─multiplyDecimal(toDecimal64(-12.647, 3), toDecimal32(2.1239, 4))─┐
│                                                         -26.8609 │
└──────────────────────────────────────────────────────────────────┘
```

```sql
SELECT
    toDecimal64(-12.647987876, 9) AS a,
    toDecimal64(123.967645643, 9) AS b,
    multiplyDecimal(a, b);

SELECT
    toDecimal64(-12.647987876, 9) AS a,
    toDecimal64(123.967645643, 9) AS b,
    a * b;
```

Result:

```result
┌─────────────a─┬─────────────b─┬─multiplyDecimal(toDecimal64(-12.647987876, 9), toDecimal64(123.967645643, 9))─┐
│ -12.647987876 │ 123.967645643 │                                                               -1567.941279108 │
└───────────────┴───────────────┴───────────────────────────────────────────────────────────────────────────────┘

Received exception from server (version 22.11.1):
Code: 407. DB::Exception: Received from localhost:9000. DB::Exception: Decimal math overflow: While processing toDecimal64(-12.647987876, 9) AS a, toDecimal64(123.967645643, 9) AS b, a * b. (DECIMAL_OVERFLOW)
```

## divideDecimal


Divides two decimals `a` and `b`. The result value will be of type [Decimal256](../../sql-reference/data-types/decimal.md).

The scale of the result can be explicitly specified by `result_scale`. If `result_scale` is not specified, it is assumed to be the maximum scale of the input values.

This function work significantly slower than usual `divide`. In case no control over the result precision is needed and/or fast computation is desired, consider using `divide`.

**Syntax**

```sql
divideDecimal(a, b[, result_scale])
```

**Arguments**

- `a` — First value: [Decimal](../../sql-reference/data-types/decimal.md).
- `b` — Second value: [Decimal](../../sql-reference/data-types/decimal.md).
- `result_scale` — Scale of result: [Int/UInt](../../sql-reference/data-types/int-uint.md).

**Returned value**

- The result of division with given scale.

Type: [Decimal256](../../sql-reference/data-types/decimal.md).

**Example**

```result
┌─divideDecimal(toDecimal256(-12, 0), toDecimal32(2.1, 1), 10)─┐
│                                                -5.7142857142 │
└──────────────────────────────────────────────────────────────┘
```

**Differences compared to regular division:**

```sql
SELECT toDecimal64(-12, 1) / toDecimal32(2.1, 1);
SELECT toDecimal64(-12, 1) as a, toDecimal32(2.1, 1) as b, divideDecimal(a, b, 1), divideDecimal(a, b, 5);
```

Result:

```result
┌─divide(toDecimal64(-12, 1), toDecimal32(2.1, 1))─┐
│                                             -5.7 │
└──────────────────────────────────────────────────┘

┌───a─┬───b─┬─divideDecimal(toDecimal64(-12, 1), toDecimal32(2.1, 1), 1)─┬─divideDecimal(toDecimal64(-12, 1), toDecimal32(2.1, 1), 5)─┐
│ -12 │ 2.1 │                                                       -5.7 │                                                   -5.71428 │
└─────┴─────┴────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────┘
```

```sql
SELECT toDecimal64(-12, 0) / toDecimal32(2.1, 1);
SELECT toDecimal64(-12, 0) as a, toDecimal32(2.1, 1) as b, divideDecimal(a, b, 1), divideDecimal(a, b, 5);
```

Result:

```result
DB::Exception: Decimal result's scale is less than argument's one: While processing toDecimal64(-12, 0) / toDecimal32(2.1, 1). (ARGUMENT_OUT_OF_BOUND)

┌───a─┬───b─┬─divideDecimal(toDecimal64(-12, 0), toDecimal32(2.1, 1), 1)─┬─divideDecimal(toDecimal64(-12, 0), toDecimal32(2.1, 1), 5)─┐
│ -12 │ 2.1 │                                                       -5.7 │                                                   -5.71428 │
└─────┴─────┴────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────┘
```
