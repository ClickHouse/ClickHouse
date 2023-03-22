---
slug: /en/sql-reference/functions/arithmetic-functions
sidebar_position: 34
sidebar_label: Arithmetic
---

# Arithmetic Functions

For all arithmetic functions, the result type is calculated as the smallest number type that the result fits in, if there is such a type. The minimum is taken simultaneously based on the number of bits, whether it is signed, and whether it floats. If there are not enough bits, the highest bit type is taken.

Example:

``` sql
SELECT toTypeName(0), toTypeName(0 + 0), toTypeName(0 + 0 + 0), toTypeName(0 + 0 + 0 + 0)
```

``` text
┌─toTypeName(0)─┬─toTypeName(plus(0, 0))─┬─toTypeName(plus(plus(0, 0), 0))─┬─toTypeName(plus(plus(plus(0, 0), 0), 0))─┐
│ UInt8         │ UInt16                 │ UInt32                          │ UInt64                                   │
└───────────────┴────────────────────────┴─────────────────────────────────┴──────────────────────────────────────────┘
```

Arithmetic functions work for any pair of types from UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, or Float64.

Overflow is produced the same way as in C++.

## plus(a, b), a + b operator

Calculates the sum of the numbers.
You can also add integer numbers with a date or date and time. In the case of a date, adding an integer means adding the corresponding number of days. For a date with time, it means adding the corresponding number of seconds.

## minus(a, b), a - b operator

Calculates the difference. The result is always signed.

You can also calculate integer numbers from a date or date with time. The idea is the same – see above for ‘plus’.

## multiply(a, b), a \* b operator

Calculates the product of the numbers.

## divide(a, b), a / b operator

Calculates the quotient of the numbers. The result type is always a floating-point type.
It is not integer division. For integer division, use the ‘intDiv’ function.
When dividing by zero you get ‘inf’, ‘-inf’, or ‘nan’.

## intDiv(a, b)

Calculates the quotient of the numbers. Divides into integers, rounding down (by the absolute value).
An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.

## intDivOrZero(a, b)

Differs from ‘intDiv’ in that it returns zero when dividing by zero or when dividing a minimal negative number by minus one.

## modulo(a, b), a % b operator

Calculates the remainder when dividing `a` by `b`.
The result type is an integer if both inputs are integers. If one of the inputs is a floating-point number, the result is a floating-point number.
The remainder is computed like in C++. Truncated division is used for negative numbers.
An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.

## moduloOrZero(a, b)

Differs from [modulo](#modulo) in that it returns zero when the divisor is zero.

## positiveModulo(a, b), positive_modulo(a, b), pmod(a, b)
Calculates the remainder when dividing `a` by `b`. Similar to the function `modulo` except that `positive_modulo` always returns a non-negative number.

Notice that `positive_modulo` is 4-5 times slower than `modulo`. You should not use `positive_modulo` unless you want to get a positive result and don't care about performance too much.

**Example**

Query:

```sql
SELECT positiveModulo(-1, 10)
```

Result:

```text

┌─positiveModulo(-1, 10)─┐
│                      9 │
└────────────────────────┘
```

## negate(a), -a operator

Calculates a number with the reverse sign. The result is always signed.

## abs(a)

Calculates the absolute value of the number (a). That is, if a \< 0, it returns -a. For unsigned types it does not do anything. For signed integer types, it returns an unsigned number.

## gcd(a, b)

Returns the greatest common divisor of the numbers.
An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.

## lcm(a, b)

Returns the least common multiple of the numbers.
An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.

## max2

Compares two values and returns the maximum. The returned value is converted to [Float64](../../sql-reference/data-types/float.md).

**Syntax**

```sql
max2(value1, value2)
```

**Arguments**

-   `value1` — First value. [Int/UInt](../../sql-reference/data-types/int-uint.md) or [Float](../../sql-reference/data-types/float.md).
-   `value2` — Second value. [Int/UInt](../../sql-reference/data-types/int-uint.md) or [Float](../../sql-reference/data-types/float.md).

**Returned value**

-   The maximum of two values.

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT max2(-1, 2);
```

Result:

```text
┌─max2(-1, 2)─┐
│           2 │
└─────────────┘
```

## min2

Compares two values and returns the minimum. The returned value is converted to [Float64](../../sql-reference/data-types/float.md).

**Syntax**

```sql
min2(value1, value2)
```

**Arguments**

-   `value1` — First value. [Int/UInt](../../sql-reference/data-types/int-uint.md) or [Float](../../sql-reference/data-types/float.md).
-   `value2` — Second value. [Int/UInt](../../sql-reference/data-types/int-uint.md) or [Float](../../sql-reference/data-types/float.md).

**Returned value**

-   The minimum of two values.

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT min2(-1, 2);
```

Result:

```text
┌─min2(-1, 2)─┐
│          -1 │
└─────────────┘
```

## multiplyDecimal(a, b[, result_scale])

Performs multiplication on two decimals. Result value will be of type [Decimal256](../../sql-reference/data-types/decimal.md).
Result scale can be explicitly specified by `result_scale` argument (const Integer in range `[0, 76]`). If not specified, the result scale is the max scale of given arguments.

:::note    
These functions work significantly slower than usual `multiply`.
In case you don't really need controlled precision and/or need fast computation, consider using [multiply](#multiply)
:::

**Syntax**

```sql
multiplyDecimal(a, b[, result_scale])
```

**Arguments**

-   `a` — First value: [Decimal](../../sql-reference/data-types/decimal.md).
-   `b` — Second value: [Decimal](../../sql-reference/data-types/decimal.md).
-   `result_scale` — Scale of result: [Int/UInt](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   The result of multiplication with given scale.

Type: [Decimal256](../../sql-reference/data-types/decimal.md).

**Example**

```text
┌─multiplyDecimal(toDecimal256(-12, 0), toDecimal32(-2.1, 1), 1)─┐
│                                                           25.2 │
└────────────────────────────────────────────────────────────────┘
```

**Difference from regular multiplication:**
```sql
SELECT toDecimal64(-12.647, 3) * toDecimal32(2.1239, 4);
SELECT toDecimal64(-12.647, 3) as a, toDecimal32(2.1239, 4) as b, multiplyDecimal(a, b);
```

```text
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

```text
┌─────────────a─┬─────────────b─┬─multiplyDecimal(toDecimal64(-12.647987876, 9), toDecimal64(123.967645643, 9))─┐
│ -12.647987876 │ 123.967645643 │                                                               -1567.941279108 │
└───────────────┴───────────────┴───────────────────────────────────────────────────────────────────────────────┘

Received exception from server (version 22.11.1):
Code: 407. DB::Exception: Received from localhost:9000. DB::Exception: Decimal math overflow: While processing toDecimal64(-12.647987876, 9) AS a, toDecimal64(123.967645643, 9) AS b, a * b. (DECIMAL_OVERFLOW)
```

## divideDecimal(a, b[, result_scale])

Performs division on two decimals. Result value will be of type [Decimal256](../../sql-reference/data-types/decimal.md).
Result scale can be explicitly specified by `result_scale` argument (const Integer in range `[0, 76]`). If not specified, the result scale is the max scale of given arguments.

:::note    
These function work significantly slower than usual `divide`.
In case you don't really need controlled precision and/or need fast computation, consider using [divide](#divide).
:::

**Syntax**

```sql
divideDecimal(a, b[, result_scale])
```

**Arguments**

-   `a` — First value: [Decimal](../../sql-reference/data-types/decimal.md).
-   `b` — Second value: [Decimal](../../sql-reference/data-types/decimal.md).
-   `result_scale` — Scale of result: [Int/UInt](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   The result of division with given scale.

Type: [Decimal256](../../sql-reference/data-types/decimal.md).

**Example**

```text
┌─divideDecimal(toDecimal256(-12, 0), toDecimal32(2.1, 1), 10)─┐
│                                                -5.7142857142 │
└──────────────────────────────────────────────────────────────┘
```

**Difference from regular division:**
```sql
SELECT toDecimal64(-12, 1) / toDecimal32(2.1, 1);
SELECT toDecimal64(-12, 1) as a, toDecimal32(2.1, 1) as b, divideDecimal(a, b, 1), divideDecimal(a, b, 5);
```

```text
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

```text
DB::Exception: Decimal result's scale is less than argument's one: While processing toDecimal64(-12, 0) / toDecimal32(2.1, 1). (ARGUMENT_OUT_OF_BOUND)

┌───a─┬───b─┬─divideDecimal(toDecimal64(-12, 0), toDecimal32(2.1, 1), 1)─┬─divideDecimal(toDecimal64(-12, 0), toDecimal32(2.1, 1), 5)─┐
│ -12 │ 2.1 │                                                       -5.7 │                                                   -5.71428 │
└─────┴─────┴────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────┘
```
