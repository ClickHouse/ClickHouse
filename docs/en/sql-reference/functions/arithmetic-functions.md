---
toc_priority: 34
toc_title: Arithmetic
---

# Arithmetic Functions {#arithmetic-functions}

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

## plus(a, b), a + b operator {#plusa-b-a-b-operator}

Calculates the sum of the numbers.
You can also add integer numbers with a date or date and time. In the case of a date, adding an integer means adding the corresponding number of days. For a date with time, it means adding the corresponding number of seconds.

## minus(a, b), a - b operator {#minusa-b-a-b-operator}

Calculates the difference. The result is always signed.

You can also calculate integer numbers from a date or date with time. The idea is the same – see above for ‘plus’.

## multiply(a, b), a \* b operator {#multiplya-b-a-b-operator}

Calculates the product of the numbers.

## divide(a, b), a / b operator {#dividea-b-a-b-operator}

Calculates the quotient of the numbers. The result type is always a floating-point type.
It is not integer division. For integer division, use the ‘intDiv’ function.
When dividing by zero you get ‘inf’, ‘-inf’, or ‘nan’.

## intDiv(a, b) {#intdiva-b}

Calculates the quotient of the numbers. Divides into integers, rounding down (by the absolute value).
An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.

## intDivOrZero(a, b) {#intdivorzeroa-b}

Differs from ‘intDiv’ in that it returns zero when dividing by zero or when dividing a minimal negative number by minus one.

## modulo(a, b), a % b operator {#modulo}

Calculates the remainder after division.
If arguments are floating-point numbers, they are pre-converted to integers by dropping the decimal portion.
The remainder is taken in the same sense as in C++. Truncated division is used for negative numbers.
An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.

## moduloOrZero(a, b) {#modulo-or-zero}

Differs from [modulo](#modulo) in that it returns zero when the divisor is zero.

## negate(a), -a operator {#negatea-a-operator}

Calculates a number with the reverse sign. The result is always signed.

## abs(a) {#arithm_func-abs}

Calculates the absolute value of the number (a). That is, if a \< 0, it returns -a. For unsigned types it does not do anything. For signed integer types, it returns an unsigned number.

## gcd(a, b) {#gcda-b}

Returns the greatest common divisor of the numbers.
An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.

## lcm(a, b) {#lcma-b}

Returns the least common multiple of the numbers.
An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.

## max2 {#max2}

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

## min2 {#min2}

Compares two values and returns the minimum. The returned value is converted to [Float64](../../sql-reference/data-types/float.md).

**Syntax**

```sql
max2(value1, value2)
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
