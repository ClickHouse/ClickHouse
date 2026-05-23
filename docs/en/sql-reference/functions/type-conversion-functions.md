---
description: 'Documentation for type conversion functions'
sidebar_label: 'Type conversion'
slug: /sql-reference/functions/type-conversion-functions
title: 'Type conversion functions'
doc_type: 'reference'
---

## Common issues with data conversion {#common-issues-with-data-conversion}

ClickHouse generally uses the [same behavior as C++ programs](https://en.cppreference.com/w/cpp/language/implicit_conversion).

`to<type>` functions and [cast](#CAST) behave differently in some cases, for example in case of [LowCardinality](../data-types/lowcardinality.md): [cast](#CAST) removes [LowCardinality](../data-types/lowcardinality.md) trait `to<type>` functions don't. The same with [Nullable](../data-types/nullable.md), this behaviour is not compatible with SQL standard, and it can be changed using [cast_keep_nullable](../../operations/settings/settings.md/#cast_keep_nullable) setting.

:::note
Be aware of potential data loss if values of a datatype are converted to a smaller datatype (for example from `Int64` to `Int32`) or between
incompatible datatypes (for example from `String` to `Int`). Make sure to check carefully if the result is as expected.
:::

Example:

```sql
SELECT
    toTypeName(toLowCardinality('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type

┌─source_type────────────┬─to_type_result_type────┬─cast_result_type─┐
│ LowCardinality(String) │ LowCardinality(String) │ String           │
└────────────────────────┴────────────────────────┴──────────────────┘

SELECT
    toTypeName(toNullable('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type

┌─source_type──────┬─to_type_result_type─┬─cast_result_type─┐
│ Nullable(String) │ Nullable(String)    │ String           │
└──────────────────┴─────────────────────┴──────────────────┘

SELECT
    toTypeName(toNullable('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type
SETTINGS cast_keep_nullable = 1

┌─source_type──────┬─to_type_result_type─┬─cast_result_type─┐
│ Nullable(String) │ Nullable(String)    │ Nullable(String) │
└──────────────────┴─────────────────────┴──────────────────┘
```

## Notes on `toString` functions {#to-string-functions}

The `toString` family of functions allows for converting between numbers, strings (but not fixed strings), dates, and dates with times.
All of these functions accept one argument.

- When converting to or from a string, the value is formatted or parsed using the same rules as for the TabSeparated format (and almost all other text formats). If the string can't be parsed, an exception is thrown and the request is canceled.
- When converting dates to numbers or vice versa, the date corresponds to the number of days since the beginning of the Unix epoch.
- When converting dates with times to numbers or vice versa, the date with time corresponds to the number of seconds since the beginning of the Unix epoch.
- The `toString` function of the `DateTime` argument can take a second String argument containing the name of the time zone, for example: `Europe/Amsterdam`. In this case, the time is formatted according to the specified time zone.

## Notes on `toDate`/`toDateTime` functions {#to-date-and-date-time-functions}

<<<<<<< HEAD
**Arguments**

- `expr` — Expression returning a number or a string. [Expression](/sql-reference/syntax#expressions).

Supported arguments:
- Values of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.
- Strings `true` or `false` (case-insensitive).

**Returned value**

- Returns `true` or `false` based on evaluation of the argument. [Bool](../data-types/boolean.md).

**Example**

Query:

```sql
SELECT
    toBool(toUInt8(1)),
    toBool(toInt8(-1)),
    toBool(toFloat32(1.01)),
    toBool('true'),
    toBool('false'),
    toBool('FALSE')
FORMAT Vertical
```

Result:

```response
toBool(toUInt8(1)):      true
toBool(toInt8(-1)):      true
toBool(toFloat32(1.01)): true
toBool('true'):          true
toBool('false'):         false
toBool('FALSE'):         false
```

## toInt8 {#toint8}

Converts an input value to a value of type [`Int8`](../data-types/int-uint.md). Throws an exception in case of an error.

**Syntax**

```sql
toInt8(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt8('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int8](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
For example: `SELECT toInt8(128) == -128;`.
:::

**Returned value**

- 8-bit integer value. [Int8](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toInt8(-8),
    toInt8(-8.8),
    toInt8('-8')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt8(-8):   -8
toInt8(-8.8): -8
toInt8('-8'): -8
```

**See also**

- [`toInt8OrZero`](#toint8orzero).
- [`toInt8OrNull`](#toInt8OrNull).
- [`toInt8OrDefault`](#toint8ordefault).

## toInt8OrZero {#toint8orzero}

Like [`toInt8`](#toint8), this function converts an input value to a value of type [Int8](../data-types/int-uint.md) but returns `0` in case of an error.

**Syntax**

```sql
toInt8OrZero(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `0`):
- String representations of ordinary Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt8OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int8](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 8-bit integer value if successful, otherwise `0`. [Int8](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toInt8OrZero('-8'),
    toInt8OrZero('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt8OrZero('-8'):  -8
toInt8OrZero('abc'): 0
```

**See also**

- [`toInt8`](#toint8).
- [`toInt8OrNull`](#toInt8OrNull).
- [`toInt8OrDefault`](#toint8ordefault).

## toInt8OrNull {#toInt8OrNull}

Like [`toInt8`](#toint8), this function converts an input value to a value of type [Int8](../data-types/int-uint.md) but returns `NULL` in case of an error.

**Syntax**

```sql
toInt8OrNull(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `\N`)
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt8OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int8](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 8-bit integer value if successful, otherwise `NULL`. [Int8](../data-types/int-uint.md) / [NULL](../data-types/nullable.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toInt8OrNull('-8'),
    toInt8OrNull('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt8OrNull('-8'):  -8
toInt8OrNull('abc'): ᴺᵁᴸᴸ
```

**See also**

- [`toInt8`](#toint8).
- [`toInt8OrZero`](#toint8orzero).
- [`toInt8OrDefault`](#toint8ordefault).

## toInt8OrDefault {#toint8ordefault}

Like [`toInt8`](#toint8), this function converts an input value to a value of type [Int8](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.

**Syntax**

```sql
toInt8OrDefault(expr[, default])
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).
- `default` (optional) — The default value to return if parsing to type `Int8` is unsuccessful. [Int8](../data-types/int-uint.md).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Arguments for which the default value is returned:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt8OrDefault('0xc0fe', CAST('-1', 'Int8'));`.

:::note
If the input value cannot be represented within the bounds of [Int8](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 8-bit integer value if successful, otherwise returns the default value if passed or `0` if not. [Int8](../data-types/int-uint.md).

:::note
- The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
- The default value type should be the same as the cast type.
:::

**Example**

Query:

```sql
SELECT
    toInt8OrDefault('-8', CAST('-1', 'Int8')),
    toInt8OrDefault('abc', CAST('-1', 'Int8'))
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt8OrDefault('-8', CAST('-1', 'Int8')):  -8
toInt8OrDefault('abc', CAST('-1', 'Int8')): -1
```

**See also**

- [`toInt8`](#toint8).
- [`toInt8OrZero`](#toint8orzero).
- [`toInt8OrNull`](#toInt8OrNull).

## toInt16 {#toint16}

Converts an input value to a value of type [`Int16`](../data-types/int-uint.md). Throws an exception in case of an error.

**Syntax**

```sql
toInt16(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt16('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int16](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
For example: `SELECT toInt16(32768) == -32768;`.
:::

**Returned value**

- 16-bit integer value. [Int16](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toInt16(-16),
    toInt16(-16.16),
    toInt16('-16')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt16(-16):    -16
toInt16(-16.16): -16
toInt16('-16'):  -16
```

**See also**

- [`toInt16OrZero`](#toint16orzero).
- [`toInt16OrNull`](#toint16ornull).
- [`toInt16OrDefault`](#toint16ordefault).

## toInt16OrZero {#toint16orzero}

Like [`toInt16`](#toint16), this function converts an input value to a value of type [Int16](../data-types/int-uint.md) but returns `0` in case of an error.

**Syntax**

```sql
toInt16OrZero(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `0`):
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt16OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int16](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered as an error.
:::

**Returned value**

- 16-bit integer value if successful, otherwise `0`. [Int16](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toInt16OrZero('-16'),
    toInt16OrZero('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt16OrZero('-16'): -16
toInt16OrZero('abc'): 0
```

**See also**

- [`toInt16`](#toint16).
- [`toInt16OrNull`](#toint16ornull).
- [`toInt16OrDefault`](#toint16ordefault).

## toInt16OrNull {#toint16ornull}

Like [`toInt16`](#toint16), this function converts an input value to a value of type [Int16](../data-types/int-uint.md) but returns `NULL` in case of an error.

**Syntax**

```sql
toInt16OrNull(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `\N`)
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt16OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int16](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 16-bit integer value if successful, otherwise `NULL`. [Int16](../data-types/int-uint.md) / [NULL](../data-types/nullable.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toInt16OrNull('-16'),
    toInt16OrNull('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt16OrNull('-16'): -16
toInt16OrNull('abc'): ᴺᵁᴸᴸ
```

**See also**

- [`toInt16`](#toint16).
- [`toInt16OrZero`](#toint16orzero).
- [`toInt16OrDefault`](#toint16ordefault).

## toInt16OrDefault {#toint16ordefault}

Like [`toInt16`](#toint16), this function converts an input value to a value of type [Int16](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.

**Syntax**

```sql
toInt16OrDefault(expr[, default])
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).
- `default` (optional) — The default value to return if parsing to type `Int16` is unsuccessful. [Int16](../data-types/int-uint.md).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Arguments for which the default value is returned:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt16OrDefault('0xc0fe', CAST('-1', 'Int16'));`.

:::note
If the input value cannot be represented within the bounds of [Int16](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 16-bit integer value if successful, otherwise returns the default value if passed or `0` if not. [Int16](../data-types/int-uint.md).

:::note
- The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
- The default value type should be the same as the cast type.
:::

**Example**

Query:

```sql
SELECT
    toInt16OrDefault('-16', CAST('-1', 'Int16')),
    toInt16OrDefault('abc', CAST('-1', 'Int16'))
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt16OrDefault('-16', CAST('-1', 'Int16')): -16
toInt16OrDefault('abc', CAST('-1', 'Int16')): -1
```

**See also**

- [`toInt16`](#toint16).
- [`toInt16OrZero`](#toint16orzero).
- [`toInt16OrNull`](#toint16ornull).

## toInt32 {#toint32}

Converts an input value to a value of type [`Int32`](../data-types/int-uint.md). Throws an exception in case of an error.

**Syntax**

```sql
toInt32(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt32('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int32](../data-types/int-uint.md), the result over or under flows.
This is not considered an error.
For example: `SELECT toInt32(2147483648) == -2147483648;`
:::

**Returned value**

- 32-bit integer value. [Int32](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toInt32(-32),
    toInt32(-32.32),
    toInt32('-32')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt32(-32):    -32
toInt32(-32.32): -32
toInt32('-32'):  -32
```

**See also**

- [`toInt32OrZero`](#toint32orzero).
- [`toInt32OrNull`](#toint32ornull).
- [`toInt32OrDefault`](#toint32ordefault).

## toInt32OrZero {#toint32orzero}

Like [`toInt32`](#toint32), this function converts an input value to a value of type [Int32](../data-types/int-uint.md) but returns `0` in case of an error.

**Syntax**

```sql
toInt32OrZero(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `0`):
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt32OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int32](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 32-bit integer value if successful, otherwise `0`. [Int32](../data-types/int-uint.md)

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncate fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toInt32OrZero('-32'),
    toInt32OrZero('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt32OrZero('-32'): -32
toInt32OrZero('abc'): 0
```
**See also**

- [`toInt32`](#toint32).
- [`toInt32OrNull`](#toint32ornull).
- [`toInt32OrDefault`](#toint32ordefault).

## toInt32OrNull {#toint32ornull}

Like [`toInt32`](#toint32), this function converts an input value to a value of type [Int32](../data-types/int-uint.md) but returns `NULL` in case of an error.

**Syntax**

```sql
toInt32OrNull(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `\N`)
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt32OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int32](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 32-bit integer value if successful, otherwise `NULL`. [Int32](../data-types/int-uint.md) / [NULL](../data-types/nullable.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toInt32OrNull('-32'),
    toInt32OrNull('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt32OrNull('-32'): -32
toInt32OrNull('abc'): ᴺᵁᴸᴸ
```

**See also**

- [`toInt32`](#toint32).
- [`toInt32OrZero`](#toint32orzero).
- [`toInt32OrDefault`](#toint32ordefault).

## toInt32OrDefault {#toint32ordefault}

Like [`toInt32`](#toint32), this function converts an input value to a value of type [Int32](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.

**Syntax**

```sql
toInt32OrDefault(expr[, default])
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).
- `default` (optional) — The default value to return if parsing to type `Int32` is unsuccessful. [Int32](../data-types/int-uint.md).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Arguments for which the default value is returned:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt32OrDefault('0xc0fe', CAST('-1', 'Int32'));`.

:::note
If the input value cannot be represented within the bounds of [Int32](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 32-bit integer value if successful, otherwise returns the default value if passed or `0` if not. [Int32](../data-types/int-uint.md).

:::note
- The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
- The default value type should be the same as the cast type.
:::

**Example**

Query:

```sql
SELECT
    toInt32OrDefault('-32', CAST('-1', 'Int32')),
    toInt32OrDefault('abc', CAST('-1', 'Int32'))
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt32OrDefault('-32', CAST('-1', 'Int32')): -32
toInt32OrDefault('abc', CAST('-1', 'Int32')): -1
```

**See also**

- [`toInt32`](#toint32).
- [`toInt32OrZero`](#toint32orzero).
- [`toInt32OrNull`](#toint32ornull).

## toInt64 {#toint64}

Converts an input value to a value of type [`Int64`](../data-types/int-uint.md). Throws an exception in case of an error.

**Syntax**

```sql
toInt64(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Unsupported types:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt64('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int64](../data-types/int-uint.md), the result over or under flows.
This is not considered an error.
For example: `SELECT toInt64(9223372036854775808) == -9223372036854775808;`
:::

**Returned value**

- 64-bit integer value. [Int64](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toInt64(-64),
    toInt64(-64.64),
    toInt64('-64')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt64(-64):    -64
toInt64(-64.64): -64
toInt64('-64'):  -64
```

**See also**

- [`toInt64OrZero`](#toint64orzero).
- [`toInt64OrNull`](#toint64ornull).
- [`toInt64OrDefault`](#toint64ordefault).

## toInt64OrZero {#toint64orzero}

Like [`toInt64`](#toint64), this function converts an input value to a value of type [Int64](../data-types/int-uint.md) but returns `0` in case of an error.

**Syntax**

```sql
toInt64OrZero(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `0`):
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt64OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int64](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 64-bit integer value if successful, otherwise `0`. [Int64](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toInt64OrZero('-64'),
    toInt64OrZero('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt64OrZero('-64'): -64
toInt64OrZero('abc'): 0
```

**See also**

- [`toInt64`](#toint64).
- [`toInt64OrNull`](#toint64ornull).
- [`toInt64OrDefault`](#toint64ordefault).

## toInt64OrNull {#toint64ornull}

Like [`toInt64`](#toint64), this function converts an input value to a value of type [Int64](../data-types/int-uint.md) but returns `NULL` in case of an error.

**Syntax**

```sql
toInt64OrNull(x)
```

**Arguments**

- `x` — A String representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `\N`)
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt64OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int64](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 64-bit integer value if successful, otherwise `NULL`. [Int64](../data-types/int-uint.md) / [NULL](../data-types/nullable.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toInt64OrNull('-64'),
    toInt64OrNull('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt64OrNull('-64'): -64
toInt64OrNull('abc'): ᴺᵁᴸᴸ
```

**See also**

- [`toInt64`](#toint64).
- [`toInt64OrZero`](#toint64orzero).
- [`toInt64OrDefault`](#toint64ordefault).

## toInt64OrDefault {#toint64ordefault}

Like [`toInt64`](#toint64), this function converts an input value to a value of type [Int64](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.

**Syntax**

```sql
toInt64OrDefault(expr[, default])
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).
- `default` (optional) — The default value to return if parsing to type `Int64` is unsuccessful. [Int64](../data-types/int-uint.md).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Arguments for which the default value is returned:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt64OrDefault('0xc0fe', CAST('-1', 'Int64'));`.

:::note
If the input value cannot be represented within the bounds of [Int64](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 64-bit integer value if successful, otherwise returns the default value if passed or `0` if not. [Int64](../data-types/int-uint.md).

:::note
- The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
- The default value type should be the same as the cast type.
:::

**Example**

Query:

```sql
SELECT
    toInt64OrDefault('-64', CAST('-1', 'Int64')),
    toInt64OrDefault('abc', CAST('-1', 'Int64'))
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt64OrDefault('-64', CAST('-1', 'Int64')): -64
toInt64OrDefault('abc', CAST('-1', 'Int64')): -1
```

**See also**

- [`toInt64`](#toint64).
- [`toInt64OrZero`](#toint64orzero).
- [`toInt64OrNull`](#toint64ornull).

## toInt128 {#toint128}

Converts an input value to a value of type [`Int128`](../data-types/int-uint.md). Throws an exception in case of an error.

**Syntax**

```sql
toInt128(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt128('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int128](../data-types/int-uint.md), the result over or under flows.
This is not considered an error.
:::

**Returned value**

- 128-bit integer value. [Int128](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toInt128(-128),
    toInt128(-128.8),
    toInt128('-128')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt128(-128):   -128
toInt128(-128.8): -128
toInt128('-128'): -128
```

**See also**

- [`toInt128OrZero`](#toint128orzero).
- [`toInt128OrNull`](#toint128ornull).
- [`toInt128OrDefault`](#toint128ordefault).

## toInt128OrZero {#toint128orzero}

Like [`toInt128`](#toint128), this function converts an input value to a value of type [Int128](../data-types/int-uint.md) but returns `0` in case of an error.

**Syntax**

```sql
toInt128OrZero(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `0`):
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt128OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int128](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 128-bit integer value if successful, otherwise `0`. [Int128](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toInt128OrZero('-128'),
    toInt128OrZero('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt128OrZero('-128'): -128
toInt128OrZero('abc'):  0
```

**See also**

- [`toInt128`](#toint128).
- [`toInt128OrNull`](#toint128ornull).
- [`toInt128OrDefault`](#toint128ordefault).

## toInt128OrNull {#toint128ornull}

Like [`toInt128`](#toint128), this function converts an input value to a value of type [Int128](../data-types/int-uint.md) but returns `NULL` in case of an error.

**Syntax**

```sql
toInt128OrNull(x)
```

**Arguments**

- `x` — A String representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `\N`)
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt128OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int128](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 128-bit integer value if successful, otherwise `NULL`. [Int128](../data-types/int-uint.md) / [NULL](../data-types/nullable.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toInt128OrNull('-128'),
    toInt128OrNull('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt128OrNull('-128'): -128
toInt128OrNull('abc'):  ᴺᵁᴸᴸ
```

**See also**

- [`toInt128`](#toint128).
- [`toInt128OrZero`](#toint128orzero).
- [`toInt128OrDefault`](#toint128ordefault).

## toInt128OrDefault {#toint128ordefault}

Like [`toInt128`](#toint128), this function converts an input value to a value of type [Int128](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.

**Syntax**

```sql
toInt128OrDefault(expr[, default])
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).
- `default` (optional) — The default value to return if parsing to type `Int128` is unsuccessful. [Int128](../data-types/int-uint.md).

Supported arguments:
- (U)Int8/16/32/64/128/256.
- Float32/64.
- String representations of (U)Int8/16/32/128/256.

Arguments for which the default value is returned:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt128OrDefault('0xc0fe', CAST('-1', 'Int128'));`.

:::note
If the input value cannot be represented within the bounds of [Int128](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 128-bit integer value if successful, otherwise returns the default value if passed or `0` if not. [Int128](../data-types/int-uint.md).

:::note
- The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
- The default value type should be the same as the cast type.
:::

**Example**

Query:

```sql
SELECT
    toInt128OrDefault('-128', CAST('-1', 'Int128')),
    toInt128OrDefault('abc', CAST('-1', 'Int128'))
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt128OrDefault('-128', CAST('-1', 'Int128')): -128
toInt128OrDefault('abc', CAST('-1', 'Int128')):  -1
```

**See also**

- [`toInt128`](#toint128).
- [`toInt128OrZero`](#toint128orzero).
- [`toInt128OrNull`](#toint128ornull).

## toInt256 {#toint256}

Converts an input value to a value of type [`Int256`](../data-types/int-uint.md). Throws an exception in case of an error.

**Syntax**

```sql
toInt256(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt256('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int256](../data-types/int-uint.md), the result over or under flows.
This is not considered an error.
:::

**Returned value**

- 256-bit integer value. [Int256](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toInt256(-256),
    toInt256(-256.256),
    toInt256('-256')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt256(-256):     -256
toInt256(-256.256): -256
toInt256('-256'):   -256
```

**See also**

- [`toInt256OrZero`](#toint256orzero).
- [`toInt256OrNull`](#toint256ornull).
- [`toInt256OrDefault`](#toint256ordefault).

## toInt256OrZero {#toint256orzero}

Like [`toInt256`](#toint256), this function converts an input value to a value of type [Int256](../data-types/int-uint.md) but returns `0` in case of an error.

**Syntax**

```sql
toInt256OrZero(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `0`):
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt256OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int256](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 256-bit integer value if successful, otherwise `0`. [Int256](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toInt256OrZero('-256'),
    toInt256OrZero('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt256OrZero('-256'): -256
toInt256OrZero('abc'):  0
```

**See also**

- [`toInt256`](#toint256).
- [`toInt256OrNull`](#toint256ornull).
- [`toInt256OrDefault`](#toint256ordefault).

## toInt256OrNull {#toint256ornull}

Like [`toInt256`](#toint256), this function converts an input value to a value of type [Int256](../data-types/int-uint.md) but returns `NULL` in case of an error.

**Syntax**

```sql
toInt256OrNull(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `\N`)
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toInt256OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [Int256](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 256-bit integer value if successful, otherwise `NULL`. [Int256](../data-types/int-uint.md) / [NULL](../data-types/nullable.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toInt256OrNull('-256'),
    toInt256OrNull('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt256OrNull('-256'): -256
toInt256OrNull('abc'):  ᴺᵁᴸᴸ
```

**See also**

- [`toInt256`](#toint256).
- [`toInt256OrZero`](#toint256orzero).
- [`toInt256OrDefault`](#toint256ordefault).

## toInt256OrDefault {#toint256ordefault}

Like [`toInt256`](#toint256), this function converts an input value to a value of type [Int256](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.

**Syntax**

```sql
toInt256OrDefault(expr[, default])
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).
- `default` (optional) — The default value to return if parsing to type `Int256` is unsuccessful. [Int256](../data-types/int-uint.md).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Arguments for which the default value is returned:
- String representations of Float32/64 values, including `NaN` and `Inf`
- String representations of binary and hexadecimal values, e.g. `SELECT toInt256OrDefault('0xc0fe', CAST('-1', 'Int256'));`

:::note
If the input value cannot be represented within the bounds of [Int256](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 256-bit integer value if successful, otherwise returns the default value if passed or `0` if not. [Int256](../data-types/int-uint.md).

:::note
- The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
- The default value type should be the same as the cast type.
:::

**Example**

Query:

```sql
SELECT
    toInt256OrDefault('-256', CAST('-1', 'Int256')),
    toInt256OrDefault('abc', CAST('-1', 'Int256'))
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toInt256OrDefault('-256', CAST('-1', 'Int256')): -256
toInt256OrDefault('abc', CAST('-1', 'Int256')):  -1
```

**See also**

- [`toInt256`](#toint256).
- [`toInt256OrZero`](#toint256orzero).
- [`toInt256OrNull`](#toint256ornull).

## toUInt8 {#touint8}

Converts an input value to a value of type [`UInt8`](../data-types/int-uint.md). Throws an exception in case of an error.

**Syntax**

```sql
toUInt8(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt8('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt8](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
For example: `SELECT toUInt8(256) == 0;`.
:::

**Returned value**

- 8-bit unsigned integer value. [UInt8](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toUInt8(8),
    toUInt8(8.8),
    toUInt8('8')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt8(8):   8
toUInt8(8.8): 8
toUInt8('8'): 8
```

**See also**

- [`toUInt8OrZero`](#touint8orzero).
- [`toUInt8OrNull`](#touint8ornull).
- [`toUInt8OrDefault`](#touint8ordefault).

## toUInt8OrZero {#touint8orzero}

Like [`toUInt8`](#touint8), this function converts an input value to a value of type [UInt8](../data-types/int-uint.md) but returns `0` in case of an error.

**Syntax**

```sql
toUInt8OrZero(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `0`):
- String representations of ordinary Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt8OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt8](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 8-bit unsigned integer value if successful, otherwise `0`. [UInt8](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toUInt8OrZero('-8'),
    toUInt8OrZero('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt8OrZero('-8'):  0
toUInt8OrZero('abc'): 0
```

**See also**

- [`toUInt8`](#touint8).
- [`toUInt8OrNull`](#touint8ornull).
- [`toUInt8OrDefault`](#touint8ordefault).

## toUInt8OrNull {#touint8ornull}

Like [`toUInt8`](#touint8), this function converts an input value to a value of type [UInt8](../data-types/int-uint.md) but returns `NULL` in case of an error.

**Syntax**

```sql
toUInt8OrNull(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `\N`)
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt8OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt8](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 8-bit unsigned integer value if successful, otherwise `NULL`. [UInt8](../data-types/int-uint.md) / [NULL](../data-types/nullable.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toUInt8OrNull('8'),
    toUInt8OrNull('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt8OrNull('8'):   8
toUInt8OrNull('abc'): ᴺᵁᴸᴸ
```

**See also**

- [`toUInt8`](#touint8).
- [`toUInt8OrZero`](#touint8orzero).
- [`toUInt8OrDefault`](#touint8ordefault).

## toUInt8OrDefault {#touint8ordefault}

Like [`toUInt8`](#touint8), this function converts an input value to a value of type [UInt8](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.

**Syntax**

```sql
toUInt8OrDefault(expr[, default])
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).
- `default` (optional) — The default value to return if parsing to type `UInt8` is unsuccessful. [UInt8](../data-types/int-uint.md).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Arguments for which the default value is returned:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt8OrDefault('0xc0fe', CAST('0', 'UInt8'));`.

:::note
If the input value cannot be represented within the bounds of [UInt8](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 8-bit unsigned integer value if successful, otherwise returns the default value if passed or `0` if not. [UInt8](../data-types/int-uint.md).

:::note
- The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
- The default value type should be the same as the cast type.
:::

**Example**

Query:

```sql
SELECT
    toUInt8OrDefault('8', CAST('0', 'UInt8')),
    toUInt8OrDefault('abc', CAST('0', 'UInt8'))
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt8OrDefault('8', CAST('0', 'UInt8')):   8
toUInt8OrDefault('abc', CAST('0', 'UInt8')): 0
```

**See also**

- [`toUInt8`](#touint8).
- [`toUInt8OrZero`](#touint8orzero).
- [`toUInt8OrNull`](#touint8ornull).

## toUInt16 {#touint16}

Converts an input value to a value of type [`UInt16`](../data-types/int-uint.md). Throws an exception in case of an error.

**Syntax**

```sql
toUInt16(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt16('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt16](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
For example: `SELECT toUInt16(65536) == 0;`.
:::

**Returned value**

- 16-bit unsigned integer value. [UInt16](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toUInt16(16),
    toUInt16(16.16),
    toUInt16('16')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt16(16):    16
toUInt16(16.16): 16
toUInt16('16'):  16
```

**See also**

- [`toUInt16OrZero`](#touint16orzero).
- [`toUInt16OrNull`](#touint16ornull).
- [`toUInt16OrDefault`](#touint16ordefault).

## toUInt16OrZero {#touint16orzero}

Like [`toUInt16`](#touint16), this function converts an input value to a value of type [UInt16](../data-types/int-uint.md) but returns `0` in case of an error.

**Syntax**

```sql
toUInt16OrZero(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `0`):
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt16OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt16](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered as an error.
:::

**Returned value**

- 16-bit unsigned integer value if successful, otherwise `0`. [UInt16](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toUInt16OrZero('16'),
    toUInt16OrZero('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt16OrZero('16'):  16
toUInt16OrZero('abc'): 0
```

**See also**

- [`toUInt16`](#touint16).
- [`toUInt16OrNull`](#touint16ornull).
- [`toUInt16OrDefault`](#touint16ordefault).

## toUInt16OrNull {#touint16ornull}

Like [`toUInt16`](#touint16), this function converts an input value to a value of type [UInt16](../data-types/int-uint.md) but returns `NULL` in case of an error.

**Syntax**

```sql
toUInt16OrNull(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `\N`)
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt16OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt16](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 16-bit unsigned integer value if successful, otherwise `NULL`. [UInt16](../data-types/int-uint.md) / [NULL](../data-types/nullable.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toUInt16OrNull('16'),
    toUInt16OrNull('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt16OrNull('16'):  16
toUInt16OrNull('abc'): ᴺᵁᴸᴸ
```

**See also**

- [`toUInt16`](#touint16).
- [`toUInt16OrZero`](#touint16orzero).
- [`toUInt16OrDefault`](#touint16ordefault).

## toUInt16OrDefault {#touint16ordefault}

Like [`toUInt16`](#touint16), this function converts an input value to a value of type [UInt16](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.

**Syntax**

```sql
toUInt16OrDefault(expr[, default])
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).
- `default` (optional) — The default value to return if parsing to type `UInt16` is unsuccessful. [UInt16](../data-types/int-uint.md).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Arguments for which the default value is returned:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt16OrDefault('0xc0fe', CAST('0', 'UInt16'));`.

:::note
If the input value cannot be represented within the bounds of [UInt16](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 16-bit unsigned integer value if successful, otherwise returns the default value if passed or `0` if not. [UInt16](../data-types/int-uint.md).

:::note
- The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
- The default value type should be the same as the cast type.
:::

**Example**

Query:

```sql
SELECT
    toUInt16OrDefault('16', CAST('0', 'UInt16')),
    toUInt16OrDefault('abc', CAST('0', 'UInt16'))
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt16OrDefault('16', CAST('0', 'UInt16')):  16
toUInt16OrDefault('abc', CAST('0', 'UInt16')): 0
```

**See also**

- [`toUInt16`](#touint16).
- [`toUInt16OrZero`](#touint16orzero).
- [`toUInt16OrNull`](#touint16ornull).

## toUInt32 {#touint32}

Converts an input value to a value of type [`UInt32`](../data-types/int-uint.md). Throws an exception in case of an error.

**Syntax**

```sql
toUInt32(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt32('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt32](../data-types/int-uint.md), the result over or under flows.
This is not considered an error.
For example: `SELECT toUInt32(4294967296) == 0;`
:::

**Returned value**

- 32-bit unsigned integer value. [UInt32](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toUInt32(32),
    toUInt32(32.32),
    toUInt32('32')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt32(32):    32
toUInt32(32.32): 32
toUInt32('32'):  32
```

**See also**

- [`toUInt32OrZero`](#touint32orzero).
- [`toUInt32OrNull`](#touint32ornull).
- [`toUInt32OrDefault`](#touint32ordefault).

## toUInt32OrZero {#touint32orzero}

Like [`toUInt32`](#touint32), this function converts an input value to a value of type [UInt32](../data-types/int-uint.md) but returns `0` in case of an error.

**Syntax**

```sql
toUInt32OrZero(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `0`):
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt32OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt32](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 32-bit unsigned integer value if successful, otherwise `0`. [UInt32](../data-types/int-uint.md)

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)
, meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toUInt32OrZero('32'),
    toUInt32OrZero('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt32OrZero('32'):  32
toUInt32OrZero('abc'): 0
```
**See also**

- [`toUInt32`](#touint32).
- [`toUInt32OrNull`](#touint32ornull).
- [`toUInt32OrDefault`](#touint32ordefault).

## toUInt32OrNull {#touint32ornull}

Like [`toUInt32`](#touint32), this function converts an input value to a value of type [UInt32](../data-types/int-uint.md) but returns `NULL` in case of an error.

**Syntax**

```sql
toUInt32OrNull(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `\N`)
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt32OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt32](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 32-bit unsigned integer value if successful, otherwise `NULL`. [UInt32](../data-types/int-uint.md) / [NULL](../data-types/nullable.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)
, meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toUInt32OrNull('32'),
    toUInt32OrNull('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt32OrNull('32'):  32
toUInt32OrNull('abc'): ᴺᵁᴸᴸ
```

**See also**

- [`toUInt32`](#touint32).
- [`toUInt32OrZero`](#touint32orzero).
- [`toUInt32OrDefault`](#touint32ordefault).

## toUInt32OrDefault {#touint32ordefault}

Like [`toUInt32`](#touint32), this function converts an input value to a value of type [UInt32](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.

**Syntax**

```sql
toUInt32OrDefault(expr[, default])
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).
- `default` (optional) — The default value to return if parsing to type `UInt32` is unsuccessful. [UInt32](../data-types/int-uint.md).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Arguments for which the default value is returned:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt32OrDefault('0xc0fe', CAST('0', 'UInt32'));`.

:::note
If the input value cannot be represented within the bounds of [UInt32](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 32-bit unsigned integer value if successful, otherwise returns the default value if passed or `0` if not. [UInt32](../data-types/int-uint.md).

:::note
- The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
- The default value type should be the same as the cast type.
:::

**Example**

Query:

```sql
SELECT
    toUInt32OrDefault('32', CAST('0', 'UInt32')),
    toUInt32OrDefault('abc', CAST('0', 'UInt32'))
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt32OrDefault('32', CAST('0', 'UInt32')):  32
toUInt32OrDefault('abc', CAST('0', 'UInt32')): 0
```

**See also**

- [`toUInt32`](#touint32).
- [`toUInt32OrZero`](#touint32orzero).
- [`toUInt32OrNull`](#touint32ornull).

## toUInt64 {#touint64}

Converts an input value to a value of type [`UInt64`](../data-types/int-uint.md). Throws an exception in case of an error.

**Syntax**

```sql
toUInt64(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Unsupported types:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt64('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt64](../data-types/int-uint.md), the result over or under flows.
This is not considered an error.
For example: `SELECT toUInt64(18446744073709551616) == 0;`
:::

**Returned value**

- 64-bit unsigned integer value. [UInt64](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toUInt64(64),
    toUInt64(64.64),
    toUInt64('64')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt64(64):    64
toUInt64(64.64): 64
toUInt64('64'):  64
```

**See also**

- [`toUInt64OrZero`](#touint64orzero).
- [`toUInt64OrNull`](#touint64ornull).
- [`toUInt64OrDefault`](#touint64ordefault).

## toUInt64OrZero {#touint64orzero}

Like [`toUInt64`](#touint64), this function converts an input value to a value of type [UInt64](../data-types/int-uint.md) but returns `0` in case of an error.

**Syntax**

```sql
toUInt64OrZero(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `0`):
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt64OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt64](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 64-bit unsigned integer value if successful, otherwise `0`. [UInt64](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toUInt64OrZero('64'),
    toUInt64OrZero('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt64OrZero('64'):  64
toUInt64OrZero('abc'): 0
```

**See also**

- [`toUInt64`](#touint64).
- [`toUInt64OrNull`](#touint64ornull).
- [`toUInt64OrDefault`](#touint64ordefault).

## toUInt64OrNull {#touint64ornull}

Like [`toUInt64`](#touint64), this function converts an input value to a value of type [UInt64](../data-types/int-uint.md) but returns `NULL` in case of an error.

**Syntax**

```sql
toUInt64OrNull(x)
```

**Arguments**

- `x` — A String representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `\N`)
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt64OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt64](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 64-bit unsigned integer value if successful, otherwise `NULL`. [UInt64](../data-types/int-uint.md) / [NULL](../data-types/nullable.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toUInt64OrNull('64'),
    toUInt64OrNull('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt64OrNull('64'):  64
toUInt64OrNull('abc'): ᴺᵁᴸᴸ
```

**See also**

- [`toUInt64`](#touint64).
- [`toUInt64OrZero`](#touint64orzero).
- [`toUInt64OrDefault`](#touint64ordefault).

## toUInt64OrDefault {#touint64ordefault}

Like [`toUInt64`](#touint64), this function converts an input value to a value of type [UInt64](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.

**Syntax**

```sql
toUInt64OrDefault(expr[, default])
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).
- `defauult` (optional) — The default value to return if parsing to type `UInt64` is unsuccessful. [UInt64](../data-types/int-uint.md).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Arguments for which the default value is returned:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt64OrDefault('0xc0fe', CAST('0', 'UInt64'));`.

:::note
If the input value cannot be represented within the bounds of [UInt64](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 64-bit unsigned integer value if successful, otherwise returns the default value if passed or `0` if not. [UInt64](../data-types/int-uint.md).

:::note
- The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
- The default value type should be the same as the cast type.
:::

**Example**

Query:

```sql
SELECT
    toUInt64OrDefault('64', CAST('0', 'UInt64')),
    toUInt64OrDefault('abc', CAST('0', 'UInt64'))
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt64OrDefault('64', CAST('0', 'UInt64')):  64
toUInt64OrDefault('abc', CAST('0', 'UInt64')): 0
```

**See also**

- [`toUInt64`](#touint64).
- [`toUInt64OrZero`](#touint64orzero).
- [`toUInt64OrNull`](#touint64ornull).

## toUInt128 {#touint128}

Converts an input value to a value of type [`UInt128`](../data-types/int-uint.md). Throws an exception in case of an error.

**Syntax**

```sql
toUInt128(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt128('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt128](../data-types/int-uint.md), the result over or under flows.
This is not considered an error.
:::

**Returned value**

- 128-bit unsigned integer value. [UInt128](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toUInt128(128),
    toUInt128(128.8),
    toUInt128('128')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt128(128):   128
toUInt128(128.8): 128
toUInt128('128'): 128
```

**See also**

- [`toUInt128OrZero`](#touint128orzero).
- [`toUInt128OrNull`](#touint128ornull).
- [`toUInt128OrDefault`](#touint128ordefault).

## toUInt128OrZero {#touint128orzero}

Like [`toUInt128`](#touint128), this function converts an input value to a value of type [UInt128](../data-types/int-uint.md) but returns `0` in case of an error.

**Syntax**

```sql
toUInt128OrZero(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `0`):
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt128OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt128](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 128-bit unsigned integer value if successful, otherwise `0`. [UInt128](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toUInt128OrZero('128'),
    toUInt128OrZero('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt128OrZero('128'): 128
toUInt128OrZero('abc'): 0
```

**See also**

- [`toUInt128`](#touint128).
- [`toUInt128OrNull`](#touint128ornull).
- [`toUInt128OrDefault`](#touint128ordefault).

## toUInt128OrNull {#touint128ornull}

Like [`toUInt128`](#touint128), this function converts an input value to a value of type [UInt128](../data-types/int-uint.md) but returns `NULL` in case of an error.

**Syntax**

```sql
toUInt128OrNull(x)
```

**Arguments**

- `x` — A String representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `\N`)
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt128OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt128](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 128-bit unsigned integer value if successful, otherwise `NULL`. [UInt128](../data-types/int-uint.md) / [NULL](../data-types/nullable.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toUInt128OrNull('128'),
    toUInt128OrNull('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt128OrNull('128'): 128
toUInt128OrNull('abc'): ᴺᵁᴸᴸ
```

**See also**

- [`toUInt128`](#touint128).
- [`toUInt128OrZero`](#touint128orzero).
- [`toUInt128OrDefault`](#touint128ordefault).

## toUInt128OrDefault {#touint128ordefault}

Like [`toUInt128`](#toint128), this function converts an input value to a value of type [UInt128](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.

**Syntax**

```sql
toUInt128OrDefault(expr[, default])
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).
- `default` (optional) — The default value to return if parsing to type `UInt128` is unsuccessful. [UInt128](../data-types/int-uint.md).

Supported arguments:
- (U)Int8/16/32/64/128/256.
- Float32/64.
- String representations of (U)Int8/16/32/128/256.

Arguments for which the default value is returned:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt128OrDefault('0xc0fe', CAST('0', 'UInt128'));`.

:::note
If the input value cannot be represented within the bounds of [UInt128](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 128-bit unsigned integer value if successful, otherwise returns the default value if passed or `0` if not. [UInt128](../data-types/int-uint.md).

:::note
- The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
- The default value type should be the same as the cast type.
:::

**Example**

Query:

```sql
SELECT
    toUInt128OrDefault('128', CAST('0', 'UInt128')),
    toUInt128OrDefault('abc', CAST('0', 'UInt128'))
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt128OrDefault('128', CAST('0', 'UInt128')): 128
toUInt128OrDefault('abc', CAST('0', 'UInt128')): 0
```

**See also**

- [`toUInt128`](#touint128).
- [`toUInt128OrZero`](#touint128orzero).
- [`toUInt128OrNull`](#touint128ornull).

## toUInt256 {#touint256}

Converts an input value to a value of type [`UInt256`](../data-types/int-uint.md). Throws an exception in case of an error.

**Syntax**

```sql
toUInt256(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt256('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt256](../data-types/int-uint.md), the result over or under flows.
This is not considered an error.
:::

**Returned value**

- 256-bit unsigned integer value. [Int256](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toUInt256(256),
    toUInt256(256.256),
    toUInt256('256')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt256(256):     256
toUInt256(256.256): 256
toUInt256('256'):   256
```

**See also**

- [`toUInt256OrZero`](#touint256orzero).
- [`toUInt256OrNull`](#touint256ornull).
- [`toUInt256OrDefault`](#touint256ordefault).

## toUInt256OrZero {#touint256orzero}

Like [`toUInt256`](#touint256), this function converts an input value to a value of type [UInt256](../data-types/int-uint.md) but returns `0` in case of an error.

**Syntax**

```sql
toUInt256OrZero(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `0`):
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt256OrZero('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt256](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 256-bit unsigned integer value if successful, otherwise `0`. [UInt256](../data-types/int-uint.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toUInt256OrZero('256'),
    toUInt256OrZero('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt256OrZero('256'): 256
toUInt256OrZero('abc'): 0
```

**See also**

- [`toUInt256`](#touint256).
- [`toUInt256OrNull`](#touint256ornull).
- [`toUInt256OrDefault`](#touint256ordefault).

## toUInt256OrNull {#touint256ornull}

Like [`toUInt256`](#touint256), this function converts an input value to a value of type [UInt256](../data-types/int-uint.md) but returns `NULL` in case of an error.

**Syntax**

```sql
toUInt256OrNull(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256.

Unsupported arguments (return `\N`)
- String representations of Float32/64 values, including `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt256OrNull('0xc0fe');`.

:::note
If the input value cannot be represented within the bounds of [UInt256](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 256-bit unsigned integer value if successful, otherwise `NULL`. [UInt256](../data-types/int-uint.md) / [NULL](../data-types/nullable.md).

:::note
The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
:::

**Example**

Query:

```sql
SELECT
    toUInt256OrNull('256'),
    toUInt256OrNull('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt256OrNull('256'): 256
toUInt256OrNull('abc'): ᴺᵁᴸᴸ
```

**See also**

- [`toUInt256`](#touint256).
- [`toUInt256OrZero`](#touint256orzero).
- [`toUInt256OrDefault`](#touint256ordefault).

## toUInt256OrDefault {#touint256ordefault}

Like [`toUInt256`](#touint256), this function converts an input value to a value of type [UInt256](../data-types/int-uint.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.

**Syntax**

```sql
toUInt256OrDefault(expr[, default])
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).
- `default` (optional) — The default value to return if parsing to type `UInt256` is unsuccessful. [UInt256](../data-types/int-uint.md).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values of type Float32/64.

Arguments for which the default value is returned:
- String representations of Float32/64 values, including `NaN` and `Inf`
- String representations of binary and hexadecimal values, e.g. `SELECT toUInt256OrDefault('0xc0fe', CAST('0', 'UInt256'));`

:::note
If the input value cannot be represented within the bounds of [UInt256](../data-types/int-uint.md), overflow or underflow of the result occurs.
This is not considered an error.
:::

**Returned value**

- 256-bit unsigned integer value if successful, otherwise returns the default value if passed or `0` if not. [UInt256](../data-types/int-uint.md).

:::note
- The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning it truncates fractional digits of numbers.
- The default value type should be the same as the cast type.
:::

**Example**

Query:

```sql
SELECT
    toUInt256OrDefault('-256', CAST('0', 'UInt256')),
    toUInt256OrDefault('abc', CAST('0', 'UInt256'))
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toUInt256OrDefault('-256', CAST('0', 'UInt256')): 0
toUInt256OrDefault('abc', CAST('0', 'UInt256')):  0
```

**See also**

- [`toUInt256`](#touint256).
- [`toUInt256OrZero`](#touint256orzero).
- [`toUInt256OrNull`](#touint256ornull).

## toFloat32 {#tofloat32}

Converts an input value to a value of type [`Float32`](../data-types/float.md). Throws an exception in case of an error.

**Syntax**

```sql
toFloat32(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).

Supported arguments:
- Values of type (U)Int8/16/32/64/128/256.
- String representations of (U)Int8/16/32/128/256.
- Values of type Float32/64, including `NaN` and `Inf`.
- String representations of Float32/64, including `NaN` and `Inf` (case-insensitive).

Unsupported arguments:
- String representations of binary and hexadecimal values, e.g. `SELECT toFloat32('0xc0fe');`.

**Returned value**

- 32-bit floating point value. [Float32](../data-types/float.md).

**Example**

Query:

```sql
SELECT
    toFloat32(42.7),
    toFloat32('42.7'),
    toFloat32('NaN')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toFloat32(42.7):   42.7
toFloat32('42.7'): 42.7
toFloat32('NaN'):  nan
```

**See also**

- [`toFloat32OrZero`](#tofloat32orzero).
- [`toFloat32OrNull`](#tofloat32ornull).
- [`toFloat32OrDefault`](#tofloat32ordefault).

## toFloat32OrZero {#tofloat32orzero}

Like [`toFloat32`](#tofloat32), this function converts an input value to a value of type [Float32](../data-types/float.md) but returns `0` in case of an error.

**Syntax**

```sql
toFloat32OrZero(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256, Float32/64.

Unsupported arguments (return `0`):
- String representations of binary and hexadecimal values, e.g. `SELECT toFloat32OrZero('0xc0fe');`.

**Returned value**

- 32-bit Float value if successful, otherwise `0`. [Float32](../data-types/float.md).

**Example**

Query:

```sql
SELECT
    toFloat32OrZero('42.7'),
    toFloat32OrZero('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toFloat32OrZero('42.7'): 42.7
toFloat32OrZero('abc'):  0
```

**See also**

- [`toFloat32`](#tofloat32).
- [`toFloat32OrNull`](#tofloat32ornull).
- [`toFloat32OrDefault`](#tofloat32ordefault).

## toFloat32OrNull {#tofloat32ornull}

Like [`toFloat32`](#tofloat32), this function converts an input value to a value of type [Float32](../data-types/float.md) but returns `NULL` in case of an error.

**Syntax**

```sql
toFloat32OrNull(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256, Float32/64.

Unsupported arguments (return `\N`):
- String representations of binary and hexadecimal values, e.g. `SELECT toFloat32OrNull('0xc0fe');`.

**Returned value**

- 32-bit Float value if successful, otherwise `\N`. [Float32](../data-types/float.md).

**Example**

Query:

```sql
SELECT
    toFloat32OrNull('42.7'),
    toFloat32OrNull('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toFloat32OrNull('42.7'): 42.7
toFloat32OrNull('abc'):  ᴺᵁᴸᴸ
```

**See also**

- [`toFloat32`](#tofloat32).
- [`toFloat32OrZero`](#tofloat32orzero).
- [`toFloat32OrDefault`](#tofloat32ordefault).

## toFloat32OrDefault {#tofloat32ordefault}

Like [`toFloat32`](#tofloat32), this function converts an input value to a value of type [Float32](../data-types/float.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.

**Syntax**

```sql
toFloat32OrDefault(expr[, default])
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).
- `default` (optional) — The default value to return if parsing to type `Float32` is unsuccessful. [Float32](../data-types/float.md).

Supported arguments:
- Values of type (U)Int8/16/32/64/128/256.
- String representations of (U)Int8/16/32/128/256.
- Values of type Float32/64, including `NaN` and `Inf`.
- String representations of Float32/64, including `NaN` and `Inf` (case-insensitive).

Arguments for which the default value is returned:
- String representations of binary and hexadecimal values, e.g. `SELECT toFloat32OrDefault('0xc0fe', CAST('0', 'Float32'));`.

**Returned value**

- 32-bit Float value if successful, otherwise returns the default value if passed or `0` if not. [Float32](../data-types/float.md).

**Example**

Query:

```sql
SELECT
    toFloat32OrDefault('8', CAST('0', 'Float32')),
    toFloat32OrDefault('abc', CAST('0', 'Float32'))
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toFloat32OrDefault('8', CAST('0', 'Float32')):   8
toFloat32OrDefault('abc', CAST('0', 'Float32')): 0
```

**See also**

- [`toFloat32`](#tofloat32).
- [`toFloat32OrZero`](#tofloat32orzero).
- [`toFloat32OrNull`](#tofloat32ornull).

## toFloat64 {#tofloat64}

Converts an input value to a value of type [`Float64`](../data-types/float.md). Throws an exception in case of an error.

**Syntax**

```sql
toFloat64(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).

Supported arguments:
- Values of type (U)Int8/16/32/64/128/256.
- String representations of (U)Int8/16/32/128/256.
- Values of type Float32/64, including `NaN` and `Inf`.
- String representations of type Float32/64, including `NaN` and `Inf` (case-insensitive).

Unsupported arguments:
- String representations of binary and hexadecimal values, e.g. `SELECT toFloat64('0xc0fe');`.

**Returned value**

- 64-bit floating point value. [Float64](../data-types/float.md).

**Example**

Query:

```sql
SELECT
    toFloat64(42.7),
    toFloat64('42.7'),
    toFloat64('NaN')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toFloat64(42.7):   42.7
toFloat64('42.7'): 42.7
toFloat64('NaN'):  nan
```

**See also**

- [`toFloat64OrZero`](#tofloat64orzero).
- [`toFloat64OrNull`](#tofloat64ornull).
- [`toFloat64OrDefault`](#tofloat64ordefault).

## toFloat64OrZero {#tofloat64orzero}

Like [`toFloat64`](#tofloat64), this function converts an input value to a value of type [Float64](../data-types/float.md) but returns `0` in case of an error.

**Syntax**

```sql
toFloat64OrZero(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256, Float32/64.

Unsupported arguments (return `0`):
- String representations of binary and hexadecimal values, e.g. `SELECT toFloat64OrZero('0xc0fe');`.

**Returned value**

- 64-bit Float value if successful, otherwise `0`. [Float64](../data-types/float.md).

**Example**

Query:

```sql
SELECT
    toFloat64OrZero('42.7'),
    toFloat64OrZero('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toFloat64OrZero('42.7'): 42.7
toFloat64OrZero('abc'):  0
```

**See also**

- [`toFloat64`](#tofloat64).
- [`toFloat64OrNull`](#tofloat64ornull).
- [`toFloat64OrDefault`](#tofloat64ordefault).

## toFloat64OrNull {#tofloat64ornull}

Like [`toFloat64`](#tofloat64), this function converts an input value to a value of type [Float64](../data-types/float.md) but returns `NULL` in case of an error.

**Syntax**

```sql
toFloat64OrNull(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:
- String representations of (U)Int8/16/32/128/256, Float32/64.

Unsupported arguments (return `\N`):
- String representations of binary and hexadecimal values, e.g. `SELECT toFloat64OrNull('0xc0fe');`.

**Returned value**

- 64-bit Float value if successful, otherwise `\N`. [Float64](../data-types/float.md).

**Example**

Query:

```sql
SELECT
    toFloat64OrNull('42.7'),
    toFloat64OrNull('abc')
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toFloat64OrNull('42.7'): 42.7
toFloat64OrNull('abc'):  ᴺᵁᴸᴸ
```

**See also**

- [`toFloat64`](#tofloat64).
- [`toFloat64OrZero`](#tofloat64orzero).
- [`toFloat64OrDefault`](#tofloat64ordefault).

## toFloat64OrDefault {#tofloat64ordefault}

Like [`toFloat64`](#tofloat64), this function converts an input value to a value of type [Float64](../data-types/float.md) but returns the default value in case of an error.
If no `default` value is passed then `0` is returned in case of an error.

**Syntax**

```sql
toFloat64OrDefault(expr[, default])
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions) / [String](../data-types/string.md).
- `default` (optional) — The default value to return if parsing to type `Float64` is unsuccessful. [Float64](../data-types/float.md).

Supported arguments:
- Values of type (U)Int8/16/32/64/128/256.
- String representations of (U)Int8/16/32/128/256.
- Values of type Float32/64, including `NaN` and `Inf`.
- String representations of Float32/64, including `NaN` and `Inf` (case-insensitive).

Arguments for which the default value is returned:
- String representations of binary and hexadecimal values, e.g. `SELECT toFloat64OrDefault('0xc0fe', CAST('0', 'Float64'));`.

**Returned value**

- 64-bit Float value if successful, otherwise returns the default value if passed or `0` if not. [Float64](../data-types/float.md).

**Example**

Query:

```sql
SELECT
    toFloat64OrDefault('8', CAST('0', 'Float64')),
    toFloat64OrDefault('abc', CAST('0', 'Float64'))
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
toFloat64OrDefault('8', CAST('0', 'Float64')):   8
toFloat64OrDefault('abc', CAST('0', 'Float64')): 0
```

**See also**

- [`toFloat64`](#tofloat64).
- [`toFloat64OrZero`](#tofloat64orzero).
- [`toFloat64OrNull`](#tofloat64ornull).

## toBFloat16 {#tobfloat16}

Converts an input value to a value of type [`BFloat16`](/sql-reference/data-types/float#bfloat16). 
Throws an exception in case of an error.

**Syntax**

```sql
toBFloat16(expr)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).

Supported arguments:
- Values of type (U)Int8/16/32/64/128/256.
- String representations of (U)Int8/16/32/128/256.
- Values of type Float32/64, including `NaN` and `Inf`.
- String representations of Float32/64, including `NaN` and `Inf` (case-insensitive).

**Returned value**

- 16-bit brain-float value. [BFloat16](/sql-reference/data-types/float#bfloat16).

**Example**

```sql
SELECT toBFloat16(toFloat32(42.7))

42.5

SELECT toBFloat16(toFloat32('42.7'));

42.5

SELECT toBFloat16('42.7');

42.5
```

**See also**

- [`toBFloat16OrZero`](#tobfloat16orzero).
- [`toBFloat16OrNull`](#tobfloat16ornull).

## toBFloat16OrZero {#tobfloat16orzero}

Converts a String input value to a value of type [`BFloat16`](/sql-reference/data-types/float#bfloat16).
If the string does not represent a floating point value, the function returns zero.

**Syntax**

```sql
toBFloat16OrZero(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:

- String representations of numeric values.

Unsupported arguments (return `0`):

- String representations of binary and hexadecimal values.
- Numeric values.

**Returned value**

- 16-bit brain-float value, otherwise `0`. [BFloat16](/sql-reference/data-types/float#bfloat16).

:::note
The function allows a silent loss of precision while converting from the string representation.
:::

**Example**

```sql
SELECT toBFloat16OrZero('0x5E'); -- unsupported arguments

0

SELECT toBFloat16OrZero('12.3'); -- typical use

12.25

SELECT toBFloat16OrZero('12.3456789');

12.3125 -- silent loss of precision
```

**See also**

- [`toBFloat16`](#tobfloat16).
- [`toBFloat16OrNull`](#tobfloat16ornull).

## toBFloat16OrNull {#tobfloat16ornull}

Converts a String input value to a value of type [`BFloat16`](/sql-reference/data-types/float#bfloat16) 
but if the string does not represent a floating point value, the function returns `NULL`.

**Syntax**

```sql
toBFloat16OrNull(x)
```

**Arguments**

- `x` — A String representation of a number. [String](../data-types/string.md).

Supported arguments:

- String representations of numeric values.

Unsupported arguments (return `NULL`):

- String representations of binary and hexadecimal values.
- Numeric values.

**Returned value**

- 16-bit brain-float value, otherwise `NULL` (`\N`). [BFloat16](/sql-reference/data-types/float#bfloat16).

:::note
The function allows a silent loss of precision while converting from the string representation.
:::

**Example**

```sql
SELECT toBFloat16OrNull('0x5E'); -- unsupported arguments

\N

SELECT toBFloat16OrNull('12.3'); -- typical use

12.25

SELECT toBFloat16OrNull('12.3456789');

12.3125 -- silent loss of precision
```

**See also**

- [`toBFloat16`](#tobfloat16).
- [`toBFloat16OrZero`](#tobfloat16orzero).

## toDate {#todate}

Converts the argument to [Date](../data-types/date.md) data type.

If the argument is [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md), it truncates it and leaves the date component of the DateTime:

```sql
SELECT
    now() AS x,
    toDate(x)
```

```response
┌───────────────────x─┬─toDate(now())─┐
│ 2022-12-30 13:44:17 │    2022-12-30 │
└─────────────────────┴───────────────┘
```

If the argument is a [String](../data-types/string.md), it is parsed as [Date](../data-types/date.md) or [DateTime](../data-types/datetime.md). If it was parsed as [DateTime](../data-types/datetime.md), the date component is being used:

```sql
SELECT
    toDate('2022-12-30') AS x,
    toTypeName(x)
```

```response
┌──────────x─┬─toTypeName(toDate('2022-12-30'))─┐
│ 2022-12-30 │ Date                             │
└────────────┴──────────────────────────────────┘

1 row in set. Elapsed: 0.001 sec.
```

```sql
SELECT
    toDate('2022-12-30 01:02:03') AS x,
    toTypeName(x)
```

```response
┌──────────x─┬─toTypeName(toDate('2022-12-30 01:02:03'))─┐
│ 2022-12-30 │ Date                                      │
└────────────┴───────────────────────────────────────────┘
```

If the argument is a number and looks like a UNIX timestamp (is greater than 65535), it is interpreted as a [DateTime](../data-types/datetime.md), then truncated to [Date](../data-types/date.md) in the current timezone. The timezone argument can be specified as a second argument of the function. The truncation to [Date](../data-types/date.md) depends on the timezone:

```sql
SELECT
    now() AS current_time,
    toUnixTimestamp(current_time) AS ts,
    toDateTime(ts) AS time_Amsterdam,
    toDateTime(ts, 'Pacific/Apia') AS time_Samoa,
    toDate(time_Amsterdam) AS date_Amsterdam,
    toDate(time_Samoa) AS date_Samoa,
    toDate(ts) AS date_Amsterdam_2,
    toDate(ts, 'Pacific/Apia') AS date_Samoa_2
```

```response
Row 1:
──────
current_time:     2022-12-30 13:51:54
ts:               1672404714
time_Amsterdam:   2022-12-30 13:51:54
time_Samoa:       2022-12-31 01:51:54
date_Amsterdam:   2022-12-30
date_Samoa:       2022-12-31
date_Amsterdam_2: 2022-12-30
date_Samoa_2:     2022-12-31
```

The example above demonstrates how the same UNIX timestamp can be interpreted as different dates in different time zones.

If the argument is a number and it is smaller than 65536, it is interpreted as the number of days since 1970-01-01 (the first UNIX day) and converted to [Date](../data-types/date.md). It corresponds to the internal numeric representation of the `Date` data type. Example:

```sql
SELECT toDate(12345)
```
```response
┌─toDate(12345)─┐
│    2003-10-20 │
└───────────────┘
```

This conversion does not depend on timezones.

If the argument does not fit in the range of the Date type, it results in an implementation-defined behavior, that can saturate to the maximum supported date or overflow:
```sql
SELECT toDate(10000000000.)
```
```response
┌─toDate(10000000000.)─┐
│           2106-02-07 │
└──────────────────────┘
```

The function `toDate` can be also written in alternative forms:

```sql
SELECT
    now() AS time,
    toDate(time),
    DATE(time),
    CAST(time, 'Date')
```
```response
┌────────────────time─┬─toDate(now())─┬─DATE(now())─┬─CAST(now(), 'Date')─┐
│ 2022-12-30 13:54:58 │    2022-12-30 │  2022-12-30 │          2022-12-30 │
└─────────────────────┴───────────────┴─────────────┴─────────────────────┘
```


## toDateOrZero {#todateorzero}

The same as [toDate](#todate) but returns lower boundary of [Date](../data-types/date.md) if an invalid argument is received. Only [String](../data-types/string.md) argument is supported.

**Example**

Query:

```sql
SELECT toDateOrZero('2022-12-30'), toDateOrZero('');
```

Result:

```response
┌─toDateOrZero('2022-12-30')─┬─toDateOrZero('')─┐
│                 2022-12-30 │       1970-01-01 │
└────────────────────────────┴──────────────────┘
```


## toDateOrNull {#todateornull}

The same as [toDate](#todate) but returns `NULL` if an invalid argument is received. Only [String](../data-types/string.md) argument is supported.

**Example**

Query:

```sql
SELECT toDateOrNull('2022-12-30'), toDateOrNull('');
```

Result:

```response
┌─toDateOrNull('2022-12-30')─┬─toDateOrNull('')─┐
│                 2022-12-30 │             ᴺᵁᴸᴸ │
└────────────────────────────┴──────────────────┘
```


## toDateOrDefault {#todateordefault}

Like [toDate](#todate) but if unsuccessful, returns a default value which is either the second argument (if specified), or otherwise the lower boundary of [Date](../data-types/date.md).

**Syntax**

```sql
toDateOrDefault(expr [, default_value])
```

**Example**

Query:

```sql
SELECT toDateOrDefault('2022-12-30'), toDateOrDefault('', '2023-01-01'::Date);
```

Result:

```response
┌─toDateOrDefault('2022-12-30')─┬─toDateOrDefault('', CAST('2023-01-01', 'Date'))─┐
│                    2022-12-30 │                                      2023-01-01 │
└───────────────────────────────┴─────────────────────────────────────────────────┘
```


## toDateTime {#todatetime}

Converts an input value to [DateTime](../data-types/datetime.md).

**Syntax**

```sql
toDateTime(expr[, time_zone ])
```

**Arguments**

- `expr` — The value. [String](../data-types/string.md), [Int](../data-types/int-uint.md), [Date](../data-types/date.md) or [DateTime](../data-types/datetime.md).
- `time_zone` — Time zone. [String](../data-types/string.md).

:::note
If `expr` is a number, it is interpreted as the number of seconds since the beginning of the Unix Epoch (as Unix timestamp).
If `expr` is a [String](../data-types/string.md), it may be interpreted as a Unix timestamp or as a string representation of date / date with time.
Thus, parsing of short numbers' string representations (up to 4 digits) is explicitly disabled due to ambiguity, e.g. a string `'1999'` may be both a year (an incomplete string representation of Date / DateTime) or a unix timestamp. Longer numeric strings are allowed.
:::

**Returned value**

- A date time. [DateTime](../data-types/datetime.md)

**Example**

Query:

```sql
SELECT toDateTime('2022-12-30 13:44:17'), toDateTime(1685457500, 'UTC');
```

Result:

```response
┌─toDateTime('2022-12-30 13:44:17')─┬─toDateTime(1685457500, 'UTC')─┐
│               2022-12-30 13:44:17 │           2023-05-30 14:38:20 │
└───────────────────────────────────┴───────────────────────────────┘
```


## toDateTimeOrZero {#todatetimeorzero}

The same as [toDateTime](#todatetime) but returns lower boundary of [DateTime](../data-types/datetime.md) if an invalid argument is received. Only [String](../data-types/string.md) argument is supported.

**Example**

Query:

```sql
SELECT toDateTimeOrZero('2022-12-30 13:44:17'), toDateTimeOrZero('');
```

Result:

```response
┌─toDateTimeOrZero('2022-12-30 13:44:17')─┬─toDateTimeOrZero('')─┐
│                     2022-12-30 13:44:17 │  1970-01-01 00:00:00 │
└─────────────────────────────────────────┴──────────────────────┘
```


## toDateTimeOrNull {#todatetimeornull}

The same as [toDateTime](#todatetime) but returns `NULL` if an invalid argument is received. Only [String](../data-types/string.md) argument is supported.

**Example**

Query:

```sql
SELECT toDateTimeOrNull('2022-12-30 13:44:17'), toDateTimeOrNull('');
```

Result:

```response
┌─toDateTimeOrNull('2022-12-30 13:44:17')─┬─toDateTimeOrNull('')─┐
│                     2022-12-30 13:44:17 │                 ᴺᵁᴸᴸ │
└─────────────────────────────────────────┴──────────────────────┘
```


## toDateTimeOrDefault {#todatetimeordefault}

Like [toDateTime](#todatetime) but if unsuccessful, returns a default value which is either the third argument (if specified), or otherwise the lower boundary of [DateTime](../data-types/datetime.md).

**Syntax**

```sql
toDateTimeOrDefault(expr [, time_zone [, default_value]])
```

**Example**

Query:

```sql
SELECT toDateTimeOrDefault('2022-12-30 13:44:17'), toDateTimeOrDefault('', 'UTC', '2023-01-01'::DateTime('UTC'));
```

Result:

```response
┌─toDateTimeOrDefault('2022-12-30 13:44:17')─┬─toDateTimeOrDefault('', 'UTC', CAST('2023-01-01', 'DateTime(\'UTC\')'))─┐
│                        2022-12-30 13:44:17 │                                                     2023-01-01 00:00:00 │
└────────────────────────────────────────────┴─────────────────────────────────────────────────────────────────────────┘
```


## toDate32 {#todate32}

Converts the argument to the [Date32](../data-types/date32.md) data type. If the value is outside the range, `toDate32` returns the border values supported by [Date32](../data-types/date32.md). If the argument has [Date](../data-types/date.md) type, it's borders are taken into account.

**Syntax**

```sql
toDate32(expr)
```

**Arguments**

- `expr` — The value. [String](../data-types/string.md), [UInt32](../data-types/int-uint.md) or [Date](../data-types/date.md).

**Returned value**

- A calendar date. Type [Date32](../data-types/date32.md).

**Example**

1. The value is within the range:

```sql
SELECT toDate32('1955-01-01') AS value, toTypeName(value);
```

```response
┌──────value─┬─toTypeName(toDate32('1925-01-01'))─┐
│ 1955-01-01 │ Date32                             │
└────────────┴────────────────────────────────────┘
```

2. The value is outside the range:

```sql
SELECT toDate32('1899-01-01') AS value, toTypeName(value);
```

```response
┌──────value─┬─toTypeName(toDate32('1899-01-01'))─┐
│ 1900-01-01 │ Date32                             │
└────────────┴────────────────────────────────────┘
```

3. With [Date](../data-types/date.md) argument:

```sql
SELECT toDate32(toDate('1899-01-01')) AS value, toTypeName(value);
```

```response
┌──────value─┬─toTypeName(toDate32(toDate('1899-01-01')))─┐
│ 1970-01-01 │ Date32                                     │
└────────────┴────────────────────────────────────────────┘
```

## toDate32OrZero {#todate32orzero}

The same as [toDate32](#todate32) but returns the min value of [Date32](../data-types/date32.md) if an invalid argument is received.

**Example**

Query:

```sql
SELECT toDate32OrZero('1899-01-01'), toDate32OrZero('');
```

Result:

```response
┌─toDate32OrZero('1899-01-01')─┬─toDate32OrZero('')─┐
│                   1900-01-01 │         1900-01-01 │
└──────────────────────────────┴────────────────────┘
```

## toDate32OrNull {#todate32ornull}

The same as [toDate32](#todate32) but returns `NULL` if an invalid argument is received.

**Example**

Query:

```sql
SELECT toDate32OrNull('1955-01-01'), toDate32OrNull('');
```

Result:

```response
┌─toDate32OrNull('1955-01-01')─┬─toDate32OrNull('')─┐
│                   1955-01-01 │               ᴺᵁᴸᴸ │
└──────────────────────────────┴────────────────────┘
```

## toDate32OrDefault {#todate32ordefault}

Converts the argument to the [Date32](../data-types/date32.md) data type. If the value is outside the range, `toDate32OrDefault` returns the lower border value supported by [Date32](../data-types/date32.md). If the argument has [Date](../data-types/date.md) type, it's borders are taken into account. Returns default value if an invalid argument is received.

**Example**

Query:

```sql
SELECT
    toDate32OrDefault('1930-01-01', toDate32('2020-01-01')),
    toDate32OrDefault('xx1930-01-01', toDate32('2020-01-01'));
```

Result:

```response
┌─toDate32OrDefault('1930-01-01', toDate32('2020-01-01'))─┬─toDate32OrDefault('xx1930-01-01', toDate32('2020-01-01'))─┐
│                                              1930-01-01 │                                                2020-01-01 │
└─────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────────┘
```

## toDateTime64 {#todatetime64}

Converts an input value to a value of type [DateTime64](../data-types/datetime64.md).

**Syntax**

```sql
toDateTime64(expr, scale, [timezone])
```

**Arguments**

- `expr` — The value. [String](../data-types/string.md), [UInt32](../data-types/int-uint.md), [Float](../data-types/float.md) or [DateTime](../data-types/datetime.md).
- `scale` - Tick size (precision): 10<sup>-precision</sup> seconds. Valid range: [ 0 : 9 ].
- `timezone` (optional) - Time zone of the specified datetime64 object.

**Returned value**

- A calendar date and time of day, with sub-second precision. [DateTime64](../data-types/datetime64.md).

**Example**

1. The value is within the range:

```sql
SELECT toDateTime64('1955-01-01 00:00:00.000', 3) AS value, toTypeName(value);
```

```response
┌───────────────────value─┬─toTypeName(toDateTime64('1955-01-01 00:00:00.000', 3))─┐
│ 1955-01-01 00:00:00.000 │ DateTime64(3)                                          │
└─────────────────────────┴────────────────────────────────────────────────────────┘
```

2. As decimal with precision:

```sql
SELECT toDateTime64(1546300800.000, 3) AS value, toTypeName(value);
```

```response
┌───────────────────value─┬─toTypeName(toDateTime64(1546300800., 3))─┐
│ 2019-01-01 00:00:00.000 │ DateTime64(3)                            │
└─────────────────────────┴──────────────────────────────────────────┘
```

Without the decimal point the value is still treated as Unix Timestamp in seconds:

```sql
SELECT toDateTime64(1546300800000, 3) AS value, toTypeName(value);
```

```response
┌───────────────────value─┬─toTypeName(toDateTime64(1546300800000, 3))─┐
│ 2282-12-31 00:00:00.000 │ DateTime64(3)                              │
└─────────────────────────┴────────────────────────────────────────────┘
```


3. With `timezone`:

```sql
SELECT toDateTime64('2019-01-01 00:00:00', 3, 'Asia/Istanbul') AS value, toTypeName(value);
```

```response
┌───────────────────value─┬─toTypeName(toDateTime64('2019-01-01 00:00:00', 3, 'Asia/Istanbul'))─┐
│ 2019-01-01 00:00:00.000 │ DateTime64(3, 'Asia/Istanbul')                                      │
└─────────────────────────┴─────────────────────────────────────────────────────────────────────┘
```

## toDateTime64OrZero {#todatetime64orzero}

Like [toDateTime64](#todatetime64), this function converts an input value to a value of type [DateTime64](../data-types/datetime64.md) but returns the min value of [DateTime64](../data-types/datetime64.md) if an invalid argument is received.

**Syntax**

```sql
toDateTime64OrZero(expr, scale, [timezone])
```

**Arguments**

- `expr` — The value. [String](../data-types/string.md), [UInt32](../data-types/int-uint.md), [Float](../data-types/float.md) or [DateTime](../data-types/datetime.md).
- `scale` - Tick size (precision): 10<sup>-precision</sup> seconds. Valid range: [ 0 : 9 ].
- `timezone` (optional) - Time zone of the specified DateTime64 object.

**Returned value**

- A calendar date and time of day, with sub-second precision, otherwise the minimum value of `DateTime64`: `1970-01-01 01:00:00.000`. [DateTime64](../data-types/datetime64.md).

**Example**

Query:

```sql
SELECT toDateTime64OrZero('2008-10-12 00:00:00 00:30:30', 3) AS invalid_arg
```

Result:

```response
┌─────────────invalid_arg─┐
│ 1970-01-01 01:00:00.000 │
└─────────────────────────┘
```

**See also**

- [toDateTime64](#todatetime64).
- [toDateTime64OrNull](#todatetime64ornull).
- [toDateTime64OrDefault](#todatetime64ordefault).

## toDateTime64OrNull {#todatetime64ornull}

Like [toDateTime64](#todatetime64), this function converts an input value to a value of type [DateTime64](../data-types/datetime64.md) but returns `NULL` if an invalid argument is received.

**Syntax**

```sql
toDateTime64OrNull(expr, scale, [timezone])
```

**Arguments**

- `expr` — The value. [String](../data-types/string.md), [UInt32](../data-types/int-uint.md), [Float](../data-types/float.md) or [DateTime](../data-types/datetime.md).
- `scale` - Tick size (precision): 10<sup>-precision</sup> seconds. Valid range: [ 0 : 9 ].
- `timezone` (optional) - Time zone of the specified DateTime64 object.

**Returned value**

- A calendar date and time of day, with sub-second precision, otherwise `NULL`. [DateTime64](../data-types/datetime64.md)/[NULL](../data-types/nullable.md).

**Example**

Query:

```sql
SELECT
    toDateTime64OrNull('1976-10-18 00:00:00.30', 3) AS valid_arg,
    toDateTime64OrNull('1976-10-18 00:00:00 30', 3) AS invalid_arg
```

Result:

```response
┌───────────────valid_arg─┬─invalid_arg─┐
│ 1976-10-18 00:00:00.300 │        ᴺᵁᴸᴸ │
└─────────────────────────┴─────────────┘
```

**See also**

- [toDateTime64](#todatetime64).
- [toDateTime64OrZero](#todatetime64orzero).
- [toDateTime64OrDefault](#todatetime64ordefault).

## toDateTime64OrDefault {#todatetime64ordefault}

Like [toDateTime64](#todatetime64), this function converts an input value to a value of type [DateTime64](../data-types/datetime64.md),
but returns either the default value of [DateTime64](../data-types/datetime64.md)
or the provided default if an invalid argument is received.

**Syntax**

```sql
toDateTime64OrNull(expr, scale, [timezone, default])
```

**Arguments**

- `expr` — The value. [String](../data-types/string.md), [UInt32](../data-types/int-uint.md), [Float](../data-types/float.md) or [DateTime](../data-types/datetime.md).
- `scale` - Tick size (precision): 10<sup>-precision</sup> seconds. Valid range: [ 0 : 9 ].
- `timezone` (optional) - Time zone of the specified DateTime64 object.
- `default` (optional) - Default value to return if an invalid argument is received. [DateTime64](../data-types/datetime64.md).

**Returned value**

- A calendar date and time of day, with sub-second precision, otherwise the minimum value of `DateTime64` or the `default` value if provided. [DateTime64](../data-types/datetime64.md).

**Example**

Query:

```sql
SELECT
    toDateTime64OrDefault('1976-10-18 00:00:00 30', 3) AS invalid_arg,
    toDateTime64OrDefault('1976-10-18 00:00:00 30', 3, 'UTC', toDateTime64('2001-01-01 00:00:00.00',3)) AS invalid_arg_with_default
```

Result:

```response
┌─────────────invalid_arg─┬─invalid_arg_with_default─┐
│ 1970-01-01 01:00:00.000 │  2000-12-31 23:00:00.000 │
└─────────────────────────┴──────────────────────────┘
```

**See also**

- [toDateTime64](#todatetime64).
- [toDateTime64OrZero](#todatetime64orzero).
- [toDateTime64OrNull](#todatetime64ornull).

## toDecimal32 {#todecimal32}

Converts an input value to a value of type [`Decimal(9, S)`](../data-types/decimal.md) with scale of `S`. Throws an exception in case of an error.

**Syntax**

```sql
toDecimal32(expr, S)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).
- `S` — Scale parameter between 0 and 9, specifying how many digits the fractional part of a number can have. [UInt8](../data-types/int-uint.md).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values or string representations of type Float32/64.

Unsupported arguments:
- Values or string representations of Float32/64 values `NaN` and `Inf` (case-insensitive).
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal32('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal32`: `( -1 * 10^(9 - S), 1 * 10^(9 - S) )`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an exception.
:::

:::warning
Conversions drop extra digits and could operate in an unexpected way when working with Float32/Float64 inputs as the operations are performed using floating point instructions.
For example: `toDecimal32(1.15, 2)` is equal to `1.14` because 1.15 * 100 in floating point is 114.99.
You can use a String input so the operations use the underlying integer type: `toDecimal32('1.15', 2) = 1.15`
:::

**Returned value**

- Value of type `Decimal(9, S)`. [Decimal32(S)](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT
    toDecimal32(2, 1) AS a, toTypeName(a) AS type_a,
    toDecimal32(4.2, 2) AS b, toTypeName(b) AS type_b,
    toDecimal32('4.2', 3) AS c, toTypeName(c) AS type_c
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
a:      2
type_a: Decimal(9, 1)
b:      4.2
type_b: Decimal(9, 2)
c:      4.2
type_c: Decimal(9, 3)
```

**See also**

- [`toDecimal32OrZero`](#todecimal32orzero).
- [`toDecimal32OrNull`](#todecimal32ornull).
- [`toDecimal32OrDefault`](#todecimal32ordefault).

## toDecimal32OrZero {#todecimal32orzero}

Like [`toDecimal32`](#todecimal32), this function converts an input value to a value of type [Decimal(9, S)](../data-types/decimal.md) but returns `0` in case of an error.

**Syntax**

```sql
toDecimal32OrZero(expr, S)
```

**Arguments**

- `expr` — A String representation of a number. [String](../data-types/string.md).
- `S` — Scale parameter between 0 and 9, specifying how many digits the fractional part of a number can have. [UInt8](../data-types/int-uint.md).

Supported arguments:
- String representations of type (U)Int8/16/32/64/128/256.
- String representations of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal32OrZero('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal32`: `( -1 * 10^(9 - S), 1 * 10^(9 - S) )`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an error.
:::

**Returned value**

- Value of type `Decimal(9, S)` if successful, otherwise `0` with `S` decimal places. [Decimal32(S)](../data-types/decimal.md).

**Example**

Query:

```sql
SELECT
    toDecimal32OrZero(toString(-1.111), 5) AS a,
    toTypeName(a),
    toDecimal32OrZero(toString('Inf'), 5) as b,
    toTypeName(b)
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
a:             -1.111
toTypeName(a): Decimal(9, 5)
b:             0
toTypeName(b): Decimal(9, 5)
```

**See also**

- [`toDecimal32`](#todecimal32).
- [`toDecimal32OrNull`](#todecimal32ornull).
- [`toDecimal32OrDefault`](#todecimal32ordefault).

## toDecimal32OrNull {#todecimal32ornull}

Like [`toDecimal32`](#todecimal32), this function converts an input value to a value of type [Nullable(Decimal(9, S))](../data-types/decimal.md) but returns `0` in case of an error.

**Syntax**

```sql
toDecimal32OrNull(expr, S)
```

**Arguments**

- `expr` — A String representation of a number. [String](../data-types/string.md).
- `S` — Scale parameter between 0 and 9, specifying how many digits the fractional part of a number can have. [UInt8](../data-types/int-uint.md).

Supported arguments:
- String representations of type (U)Int8/16/32/64/128/256.
- String representations of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal32OrNull('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal32`: `( -1 * 10^(9 - S), 1 * 10^(9 - S) )`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an error.
:::

**Returned value**

- Value of type `Nullable(Decimal(9, S))` if successful, otherwise value `NULL` of the same type. [Decimal32(S)](../data-types/decimal.md).

**Examples**

Query:

```sql
SELECT
    toDecimal32OrNull(toString(-1.111), 5) AS a,
    toTypeName(a),
    toDecimal32OrNull(toString('Inf'), 5) as b,
    toTypeName(b)
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
a:             -1.111
toTypeName(a): Nullable(Decimal(9, 5))
b:             ᴺᵁᴸᴸ
toTypeName(b): Nullable(Decimal(9, 5))
```

**See also**

- [`toDecimal32`](#todecimal32).
- [`toDecimal32OrZero`](#todecimal32orzero).
- [`toDecimal32OrDefault`](#todecimal32ordefault).

## toDecimal32OrDefault {#todecimal32ordefault}

Like [`toDecimal32`](#todecimal32), this function converts an input value to a value of type [Decimal(9, S)](../data-types/decimal.md) but returns the default value in case of an error.

**Syntax**

```sql
toDecimal32OrDefault(expr, S[, default])
```

**Arguments**

- `expr` — A String representation of a number. [String](../data-types/string.md).
- `S` — Scale parameter between 0 and 9, specifying how many digits the fractional part of a number can have. [UInt8](../data-types/int-uint.md).
- `default` (optional) — The default value to return if parsing to type `Decimal32(S)` is unsuccessful. [Decimal32(S)](../data-types/decimal.md).

Supported arguments:
- String representations of type (U)Int8/16/32/64/128/256.
- String representations of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal32OrDefault('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal32`: `( -1 * 10^(9 - S), 1 * 10^(9 - S) )`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an error.
:::

:::warning
Conversions drop extra digits and could operate in an unexpected way when working with Float32/Float64 inputs as the operations are performed using floating point instructions.
For example: `toDecimal32OrDefault(1.15, 2)` is equal to `1.14` because 1.15 * 100 in floating point is 114.99.
You can use a String input so the operations use the underlying integer type: `toDecimal32OrDefault('1.15', 2) = 1.15`
:::

**Returned value**

- Value of type `Decimal(9, S)` if successful, otherwise returns the default value if passed or `0` if not. [Decimal32(S)](../data-types/decimal.md).

**Examples**

Query:

```sql
SELECT
    toDecimal32OrDefault(toString(0.0001), 5) AS a,
    toTypeName(a),
    toDecimal32OrDefault('Inf', 0, CAST('-1', 'Decimal32(0)')) AS b,
    toTypeName(b)
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
a:             0.0001
toTypeName(a): Decimal(9, 5)
b:             -1
toTypeName(b): Decimal(9, 0)
```

**See also**

- [`toDecimal32`](#todecimal32).
- [`toDecimal32OrZero`](#todecimal32orzero).
- [`toDecimal32OrNull`](#todecimal32ornull).

## toDecimal64 {#todecimal64}

Converts an input value to a value of type [`Decimal(18, S)`](../data-types/decimal.md) with scale of `S`. Throws an exception in case of an error.

**Syntax**

```sql
toDecimal64(expr, S)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).
- `S` — Scale parameter between 0 and 18, specifying how many digits the fractional part of a number can have. [UInt8](../data-types/int-uint.md).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values or string representations of type Float32/64.

Unsupported arguments:
- Values or string representations of Float32/64 values `NaN` and `Inf` (case-insensitive).
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal64('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal64`: `( -1 * 10^(18 - S), 1 * 10^(18 - S) )`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an exception.
:::

:::warning
Conversions drop extra digits and could operate in an unexpected way when working with Float32/Float64 inputs as the operations are performed using floating point instructions.
For example: `toDecimal64(1.15, 2)` is equal to `1.14` because 1.15 * 100 in floating point is 114.99.
You can use a String input so the operations use the underlying integer type: `toDecimal64('1.15', 2) = 1.15`
:::

**Returned value**

- Value of type `Decimal(18, S)`. [Decimal64(S)](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT
    toDecimal64(2, 1) AS a, toTypeName(a) AS type_a,
    toDecimal64(4.2, 2) AS b, toTypeName(b) AS type_b,
    toDecimal64('4.2', 3) AS c, toTypeName(c) AS type_c
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
a:      2
type_a: Decimal(18, 1)
b:      4.2
type_b: Decimal(18, 2)
c:      4.2
type_c: Decimal(18, 3)
```

**See also**

- [`toDecimal64OrZero`](#todecimal64orzero).
- [`toDecimal64OrNull`](#todecimal64ornull).
- [`toDecimal64OrDefault`](#todecimal64ordefault).

## toDecimal64OrZero {#todecimal64orzero}

Like [`toDecimal64`](#todecimal64), this function converts an input value to a value of type [Decimal(18, S)](../data-types/decimal.md) but returns `0` in case of an error.

**Syntax**

```sql
toDecimal64OrZero(expr, S)
```

**Arguments**

- `expr` — A String representation of a number. [String](../data-types/string.md).
- `S` — Scale parameter between 0 and 18, specifying how many digits the fractional part of a number can have. [UInt8](../data-types/int-uint.md).

Supported arguments:
- String representations of type (U)Int8/16/32/64/128/256.
- String representations of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal64OrZero('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal64`: `( -1 * 10^(18 - S), 1 * 10^(18 - S) )`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an error.
:::

**Returned value**

- Value of type `Decimal(18, S)` if successful, otherwise `0` with `S` decimal places. [Decimal64(S)](../data-types/decimal.md).

**Example**

Query:

```sql
SELECT
    toDecimal64OrZero(toString(0.0001), 18) AS a,
    toTypeName(a),
    toDecimal64OrZero(toString('Inf'), 18) as b,
    toTypeName(b)
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
a:             0.0001
toTypeName(a): Decimal(18, 18)
b:             0
toTypeName(b): Decimal(18, 18)
```

**See also**

- [`toDecimal64`](#todecimal64).
- [`toDecimal64OrNull`](#todecimal64ornull).
- [`toDecimal64OrDefault`](#todecimal64ordefault).

## toDecimal64OrNull {#todecimal64ornull}

Like [`toDecimal64`](#todecimal64), this function converts an input value to a value of type [Nullable(Decimal(18, S))](../data-types/decimal.md) but returns `0` in case of an error.

**Syntax**

```sql
toDecimal64OrNull(expr, S)
```

**Arguments**

- `expr` — A String representation of a number. [String](../data-types/string.md).
- `S` — Scale parameter between 0 and 18, specifying how many digits the fractional part of a number can have. [UInt8](../data-types/int-uint.md).

Supported arguments:
- String representations of type (U)Int8/16/32/64/128/256.
- String representations of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal64OrNull('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal64`: `( -1 * 10^(18 - S), 1 * 10^(18 - S) )`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an error.
:::

**Returned value**

- Value of type `Nullable(Decimal(18, S))` if successful, otherwise value `NULL` of the same type. [Decimal64(S)](../data-types/decimal.md).

**Examples**

Query:

```sql
SELECT
    toDecimal64OrNull(toString(0.0001), 18) AS a,
    toTypeName(a),
    toDecimal64OrNull(toString('Inf'), 18) as b,
    toTypeName(b)
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
a:             0.0001
toTypeName(a): Nullable(Decimal(18, 18))
b:             ᴺᵁᴸᴸ
toTypeName(b): Nullable(Decimal(18, 18))
```

**See also**

- [`toDecimal64`](#todecimal64).
- [`toDecimal64OrZero`](#todecimal64orzero).
- [`toDecimal64OrDefault`](#todecimal64ordefault).

## toDecimal64OrDefault {#todecimal64ordefault}

Like [`toDecimal64`](#todecimal64), this function converts an input value to a value of type [Decimal(18, S)](../data-types/decimal.md) but returns the default value in case of an error.

**Syntax**

```sql
toDecimal64OrDefault(expr, S[, default])
```

**Arguments**

- `expr` — A String representation of a number. [String](../data-types/string.md).
- `S` — Scale parameter between 0 and 18, specifying how many digits the fractional part of a number can have. [UInt8](../data-types/int-uint.md).
- `default` (optional) — The default value to return if parsing to type `Decimal64(S)` is unsuccessful. [Decimal64(S)](../data-types/decimal.md).

Supported arguments:
- String representations of type (U)Int8/16/32/64/128/256.
- String representations of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal64OrDefault('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal64`: `( -1 * 10^(18 - S), 1 * 10^(18 - S) )`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an error.
:::

:::warning
Conversions drop extra digits and could operate in an unexpected way when working with Float32/Float64 inputs as the operations are performed using floating point instructions.
For example: `toDecimal64OrDefault(1.15, 2)` is equal to `1.14` because 1.15 * 100 in floating point is 114.99.
You can use a String input so the operations use the underlying integer type: `toDecimal64OrDefault('1.15', 2) = 1.15`
:::

**Returned value**

- Value of type `Decimal(18, S)` if successful, otherwise returns the default value if passed or `0` if not. [Decimal64(S)](../data-types/decimal.md).

**Examples**

Query:

```sql
SELECT
    toDecimal64OrDefault(toString(0.0001), 18) AS a,
    toTypeName(a),
    toDecimal64OrDefault('Inf', 0, CAST('-1', 'Decimal64(0)')) AS b,
    toTypeName(b)
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
a:             0.0001
toTypeName(a): Decimal(18, 18)
b:             -1
toTypeName(b): Decimal(18, 0)
```

**See also**

- [`toDecimal64`](#todecimal64).
- [`toDecimal64OrZero`](#todecimal64orzero).
- [`toDecimal64OrNull`](#todecimal64ornull).

## toDecimal128 {#todecimal128}

Converts an input value to a value of type [`Decimal(38, S)`](../data-types/decimal.md) with scale of `S`. Throws an exception in case of an error.

**Syntax**

```sql
toDecimal128(expr, S)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).
- `S` — Scale parameter between 0 and 38, specifying how many digits the fractional part of a number can have. [UInt8](../data-types/int-uint.md).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values or string representations of type Float32/64.

Unsupported arguments:
- Values or string representations of Float32/64 values `NaN` and `Inf` (case-insensitive).
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal128('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal128`: `( -1 * 10^(38 - S), 1 * 10^(38 - S) )`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an exception.
:::

:::warning
Conversions drop extra digits and could operate in an unexpected way when working with Float32/Float64 inputs as the operations are performed using floating point instructions.
For example: `toDecimal128(1.15, 2)` is equal to `1.14` because 1.15 * 100 in floating point is 114.99.
You can use a String input so the operations use the underlying integer type: `toDecimal128('1.15', 2) = 1.15`
:::

**Returned value**

- Value of type `Decimal(38, S)`. [Decimal128(S)](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT
    toDecimal128(99, 1) AS a, toTypeName(a) AS type_a,
    toDecimal128(99.67, 2) AS b, toTypeName(b) AS type_b,
    toDecimal128('99.67', 3) AS c, toTypeName(c) AS type_c
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
a:      99
type_a: Decimal(38, 1)
b:      99.67
type_b: Decimal(38, 2)
c:      99.67
type_c: Decimal(38, 3)
```

**See also**

- [`toDecimal128OrZero`](#todecimal128orzero).
- [`toDecimal128OrNull`](#todecimal128ornull).
- [`toDecimal128OrDefault`](#todecimal128ordefault).

## toDecimal128OrZero {#todecimal128orzero}

Like [`toDecimal128`](#todecimal128), this function converts an input value to a value of type [Decimal(38, S)](../data-types/decimal.md) but returns `0` in case of an error.

**Syntax**

```sql
toDecimal128OrZero(expr, S)
```

**Arguments**

- `expr` — A String representation of a number. [String](../data-types/string.md).
- `S` — Scale parameter between 0 and 38, specifying how many digits the fractional part of a number can have. [UInt8](../data-types/int-uint.md).

Supported arguments:
- String representations of type (U)Int8/16/32/64/128/256.
- String representations of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal128OrZero('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal128`: `( -1 * 10^(38 - S), 1 * 10^(38 - S) )`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an error.
:::

**Returned value**

- Value of type `Decimal(38, S)` if successful, otherwise `0` with `S` decimal places. [Decimal128(S)](../data-types/decimal.md).

**Example**

Query:

```sql
SELECT
    toDecimal128OrZero(toString(0.0001), 38) AS a,
    toTypeName(a),
    toDecimal128OrZero(toString('Inf'), 38) as b,
    toTypeName(b)
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
a:             0.0001
toTypeName(a): Decimal(38, 38)
b:             0
toTypeName(b): Decimal(38, 38)
```

**See also**

- [`toDecimal128`](#todecimal128).
- [`toDecimal128OrNull`](#todecimal128ornull).
- [`toDecimal128OrDefault`](#todecimal128ordefault).

## toDecimal128OrNull {#todecimal128ornull}

Like [`toDecimal128`](#todecimal128), this function converts an input value to a value of type [Nullable(Decimal(38, S))](../data-types/decimal.md) but returns `0` in case of an error.

**Syntax**

```sql
toDecimal128OrNull(expr, S)
```

**Arguments**

- `expr` — A String representation of a number. [String](../data-types/string.md).
- `S` — Scale parameter between 0 and 38, specifying how many digits the fractional part of a number can have. [UInt8](../data-types/int-uint.md).

Supported arguments:
- String representations of type (U)Int8/16/32/64/128/256.
- String representations of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal128OrNull('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal128`: `( -1 * 10^(38 - S), 1 * 10^(38 - S) )`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an error.
:::

**Returned value**

- Value of type `Nullable(Decimal(38, S))` if successful, otherwise value `NULL` of the same type. [Decimal128(S)](../data-types/decimal.md).

**Examples**

Query:

```sql
SELECT
    toDecimal128OrNull(toString(1/42), 38) AS a,
    toTypeName(a),
    toDecimal128OrNull(toString('Inf'), 38) as b,
    toTypeName(b)
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
a:             0.023809523809523808
toTypeName(a): Nullable(Decimal(38, 38))
b:             ᴺᵁᴸᴸ
toTypeName(b): Nullable(Decimal(38, 38))
```

**See also**

- [`toDecimal128`](#todecimal128).
- [`toDecimal128OrZero`](#todecimal128orzero).
- [`toDecimal128OrDefault`](#todecimal128ordefault).

## toDecimal128OrDefault {#todecimal128ordefault}

Like [`toDecimal128`](#todecimal128), this function converts an input value to a value of type [Decimal(38, S)](../data-types/decimal.md) but returns the default value in case of an error.

**Syntax**

```sql
toDecimal128OrDefault(expr, S[, default])
```

**Arguments**

- `expr` — A String representation of a number. [String](../data-types/string.md).
- `S` — Scale parameter between 0 and 38, specifying how many digits the fractional part of a number can have. [UInt8](../data-types/int-uint.md).
- `default` (optional) — The default value to return if parsing to type `Decimal128(S)` is unsuccessful. [Decimal128(S)](../data-types/decimal.md).

Supported arguments:
- String representations of type (U)Int8/16/32/64/128/256.
- String representations of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal128OrDefault('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal128`: `( -1 * 10^(38 - S), 1 * 10^(38 - S) )`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an error.
:::

:::warning
Conversions drop extra digits and could operate in an unexpected way when working with Float32/Float64 inputs as the operations are performed using floating point instructions.
For example: `toDecimal128OrDefault(1.15, 2)` is equal to `1.14` because 1.15 * 100 in floating point is 114.99.
You can use a String input so the operations use the underlying integer type: `toDecimal128OrDefault('1.15', 2) = 1.15`
:::

**Returned value**

- Value of type `Decimal(38, S)` if successful, otherwise returns the default value if passed or `0` if not. [Decimal128(S)](../data-types/decimal.md).

**Examples**

Query:

```sql
SELECT
    toDecimal128OrDefault(toString(1/42), 18) AS a,
    toTypeName(a),
    toDecimal128OrDefault('Inf', 0, CAST('-1', 'Decimal128(0)')) AS b,
    toTypeName(b)
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
a:             0.023809523809523808
toTypeName(a): Decimal(38, 18)
b:             -1
toTypeName(b): Decimal(38, 0)
```

**See also**

- [`toDecimal128`](#todecimal128).
- [`toDecimal128OrZero`](#todecimal128orzero).
- [`toDecimal128OrNull`](#todecimal128ornull).

## toDecimal256 {#todecimal256}

Converts an input value to a value of type [`Decimal(76, S)`](../data-types/decimal.md) with scale of `S`. Throws an exception in case of an error.

**Syntax**

```sql
toDecimal256(expr, S)
```

**Arguments**

- `expr` — Expression returning a number or a string representation of a number. [Expression](/sql-reference/syntax#expressions).
- `S` — Scale parameter between 0 and 76, specifying how many digits the fractional part of a number can have. [UInt8](../data-types/int-uint.md).

Supported arguments:
- Values or string representations of type (U)Int8/16/32/64/128/256.
- Values or string representations of type Float32/64.

Unsupported arguments:
- Values or string representations of Float32/64 values `NaN` and `Inf` (case-insensitive).
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal256('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal256`: `( -1 * 10^(76 - S), 1 * 10^(76 - S) )`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an exception.
:::

:::warning
Conversions drop extra digits and could operate in an unexpected way when working with Float32/Float64 inputs as the operations are performed using floating point instructions.
For example: `toDecimal256(1.15, 2)` is equal to `1.14` because 1.15 * 100 in floating point is 114.99.
You can use a String input so the operations use the underlying integer type: `toDecimal256('1.15', 2) = 1.15`
:::

**Returned value**

- Value of type `Decimal(76, S)`. [Decimal256(S)](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT
    toDecimal256(99, 1) AS a, toTypeName(a) AS type_a,
    toDecimal256(99.67, 2) AS b, toTypeName(b) AS type_b,
    toDecimal256('99.67', 3) AS c, toTypeName(c) AS type_c
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
a:      99
type_a: Decimal(76, 1)
b:      99.67
type_b: Decimal(76, 2)
c:      99.67
type_c: Decimal(76, 3)
```

**See also**

- [`toDecimal256OrZero`](#todecimal256orzero).
- [`toDecimal256OrNull`](#todecimal256ornull).
- [`toDecimal256OrDefault`](#todecimal256ordefault).

## toDecimal256OrZero {#todecimal256orzero}

Like [`toDecimal256`](#todecimal256), this function converts an input value to a value of type [Decimal(76, S)](../data-types/decimal.md) but returns `0` in case of an error.

**Syntax**

```sql
toDecimal256OrZero(expr, S)
```

**Arguments**

- `expr` — A String representation of a number. [String](../data-types/string.md).
- `S` — Scale parameter between 0 and 76, specifying how many digits the fractional part of a number can have. [UInt8](../data-types/int-uint.md).

Supported arguments:
- String representations of type (U)Int8/16/32/64/128/256.
- String representations of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal256OrZero('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal256`: `( -1 * 10^(76 - S), 1 * 10^(76 - S) )`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an error.
:::

**Returned value**

- Value of type `Decimal(76, S)` if successful, otherwise `0` with `S` decimal places. [Decimal256(S)](../data-types/decimal.md).

**Example**

Query:

```sql
SELECT
    toDecimal256OrZero(toString(0.0001), 76) AS a,
    toTypeName(a),
    toDecimal256OrZero(toString('Inf'), 76) as b,
    toTypeName(b)
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
a:             0.0001
toTypeName(a): Decimal(76, 76)
b:             0
toTypeName(b): Decimal(76, 76)
```

**See also**

- [`toDecimal256`](#todecimal256).
- [`toDecimal256OrNull`](#todecimal256ornull).
- [`toDecimal256OrDefault`](#todecimal256ordefault).

## toDecimal256OrNull {#todecimal256ornull}

Like [`toDecimal256`](#todecimal256), this function converts an input value to a value of type [Nullable(Decimal(76, S))](../data-types/decimal.md) but returns `0` in case of an error.

**Syntax**

```sql
toDecimal256OrNull(expr, S)
```

**Arguments**

- `expr` — A String representation of a number. [String](../data-types/string.md).
- `S` — Scale parameter between 0 and 76, specifying how many digits the fractional part of a number can have. [UInt8](../data-types/int-uint.md).

Supported arguments:
- String representations of type (U)Int8/16/32/64/128/256.
- String representations of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal256OrNull('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal256`: `( -1 * 10^(76 - S), 1 * 10^(76 - S) )`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an error.
:::

**Returned value**

- Value of type `Nullable(Decimal(76, S))` if successful, otherwise value `NULL` of the same type. [Decimal256(S)](../data-types/decimal.md).

**Examples**

Query:

```sql
SELECT
    toDecimal256OrNull(toString(1/42), 76) AS a,
    toTypeName(a),
    toDecimal256OrNull(toString('Inf'), 76) as b,
    toTypeName(b)
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
a:             0.023809523809523808
toTypeName(a): Nullable(Decimal(76, 76))
b:             ᴺᵁᴸᴸ
toTypeName(b): Nullable(Decimal(76, 76))
```

**See also**

- [`toDecimal256`](#todecimal256).
- [`toDecimal256OrZero`](#todecimal256orzero).
- [`toDecimal256OrDefault`](#todecimal256ordefault).

## toDecimal256OrDefault {#todecimal256ordefault}

Like [`toDecimal256`](#todecimal256), this function converts an input value to a value of type [Decimal(76, S)](../data-types/decimal.md) but returns the default value in case of an error.

**Syntax**

```sql
toDecimal256OrDefault(expr, S[, default])
```

**Arguments**

- `expr` — A String representation of a number. [String](../data-types/string.md).
- `S` — Scale parameter between 0 and 76, specifying how many digits the fractional part of a number can have. [UInt8](../data-types/int-uint.md).
- `default` (optional) — The default value to return if parsing to type `Decimal256(S)` is unsuccessful. [Decimal256(S)](../data-types/decimal.md).

Supported arguments:
- String representations of type (U)Int8/16/32/64/128/256.
- String representations of type Float32/64.

Unsupported arguments:
- String representations of Float32/64 values `NaN` and `Inf`.
- String representations of binary and hexadecimal values, e.g. `SELECT toDecimal256OrDefault('0xc0fe', 1);`.

:::note
An overflow can occur if the value of `expr` exceeds the bounds of `Decimal256`: `( -1 * 10^(76 - S), 1 * 10^(76 - S) )`.
Excessive digits in a fraction are discarded (not rounded).
Excessive digits in the integer part will lead to an error.
:::

:::warning
Conversions drop extra digits and could operate in an unexpected way when working with Float32/Float64 inputs as the operations are performed using floating point instructions.
For example: `toDecimal256OrDefault(1.15, 2)` is equal to `1.14` because 1.15 * 100 in floating point is 114.99.
You can use a String input so the operations use the underlying integer type: `toDecimal256OrDefault('1.15', 2) = 1.15`
:::

**Returned value**

- Value of type `Decimal(76, S)` if successful, otherwise returns the default value if passed or `0` if not. [Decimal256(S)](../data-types/decimal.md).

**Examples**

Query:

```sql
SELECT
    toDecimal256OrDefault(toString(1/42), 76) AS a,
    toTypeName(a),
    toDecimal256OrDefault('Inf', 0, CAST('-1', 'Decimal256(0)')) AS b,
    toTypeName(b)
FORMAT Vertical;
```

Result:

```response
Row 1:
──────
a:             0.023809523809523808
toTypeName(a): Decimal(76, 76)
b:             -1
toTypeName(b): Decimal(76, 0)
```

**See also**

- [`toDecimal256`](#todecimal256).
- [`toDecimal256OrZero`](#todecimal256orzero).
- [`toDecimal256OrNull`](#todecimal256ornull).

## toString {#tostring}

Functions for converting between numbers, strings (but not fixed strings), dates, and dates with times.
All these functions accept one argument.

When converting to or from a string, the value is formatted or parsed using the same rules as for the TabSeparated format (and almost all other text formats). If the string can't be parsed, an exception is thrown and the request is canceled.

When converting dates to numbers or vice versa, the date corresponds to the number of days since the beginning of the Unix epoch.
When converting dates with times to numbers or vice versa, the date with time corresponds to the number of seconds since the beginning of the Unix epoch.

The date and date-with-time formats for the toDate/toDateTime functions are defined as follows:
=======
The date and date-with-time formats for the `toDate`/`toDateTime` functions are defined as follows:
>>>>>>> origin/master

```response
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

As an exception, if converting from UInt32, Int32, UInt64, or Int64 numeric types to Date, and if the number is greater than or equal to 65536, the number is interpreted as a Unix timestamp (and not as the number of days) and is rounded to the date.
This allows support for the common occurrence of writing `toDate(unix_timestamp)`, which otherwise would be an error and would require writing the more cumbersome `toDate(toDateTime(unix_timestamp))`.

Conversion between a date and a date with time is performed the natural way: by adding a null time or dropping the time.

Conversion between numeric types uses the same rules as assignments between different numeric types in C++.

**Example**


```sql title="Query"
SELECT
    now() AS ts,
    time_zone,
    toString(ts, time_zone) AS str_tz_datetime
FROM system.time_zones
WHERE time_zone LIKE 'Europe%'
LIMIT 10
```


```response title="Response"
┌──────────────────ts─┬─time_zone─────────┬─str_tz_datetime─────┐
│ 2023-09-08 19:14:59 │ Europe/Amsterdam  │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Andorra    │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Astrakhan  │ 2023-09-08 23:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Athens     │ 2023-09-08 22:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Belfast    │ 2023-09-08 20:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Belgrade   │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Berlin     │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Bratislava │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Brussels   │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Bucharest  │ 2023-09-08 22:14:59 │
└─────────────────────┴───────────────────┴─────────────────────┘
```

Also see the [`toUnixTimestamp`](/sql-reference/functions/date-time-functions#toUnixTimestamp) function.

<!-- 
The inner content of the tags below are replaced at doc framework build time with 
docs generated from system.functions. Please do not modify or remove the tags.
See: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
-->

<!--AUTOGENERATED_START-->
<!--AUTOGENERATED_END-->
