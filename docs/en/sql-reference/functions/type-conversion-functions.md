---
slug: /en/sql-reference/functions/type-conversion-functions
sidebar_position: 185
sidebar_label: Type Conversion
---

# Type Conversion Functions

## Common Issues with Data Conversion

ClickHouse generally uses the [same behavior as C++ programs](https://en.cppreference.com/w/cpp/language/implicit_conversion).

`to<type>` functions and [cast](#castx-t) behave differently in some cases, for example in case of [LowCardinality](../data-types/lowcardinality.md): [cast](#castx-t) removes [LowCardinality](../data-types/lowcardinality.md) trait `to<type>` functions don't. The same with [Nullable](../data-types/nullable.md), this behaviour is not compatible with SQL standard, and it can be changed using [cast_keep_nullable](../../operations/settings/settings.md/#cast_keep_nullable) setting.

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

## toInt(8\|16\|32\|64\|128\|256)

Converts an input value to a value the [Int](/docs/en/sql-reference/data-types/int-uint.md) data type. This function family includes:

- `toInt8(expr)` — Converts to a value of data type `Int8`.
- `toInt16(expr)` — Converts to a value of data type `Int16`.
- `toInt32(expr)` — Converts to a value of data type `Int32`.
- `toInt64(expr)` — Converts to a value of data type `Int64`.
- `toInt128(expr)` — Converts to a value of data type `Int128`.
- `toInt256(expr)` — Converts to a value of data type `Int256`.

**Arguments**

- `expr` — [Expression](/docs/en/sql-reference/syntax.md/#syntax-expressions) returning a number or a string with the decimal representation of a number. Binary, octal, and hexadecimal representations of numbers are not supported. Leading zeroes are stripped.

**Returned value**

Integer value in the `Int8`, `Int16`, `Int32`, `Int64`, `Int128` or `Int256` data type.

Functions use [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning they truncate fractional digits of numbers.

The behavior of functions for the [NaN and Inf](/docs/en/sql-reference/data-types/float.md/#data_type-float-nan-inf) arguments is undefined. Remember about [numeric conversions issues](#numeric-conversion-issues), when using the functions.

**Example**

Query:

``` sql
SELECT toInt64(nan), toInt32(32), toInt16('16'), toInt8(8.8);
```

Result:

```response
┌─────────toInt64(nan)─┬─toInt32(32)─┬─toInt16('16')─┬─toInt8(8.8)─┐
│ -9223372036854775808 │          32 │            16 │           8 │
└──────────────────────┴─────────────┴───────────────┴─────────────┘
```

## toInt(8\|16\|32\|64\|128\|256)OrZero

Takes an argument of type [String](/docs/en/sql-reference/data-types/string.md) and tries to parse it into an Int (8 \| 16 \| 32 \| 64 \| 128 \| 256). If unsuccessful, returns `0`.

**Example**

Query:

``` sql
SELECT toInt64OrZero('123123'), toInt8OrZero('123qwe123');
```

Result:

```response
┌─toInt64OrZero('123123')─┬─toInt8OrZero('123qwe123')─┐
│                  123123 │                         0 │
└─────────────────────────┴───────────────────────────┘
```

## toInt(8\|16\|32\|64\|128\|256)OrNull

It takes an argument of type String and tries to parse it into Int (8 \| 16 \| 32 \| 64 \| 128 \| 256). If unsuccessful, returns `NULL`.

**Example**

Query:

``` sql
SELECT toInt64OrNull('123123'), toInt8OrNull('123qwe123');
```

Result:

```response
┌─toInt64OrNull('123123')─┬─toInt8OrNull('123qwe123')─┐
│                  123123 │                      ᴺᵁᴸᴸ │
└─────────────────────────┴───────────────────────────┘
```

## toInt(8\|16\|32\|64\|128\|256)OrDefault

It takes an argument of type String and tries to parse it into Int (8 \| 16 \| 32 \| 64 \| 128 \| 256). If unsuccessful, returns the default type value.

**Example**

Query:

``` sql
SELECT toInt64OrDefault('123123', cast('-1' as Int64)), toInt8OrDefault('123qwe123', cast('-1' as Int8));
```

Result:

```response
┌─toInt64OrDefault('123123', CAST('-1', 'Int64'))─┬─toInt8OrDefault('123qwe123', CAST('-1', 'Int8'))─┐
│                                          123123 │                                               -1 │
└─────────────────────────────────────────────────┴──────────────────────────────────────────────────┘
```


## toUInt(8\|16\|32\|64\|256)

Converts an input value to the [UInt](/docs/en/sql-reference/data-types/int-uint.md) data type. This function family includes:

- `toUInt8(expr)` — Converts to a value of data type `UInt8`.
- `toUInt16(expr)` — Converts to a value of data type `UInt16`.
- `toUInt32(expr)` — Converts to a value of data type `UInt32`.
- `toUInt64(expr)` — Converts to a value of data type `UInt64`.
- `toUInt256(expr)` — Converts to a value of data type `UInt256`.

**Arguments**

- `expr` — [Expression](/docs/en/sql-reference/syntax.md/#syntax-expressions) returning a number or a string with the decimal representation of a number. Binary, octal, and hexadecimal representations of numbers are not supported. Leading zeroes are stripped.

**Returned value**

- Integer value in the `UInt8`, `UInt16`, `UInt32`, `UInt64` or `UInt256` data type.

Functions use [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning they truncate fractional digits of numbers.

The behavior of functions for negative arguments and for the [NaN and Inf](/docs/en/sql-reference/data-types/float.md/#data_type-float-nan-inf) arguments is undefined. If you pass a string with a negative number, for example `'-32'`, ClickHouse raises an exception. Remember about [numeric conversions issues](#numeric-conversion-issues), when using the functions.

**Example**

Query:

``` sql
SELECT toUInt64(nan), toUInt32(-32), toUInt16('16'), toUInt8(8.8);
```

Result:

```response
┌───────toUInt64(nan)─┬─toUInt32(-32)─┬─toUInt16('16')─┬─toUInt8(8.8)─┐
│ 9223372036854775808 │    4294967264 │             16 │            8 │
└─────────────────────┴───────────────┴────────────────┴──────────────┘
```

## toUInt(8\|16\|32\|64\|256)OrZero

## toUInt(8\|16\|32\|64\|256)OrNull

## toUInt(8\|16\|32\|64\|256)OrDefault

## toFloat(32\|64)

## toFloat(32\|64)OrZero

## toFloat(32\|64)OrNull

## toFloat(32\|64)OrDefault

## toDate

Converts the argument to [Date](/docs/en/sql-reference/data-types/date.md) data type.

If the argument is [DateTime](/docs/en/sql-reference/data-types/datetime.md) or [DateTime64](/docs/en/sql-reference/data-types/datetime64.md), it truncates it and leaves the date component of the DateTime:

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

If the argument is a [String](/docs/en/sql-reference/data-types/string.md), it is parsed as [Date](/docs/en/sql-reference/data-types/date.md) or [DateTime](/docs/en/sql-reference/data-types/datetime.md). If it was parsed as [DateTime](/docs/en/sql-reference/data-types/datetime.md), the date component is being used:

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

If the argument is a number and looks like a UNIX timestamp (is greater than 65535), it is interpreted as a [DateTime](/docs/en/sql-reference/data-types/datetime.md), then truncated to [Date](/docs/en/sql-reference/data-types/date.md) in the current timezone. The timezone argument can be specified as a second argument of the function. The truncation to [Date](/docs/en/sql-reference/data-types/date.md) depends on the timezone:

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

If the argument is a number and it is smaller than 65536, it is interpreted as the number of days since 1970-01-01 (the first UNIX day) and converted to [Date](/docs/en/sql-reference/data-types/date.md). It corresponds to the internal numeric representation of the `Date` data type. Example:

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


## toDateOrZero

The same as [toDate](#todate) but returns lower boundary of [Date](/docs/en/sql-reference/data-types/date.md) if an invalid argument is received. Only [String](/docs/en/sql-reference/data-types/string.md) argument is supported.

**Example**

Query:

``` sql
SELECT toDateOrZero('2022-12-30'), toDateOrZero('');
```

Result:

```response
┌─toDateOrZero('2022-12-30')─┬─toDateOrZero('')─┐
│                 2022-12-30 │       1970-01-01 │
└────────────────────────────┴──────────────────┘
```


## toDateOrNull

The same as [toDate](#todate) but returns `NULL` if an invalid argument is received. Only [String](/docs/en/sql-reference/data-types/string.md) argument is supported.

**Example**

Query:

``` sql
SELECT toDateOrNull('2022-12-30'), toDateOrNull('');
```

Result:

```response
┌─toDateOrNull('2022-12-30')─┬─toDateOrNull('')─┐
│                 2022-12-30 │             ᴺᵁᴸᴸ │
└────────────────────────────┴──────────────────┘
```


## toDateOrDefault

Like [toDate](#todate) but if unsuccessful, returns a default value which is either the second argument (if specified), or otherwise the lower boundary of [Date](/docs/en/sql-reference/data-types/date.md).

**Syntax**

``` sql
toDateOrDefault(expr [, default_value])
```

**Example**

Query:

``` sql
SELECT toDateOrDefault('2022-12-30'), toDateOrDefault('', '2023-01-01'::Date);
```

Result:

```response
┌─toDateOrDefault('2022-12-30')─┬─toDateOrDefault('', CAST('2023-01-01', 'Date'))─┐
│                    2022-12-30 │                                      2023-01-01 │
└───────────────────────────────┴─────────────────────────────────────────────────┘
```


## toDateTime

Converts an input value to [DateTime](/docs/en/sql-reference/data-types/datetime.md).

**Syntax**

``` sql
toDateTime(expr[, time_zone ])
```

**Arguments**

- `expr` — The value. [String](/docs/en/sql-reference/data-types/string.md), [Int](/docs/en/sql-reference/data-types/int-uint.md), [Date](/docs/en/sql-reference/data-types/date.md) or [DateTime](/docs/en/sql-reference/data-types/datetime.md).
- `time_zone` — Time zone. [String](/docs/en/sql-reference/data-types/string.md).

:::note
If `expr` is a number, it is interpreted as the number of seconds since the beginning of the Unix Epoch (as Unix timestamp).  
If `expr` is a [String](/docs/en/sql-reference/data-types/string.md), it may be interpreted as a Unix timestamp or as a string representation of date / date with time.  
Thus, parsing of short numbers' string representations (up to 4 digits) is explicitly disabled due to ambiguity, e.g. a string `'1999'` may be both a year (an incomplete string representation of Date / DateTime) or a unix timestamp. Longer numeric strings are allowed.
:::

**Returned value**

- A date time. [DateTime](/docs/en/sql-reference/data-types/datetime.md)

**Example**

Query:

``` sql
SELECT toDateTime('2022-12-30 13:44:17'), toDateTime(1685457500, 'UTC');
```

Result:

```response
┌─toDateTime('2022-12-30 13:44:17')─┬─toDateTime(1685457500, 'UTC')─┐
│               2022-12-30 13:44:17 │           2023-05-30 14:38:20 │
└───────────────────────────────────┴───────────────────────────────┘
```


## toDateTimeOrZero

The same as [toDateTime](#todatetime) but returns lower boundary of [DateTime](/docs/en/sql-reference/data-types/datetime.md) if an invalid argument is received. Only [String](/docs/en/sql-reference/data-types/string.md) argument is supported.

**Example**

Query:

``` sql
SELECT toDateTimeOrZero('2022-12-30 13:44:17'), toDateTimeOrZero('');
```

Result:

```response
┌─toDateTimeOrZero('2022-12-30 13:44:17')─┬─toDateTimeOrZero('')─┐
│                     2022-12-30 13:44:17 │  1970-01-01 00:00:00 │
└─────────────────────────────────────────┴──────────────────────┘
```


## toDateTimeOrNull

The same as [toDateTime](#todatetime) but returns `NULL` if an invalid argument is received. Only [String](/docs/en/sql-reference/data-types/string.md) argument is supported.

**Example**

Query:

``` sql
SELECT toDateTimeOrNull('2022-12-30 13:44:17'), toDateTimeOrNull('');
```

Result:

```response
┌─toDateTimeOrNull('2022-12-30 13:44:17')─┬─toDateTimeOrNull('')─┐
│                     2022-12-30 13:44:17 │                 ᴺᵁᴸᴸ │
└─────────────────────────────────────────┴──────────────────────┘
```


## toDateTimeOrDefault

Like [toDateTime](#todatetime) but if unsuccessful, returns a default value which is either the third argument (if specified), or otherwise the lower boundary of [DateTime](/docs/en/sql-reference/data-types/datetime.md).

**Syntax**

``` sql
toDateTimeOrDefault(expr [, time_zone [, default_value]])
```

**Example**

Query:

``` sql
SELECT toDateTimeOrDefault('2022-12-30 13:44:17'), toDateTimeOrDefault('', 'UTC', '2023-01-01'::DateTime('UTC'));
```

Result:

```response
┌─toDateTimeOrDefault('2022-12-30 13:44:17')─┬─toDateTimeOrDefault('', 'UTC', CAST('2023-01-01', 'DateTime(\'UTC\')'))─┐
│                        2022-12-30 13:44:17 │                                                     2023-01-01 00:00:00 │
└────────────────────────────────────────────┴─────────────────────────────────────────────────────────────────────────┘
```


## toDate32

Converts the argument to the [Date32](/docs/en/sql-reference/data-types/date32.md) data type. If the value is outside the range, `toDate32` returns the border values supported by [Date32](/docs/en/sql-reference/data-types/date32.md). If the argument has [Date](/docs/en/sql-reference/data-types/date.md) type, it's borders are taken into account.

**Syntax**

``` sql
toDate32(expr)
```

**Arguments**

- `expr` — The value. [String](/docs/en/sql-reference/data-types/string.md), [UInt32](/docs/en/sql-reference/data-types/int-uint.md) or [Date](/docs/en/sql-reference/data-types/date.md).

**Returned value**

- A calendar date. Type [Date32](/docs/en/sql-reference/data-types/date32.md).

**Example**

1. The value is within the range:

``` sql
SELECT toDate32('1955-01-01') AS value, toTypeName(value);
```

```response
┌──────value─┬─toTypeName(toDate32('1925-01-01'))─┐
│ 1955-01-01 │ Date32                             │
└────────────┴────────────────────────────────────┘
```

2. The value is outside the range:

``` sql
SELECT toDate32('1899-01-01') AS value, toTypeName(value);
```

```response
┌──────value─┬─toTypeName(toDate32('1899-01-01'))─┐
│ 1900-01-01 │ Date32                             │
└────────────┴────────────────────────────────────┘
```

3. With [Date](/docs/en/sql-reference/data-types/date.md) argument:

``` sql
SELECT toDate32(toDate('1899-01-01')) AS value, toTypeName(value);
```

```response
┌──────value─┬─toTypeName(toDate32(toDate('1899-01-01')))─┐
│ 1970-01-01 │ Date32                                     │
└────────────┴────────────────────────────────────────────┘
```

## toDate32OrZero

The same as [toDate32](#todate32) but returns the min value of [Date32](/docs/en/sql-reference/data-types/date32.md) if an invalid argument is received.

**Example**

Query:

``` sql
SELECT toDate32OrZero('1899-01-01'), toDate32OrZero('');
```

Result:

```response
┌─toDate32OrZero('1899-01-01')─┬─toDate32OrZero('')─┐
│                   1900-01-01 │         1900-01-01 │
└──────────────────────────────┴────────────────────┘
```

## toDate32OrNull

The same as [toDate32](#todate32) but returns `NULL` if an invalid argument is received.

**Example**

Query:

``` sql
SELECT toDate32OrNull('1955-01-01'), toDate32OrNull('');
```

Result:

```response
┌─toDate32OrNull('1955-01-01')─┬─toDate32OrNull('')─┐
│                   1955-01-01 │               ᴺᵁᴸᴸ │
└──────────────────────────────┴────────────────────┘
```

## toDate32OrDefault

Converts the argument to the [Date32](/docs/en/sql-reference/data-types/date32.md) data type. If the value is outside the range, `toDate32OrDefault` returns the lower border value supported by [Date32](/docs/en/sql-reference/data-types/date32.md). If the argument has [Date](/docs/en/sql-reference/data-types/date.md) type, it's borders are taken into account. Returns default value if an invalid argument is received.

**Example**

Query:

``` sql
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

## toDateTime64

Converts the argument to the [DateTime64](/docs/en/sql-reference/data-types/datetime64.md) data type.

**Syntax**

``` sql
toDateTime64(expr, scale, [timezone])
```

**Arguments**

- `expr` — The value. [String](/docs/en/sql-reference/data-types/string.md), [UInt32](/docs/en/sql-reference/data-types/int-uint.md), [Float](/docs/en/sql-reference/data-types/float.md) or [DateTime](/docs/en/sql-reference/data-types/datetime.md).
- `scale` - Tick size (precision): 10<sup>-precision</sup> seconds. Valid range: [ 0 : 9 ].
- `timezone` - Time zone of the specified datetime64 object.

**Returned value**

- A calendar date and time of day, with sub-second precision.

Type: [DateTime64](/docs/en/sql-reference/data-types/datetime64.md).

**Example**

1. The value is within the range:

``` sql
SELECT toDateTime64('1955-01-01 00:00:00.000', 3) AS value, toTypeName(value);
```

```response
┌───────────────────value─┬─toTypeName(toDateTime64('1955-01-01 00:00:00.000', 3))─┐
│ 1955-01-01 00:00:00.000 │ DateTime64(3)                                          │
└─────────────────────────┴────────────────────────────────────────────────────────┘
```

2. As decimal with precision:

``` sql
SELECT toDateTime64(1546300800.000, 3) AS value, toTypeName(value);
```

```response
┌───────────────────value─┬─toTypeName(toDateTime64(1546300800., 3))─┐
│ 2019-01-01 00:00:00.000 │ DateTime64(3)                            │
└─────────────────────────┴──────────────────────────────────────────┘
```

Without the decimal point the value is still treated as Unix Timestamp in seconds:

``` sql
SELECT toDateTime64(1546300800000, 3) AS value, toTypeName(value);
```

```response
┌───────────────────value─┬─toTypeName(toDateTime64(1546300800000, 3))─┐
│ 2282-12-31 00:00:00.000 │ DateTime64(3)                              │
└─────────────────────────┴────────────────────────────────────────────┘
```


3. With `timezone`:

``` sql
SELECT toDateTime64('2019-01-01 00:00:00', 3, 'Asia/Istanbul') AS value, toTypeName(value);
```

```response
┌───────────────────value─┬─toTypeName(toDateTime64('2019-01-01 00:00:00', 3, 'Asia/Istanbul'))─┐
│ 2019-01-01 00:00:00.000 │ DateTime64(3, 'Asia/Istanbul')                                      │
└─────────────────────────┴─────────────────────────────────────────────────────────────────────┘
```

## toDateTime64OrZero

## toDateTime64OrNull

## toDateTime64OrDefault

## toDecimal(32\|64\|128\|256)

Converts `value` to the [Decimal](/docs/en/sql-reference/data-types/decimal.md) data type with precision of `S`. The `value` can be a number or a string. The `S` (scale) parameter specifies the number of decimal places.

- `toDecimal32(value, S)`
- `toDecimal64(value, S)`
- `toDecimal128(value, S)`
- `toDecimal256(value, S)`

## toDecimal(32\|64\|128\|256)OrNull

Converts an input string to a [Nullable(Decimal(P,S))](/docs/en/sql-reference/data-types/decimal.md) data type value. This family of functions includes:

- `toDecimal32OrNull(expr, S)` — Results in `Nullable(Decimal32(S))` data type.
- `toDecimal64OrNull(expr, S)` — Results in `Nullable(Decimal64(S))` data type.
- `toDecimal128OrNull(expr, S)` — Results in `Nullable(Decimal128(S))` data type.
- `toDecimal256OrNull(expr, S)` — Results in `Nullable(Decimal256(S))` data type.

These functions should be used instead of `toDecimal*()` functions, if you prefer to get a `NULL` value instead of an exception in the event of an input value parsing error.

**Arguments**

- `expr` — [Expression](/docs/en/sql-reference/syntax.md/#syntax-expressions), returns a value in the [String](/docs/en/sql-reference/data-types/string.md) data type. ClickHouse expects the textual representation of the decimal number. For example, `'1.111'`.
- `S` — Scale, the number of decimal places in the resulting value.

**Returned value**

A value in the `Nullable(Decimal(P,S))` data type. The value contains:

- Number with `S` decimal places, if ClickHouse interprets the input string as a number.
- `NULL`, if ClickHouse can’t interpret the input string as a number or if the input number contains more than `S` decimal places.

**Examples**

Query:

``` sql
SELECT toDecimal32OrNull(toString(-1.111), 5) AS val, toTypeName(val);
```

Result:

```response
┌────val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 5))─┐
│ -1.111 │ Nullable(Decimal(9, 5))                            │
└────────┴────────────────────────────────────────────────────┘
```

Query:

``` sql
SELECT toDecimal32OrNull(toString(-1.111), 2) AS val, toTypeName(val);
```

Result:

```response
┌──val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 2))─┐
│ ᴺᵁᴸᴸ │ Nullable(Decimal(9, 2))                            │
└──────┴────────────────────────────────────────────────────┘
```


## toDecimal(32\|64\|128\|256)OrDefault

Converts an input string to a [Decimal(P,S)](/docs/en/sql-reference/data-types/decimal.md) data type value. This family of functions includes:

- `toDecimal32OrDefault(expr, S)` — Results in `Decimal32(S)` data type.
- `toDecimal64OrDefault(expr, S)` — Results in `Decimal64(S)` data type.
- `toDecimal128OrDefault(expr, S)` — Results in `Decimal128(S)` data type.
- `toDecimal256OrDefault(expr, S)` — Results in `Decimal256(S)` data type.

These functions should be used instead of `toDecimal*()` functions, if you prefer to get a default value instead of an exception in the event of an input value parsing error.

**Arguments**

- `expr` — [Expression](/docs/en/sql-reference/syntax.md/#syntax-expressions), returns a value in the [String](/docs/en/sql-reference/data-types/string.md) data type. ClickHouse expects the textual representation of the decimal number. For example, `'1.111'`.
- `S` — Scale, the number of decimal places in the resulting value.

**Returned value**

A value in the `Decimal(P,S)` data type. The value contains:

- Number with `S` decimal places, if ClickHouse interprets the input string as a number.
- Default `Decimal(P,S)` data type value, if ClickHouse can’t interpret the input string as a number or if the input number contains more than `S` decimal places.

**Examples**

Query:

``` sql
SELECT toDecimal32OrDefault(toString(-1.111), 5) AS val, toTypeName(val);
```

Result:

```response
┌────val─┬─toTypeName(toDecimal32OrDefault(toString(-1.111), 5))─┐
│ -1.111 │ Decimal(9, 5)                                         │
└────────┴───────────────────────────────────────────────────────┘
```

Query:

``` sql
SELECT toDecimal32OrDefault(toString(-1.111), 2) AS val, toTypeName(val);
```

Result:

```response
┌─val─┬─toTypeName(toDecimal32OrDefault(toString(-1.111), 2))─┐
│   0 │ Decimal(9, 2)                                         │
└─────┴───────────────────────────────────────────────────────┘
```

## toDecimal(32\|64\|128\|256)OrZero

Converts an input value to the [Decimal(P,S)](/docs/en/sql-reference/data-types/decimal.md) data type. This family of functions includes:

- `toDecimal32OrZero( expr, S)` — Results in `Decimal32(S)` data type.
- `toDecimal64OrZero( expr, S)` — Results in `Decimal64(S)` data type.
- `toDecimal128OrZero( expr, S)` — Results in `Decimal128(S)` data type.
- `toDecimal256OrZero( expr, S)` — Results in `Decimal256(S)` data type.

These functions should be used instead of `toDecimal*()` functions, if you prefer to get a `0` value instead of an exception in the event of an input value parsing error.

**Arguments**

- `expr` — [Expression](/docs/en/sql-reference/syntax.md/#syntax-expressions), returns a value in the [String](/docs/en/sql-reference/data-types/string.md) data type. ClickHouse expects the textual representation of the decimal number. For example, `'1.111'`.
- `S` — Scale, the number of decimal places in the resulting value.

**Returned value**

A value in the `Nullable(Decimal(P,S))` data type. The value contains:

- Number with `S` decimal places, if ClickHouse interprets the input string as a number.
- 0 with `S` decimal places, if ClickHouse can’t interpret the input string as a number or if the input number contains more than `S` decimal places.

**Example**

Query:

``` sql
SELECT toDecimal32OrZero(toString(-1.111), 5) AS val, toTypeName(val);
```

Result:

```response
┌────val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 5))─┐
│ -1.111 │ Decimal(9, 5)                                      │
└────────┴────────────────────────────────────────────────────┘
```

Query:

``` sql
SELECT toDecimal32OrZero(toString(-1.111), 2) AS val, toTypeName(val);
```

Result:

```response
┌──val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 2))─┐
│ 0.00 │ Decimal(9, 2)                                      │
└──────┴────────────────────────────────────────────────────┘
```

## toString

Functions for converting between numbers, strings (but not fixed strings), dates, and dates with times.
All these functions accept one argument.

When converting to or from a string, the value is formatted or parsed using the same rules as for the TabSeparated format (and almost all other text formats). If the string can’t be parsed, an exception is thrown and the request is canceled.

When converting dates to numbers or vice versa, the date corresponds to the number of days since the beginning of the Unix epoch.
When converting dates with times to numbers or vice versa, the date with time corresponds to the number of seconds since the beginning of the Unix epoch.

The date and date-with-time formats for the toDate/toDateTime functions are defined as follows:

```response
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

As an exception, if converting from UInt32, Int32, UInt64, or Int64 numeric types to Date, and if the number is greater than or equal to 65536, the number is interpreted as a Unix timestamp (and not as the number of days) and is rounded to the date. This allows support for the common occurrence of writing `toDate(unix_timestamp)`, which otherwise would be an error and would require writing the more cumbersome `toDate(toDateTime(unix_timestamp))`.

Conversion between a date and a date with time is performed the natural way: by adding a null time or dropping the time.

Conversion between numeric types uses the same rules as assignments between different numeric types in C++.

Additionally, the toString function of the DateTime argument can take a second String argument containing the name of the time zone. Example: `Asia/Yekaterinburg` In this case, the time is formatted according to the specified time zone.

**Example**

Query:

``` sql
SELECT
    now() AS ts,
    time_zone,
    toString(ts, time_zone) AS str_tz_datetime
FROM system.time_zones
WHERE time_zone LIKE 'Europe%'
LIMIT 10
```

Result:

```response
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

Also see the `toUnixTimestamp` function.

## toFixedString(s, N)

Converts a [String](/docs/en/sql-reference/data-types/string.md) type argument to a [FixedString(N)](/docs/en/sql-reference/data-types/fixedstring.md) type (a string of fixed length N).
If the string has fewer bytes than N, it is padded with null bytes to the right. If the string has more bytes than N, an exception is thrown.

## toStringCutToZero(s)

Accepts a String or FixedString argument. Returns the String with the content truncated at the first zero byte found.

**Example**

Query:

``` sql
SELECT toFixedString('foo', 8) AS s, toStringCutToZero(s) AS s_cut;
```

Result:

```response
┌─s─────────────┬─s_cut─┐
│ foo\0\0\0\0\0 │ foo   │
└───────────────┴───────┘
```

Query:

``` sql
SELECT toFixedString('foo\0bar', 8) AS s, toStringCutToZero(s) AS s_cut;
```

Result:

```response
┌─s──────────┬─s_cut─┐
│ foo\0bar\0 │ foo   │
└────────────┴───────┘
```

## toDecimalString

Converts a numeric value to String with the number of fractional digits in the output specified by the user.

**Syntax**

``` sql
toDecimalString(number, scale)
```

**Parameters**

- `number` — Value to be represented as String, [Int, UInt](/docs/en/sql-reference/data-types/int-uint.md), [Float](/docs/en/sql-reference/data-types/float.md), [Decimal](/docs/en/sql-reference/data-types/decimal.md),
- `scale` — Number of fractional digits, [UInt8](/docs/en/sql-reference/data-types/int-uint.md).
    * Maximum scale for [Decimal](/docs/en/sql-reference/data-types/decimal.md) and [Int, UInt](/docs/en/sql-reference/data-types/int-uint.md) types is 77 (it is the maximum possible number of significant digits for Decimal),
    * Maximum scale for [Float](/docs/en/sql-reference/data-types/float.md) is 60.

**Returned value**

- Input value represented as [String](/docs/en/sql-reference/data-types/string.md) with given number of fractional digits (scale).
    The number is rounded up or down according to common arithmetic in case requested scale is smaller than original number's scale.

**Example**

Query:

``` sql
SELECT toDecimalString(CAST('64.32', 'Float64'), 5);
```

Result:

```response
┌toDecimalString(CAST('64.32', 'Float64'), 5)─┐
│ 64.32000                                    │
└─────────────────────────────────────────────┘
```

## reinterpretAsUInt(8\|16\|32\|64)

## reinterpretAsInt(8\|16\|32\|64)

## reinterpretAsFloat(32\|64)

## reinterpretAsDate

## reinterpretAsDateTime

These functions accept a string and interpret the bytes placed at the beginning of the string as a number in host order (little endian). If the string isn’t long enough, the functions work as if the string is padded with the necessary number of null bytes. If the string is longer than needed, the extra bytes are ignored. A date is interpreted as the number of days since the beginning of the Unix Epoch, and a date with time is interpreted as the number of seconds since the beginning of the Unix Epoch.

## reinterpretAsString

This function accepts a number or date or date with time and returns a string containing bytes representing the corresponding value in host order (little endian). Null bytes are dropped from the end. For example, a UInt32 type value of 255 is a string that is one byte long.

## reinterpretAsFixedString

This function accepts a number or date or date with time and returns a FixedString containing bytes representing the corresponding value in host order (little endian). Null bytes are dropped from the end. For example, a UInt32 type value of 255 is a FixedString that is one byte long.

## reinterpretAsUUID

:::note
In addition to the UUID functions listed here, there is dedicated [UUID function documentation](/docs/en/sql-reference/functions/uuid-functions.md).
:::

Accepts 16 bytes string and returns UUID containing bytes representing the corresponding value in network byte order (big-endian). If the string isn't long enough, the function works as if the string is padded with the necessary number of null bytes to the end. If the string is longer than 16 bytes, the extra bytes at the end are ignored.

**Syntax**

``` sql
reinterpretAsUUID(fixed_string)
```

**Arguments**

- `fixed_string` — Big-endian byte string. [FixedString](/docs/en/sql-reference/data-types/fixedstring.md/#fixedstring).

**Returned value**

- The UUID type value. [UUID](/docs/en/sql-reference/data-types/uuid.md/#uuid-data-type).

**Examples**

String to UUID.

Query:

``` sql
SELECT reinterpretAsUUID(reverse(unhex('000102030405060708090a0b0c0d0e0f')));
```

Result:

```response
┌─reinterpretAsUUID(reverse(unhex('000102030405060708090a0b0c0d0e0f')))─┐
│                                  08090a0b-0c0d-0e0f-0001-020304050607 │
└───────────────────────────────────────────────────────────────────────┘
```

Going back and forth from String to UUID.

Query:

``` sql
WITH
    generateUUIDv4() AS uuid,
    identity(lower(hex(reverse(reinterpretAsString(uuid))))) AS str,
    reinterpretAsUUID(reverse(unhex(str))) AS uuid2
SELECT uuid = uuid2;
```

Result:

```response
┌─equals(uuid, uuid2)─┐
│                   1 │
└─────────────────────┘
```

## reinterpret(x, T)

Uses the same source in-memory bytes sequence for `x` value and reinterprets it to destination type.

**Syntax**

``` sql
reinterpret(x, type)
```

**Arguments**

- `x` — Any type.
- `type` — Destination type. [String](/docs/en/sql-reference/data-types/string.md).

**Returned value**

- Destination type value.

**Examples**

Query:
```sql
SELECT reinterpret(toInt8(-1), 'UInt8') as int_to_uint,
    reinterpret(toInt8(1), 'Float32') as int_to_float,
    reinterpret('1', 'UInt32') as string_to_int;
```

Result:

```
┌─int_to_uint─┬─int_to_float─┬─string_to_int─┐
│         255 │        1e-45 │            49 │
└─────────────┴──────────────┴───────────────┘
```

## CAST(x, T)

Converts an input value to the specified data type. Unlike the [reinterpret](#type_conversion_function-reinterpret) function, `CAST` tries to present the same value using the new data type. If the conversion can not be done then an exception is raised.
Several syntax variants are supported.

**Syntax**

``` sql
CAST(x, T)
CAST(x AS t)
x::t
```

**Arguments**

- `x` — A value to convert. May be of any type.
- `T` — The name of the target data type. [String](/docs/en/sql-reference/data-types/string.md).
- `t` — The target data type.

**Returned value**

- Converted value.

:::note
If the input value does not fit the bounds of the target type, the result overflows. For example, `CAST(-1, 'UInt8')` returns `255`.
:::

**Examples**

Query:

```sql
SELECT
    CAST(toInt8(-1), 'UInt8') AS cast_int_to_uint,
    CAST(1.5 AS Decimal(3,2)) AS cast_float_to_decimal,
    '1'::Int32 AS cast_string_to_int;
```

Result:

```
┌─cast_int_to_uint─┬─cast_float_to_decimal─┬─cast_string_to_int─┐
│              255 │                  1.50 │                  1 │
└──────────────────┴───────────────────────┴────────────────────┘
```

Query:

``` sql
SELECT
    '2016-06-15 23:00:00' AS timestamp,
    CAST(timestamp AS DateTime) AS datetime,
    CAST(timestamp AS Date) AS date,
    CAST(timestamp, 'String') AS string,
    CAST(timestamp, 'FixedString(22)') AS fixed_string;
```

Result:

```response
┌─timestamp───────────┬────────────datetime─┬───────date─┬─string──────────────┬─fixed_string──────────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00\0\0\0 │
└─────────────────────┴─────────────────────┴────────────┴─────────────────────┴───────────────────────────┘
```

Conversion to [FixedString (N)](/docs/en/sql-reference/data-types/fixedstring.md) only works for arguments of type [String](/docs/en/sql-reference/data-types/string.md) or [FixedString](/docs/en/sql-reference/data-types/fixedstring.md).

Type conversion to [Nullable](/docs/en/sql-reference/data-types/nullable.md) and back is supported.

**Example**

Query:

``` sql
SELECT toTypeName(x) FROM t_null;
```

Result:

```response
┌─toTypeName(x)─┐
│ Int8          │
│ Int8          │
└───────────────┘
```

Query:

``` sql
SELECT toTypeName(CAST(x, 'Nullable(UInt16)')) FROM t_null;
```

Result:

```response
┌─toTypeName(CAST(x, 'Nullable(UInt16)'))─┐
│ Nullable(UInt16)                        │
│ Nullable(UInt16)                        │
└─────────────────────────────────────────┘
```

**See also**

- [cast_keep_nullable](../../operations/settings/settings.md/#cast_keep_nullable) setting

## accurateCast(x, T)

Converts `x` to the `T` data type.

The difference from [cast(x, T)](#type_conversion_function-cast) is that `accurateCast` does not allow overflow of numeric types during cast if type value `x` does not fit the bounds of type `T`. For example, `accurateCast(-1, 'UInt8')` throws an exception.

**Example**

Query:

``` sql
SELECT cast(-1, 'UInt8') as uint8;
```

Result:

```response
┌─uint8─┐
│   255 │
└───────┘
```

Query:

```sql
SELECT accurateCast(-1, 'UInt8') as uint8;
```

Result:

```response
Code: 70. DB::Exception: Received from localhost:9000. DB::Exception: Value in column Int8 cannot be safely converted into type UInt8: While processing accurateCast(-1, 'UInt8') AS uint8.
```

## accurateCastOrNull(x, T)

Converts input value `x` to the specified data type `T`. Always returns [Nullable](/docs/en/sql-reference/data-types/nullable.md) type and returns [NULL](/docs/en/sql-reference/syntax.md/#null-literal) if the casted value is not representable in the target type.

**Syntax**

```sql
accurateCastOrNull(x, T)
```

**Parameters**

- `x` — Input value.
- `T` — The name of the returned data type.

**Returned value**

- The value, converted to the specified data type `T`.

**Example**

Query:

``` sql
SELECT toTypeName(accurateCastOrNull(5, 'UInt8'));
```

Result:

```response
┌─toTypeName(accurateCastOrNull(5, 'UInt8'))─┐
│ Nullable(UInt8)                            │
└────────────────────────────────────────────┘
```

Query:

``` sql
SELECT
    accurateCastOrNull(-1, 'UInt8') as uint8,
    accurateCastOrNull(128, 'Int8') as int8,
    accurateCastOrNull('Test', 'FixedString(2)') as fixed_string;
```

Result:

```response
┌─uint8─┬─int8─┬─fixed_string─┐
│  ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │
└───────┴──────┴──────────────┘
```


## accurateCastOrDefault(x, T[, default_value])

Converts input value `x` to the specified data type `T`. Returns default type value or `default_value` if specified if the casted value is not representable in the target type.

**Syntax**

```sql
accurateCastOrDefault(x, T)
```

**Parameters**

- `x` — Input value.
- `T` — The name of the returned data type.
- `default_value` — Default value of returned data type.

**Returned value**

- The value converted to the specified data type `T`.

**Example**

Query:

``` sql
SELECT toTypeName(accurateCastOrDefault(5, 'UInt8'));
```

Result:

```response
┌─toTypeName(accurateCastOrDefault(5, 'UInt8'))─┐
│ UInt8                                         │
└───────────────────────────────────────────────┘
```

Query:

``` sql
SELECT
    accurateCastOrDefault(-1, 'UInt8') as uint8,
    accurateCastOrDefault(-1, 'UInt8', 5) as uint8_default,
    accurateCastOrDefault(128, 'Int8') as int8,
    accurateCastOrDefault(128, 'Int8', 5) as int8_default,
    accurateCastOrDefault('Test', 'FixedString(2)') as fixed_string,
    accurateCastOrDefault('Test', 'FixedString(2)', 'Te') as fixed_string_default;
```

Result:

```response
┌─uint8─┬─uint8_default─┬─int8─┬─int8_default─┬─fixed_string─┬─fixed_string_default─┐
│     0 │             5 │    0 │            5 │              │ Te                   │
└───────┴───────────────┴──────┴──────────────┴──────────────┴──────────────────────┘
```

## toInterval(Year\|Quarter\|Month\|Week\|Day\|Hour\|Minute\|Second)

Converts a Number type argument to an [Interval](/docs/en/sql-reference/data-types/special-data-types/interval.md) data type.

**Syntax**

``` sql
toIntervalSecond(number)
toIntervalMinute(number)
toIntervalHour(number)
toIntervalDay(number)
toIntervalWeek(number)
toIntervalMonth(number)
toIntervalQuarter(number)
toIntervalYear(number)
```

**Arguments**

- `number` — Duration of interval. Positive integer number.

**Returned values**

- The value in `Interval` data type.

**Example**

Query:

``` sql
WITH
    toDate('2019-01-01') AS date,
    INTERVAL 1 WEEK AS interval_week,
    toIntervalWeek(1) AS interval_to_week
SELECT
    date + interval_week,
    date + interval_to_week;
```

Result:

```response
┌─plus(date, interval_week)─┬─plus(date, interval_to_week)─┐
│                2019-01-08 │                   2019-01-08 │
└───────────────────────────┴──────────────────────────────┘
```

## parseDateTime {#type_conversion_functions-parseDateTime}

Converts a [String](/docs/en/sql-reference/data-types/string.md) to [DateTime](/docs/en/sql-reference/data-types/datetime.md) according to a [MySQL format string](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format).

This function is the opposite operation of function [formatDateTime](/docs/en/sql-reference/functions/date-time-functions.md#date_time_functions-formatDateTime).

**Syntax**

``` sql
parseDateTime(str, format[, timezone])
```

**Arguments**

- `str` — the String to be parsed
- `format` — the format string
- `timezone` — [Timezone](/docs/en/operations/server-configuration-parameters/settings.md/#server_configuration_parameters-timezone). Optional.

**Returned value(s)**

Returns DateTime values parsed from input string according to a MySQL style format string.

**Supported format specifiers**

All format specifiers listed in [formatDateTime](/docs/en/sql-reference/functions/date-time-functions.md#date_time_functions-formatDateTime) except:
- %Q: Quarter (1-4)

**Example**

``` sql
SELECT parseDateTime('2021-01-04+23:00:00', '%Y-%m-%d+%H:%i:%s')

┌─parseDateTime('2021-01-04+23:00:00', '%Y-%m-%d+%H:%i:%s')─┐
│                                       2021-01-04 23:00:00 │
└───────────────────────────────────────────────────────────┘
```

Alias: `TO_TIMESTAMP`.

## parseDateTimeOrZero

Same as for [parseDateTime](#type_conversion_functions-parseDateTime) except that it returns zero date when it encounters a date format that cannot be processed.

## parseDateTimeOrNull

Same as for [parseDateTime](#type_conversion_functions-parseDateTime) except that it returns `NULL` when it encounters a date format that cannot be processed.

Alias: `str_to_date`.

## parseDateTimeInJodaSyntax {#type_conversion_functions-parseDateTimeInJodaSyntax}

Similar to [parseDateTime](#parsedatetime), except that the format string is in [Joda](https://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html) instead of MySQL syntax.

This function is the opposite operation of function [formatDateTimeInJodaSyntax](/docs/en/sql-reference/functions/date-time-functions.md#date_time_functions-formatDateTimeInJodaSyntax).

**Syntax**

``` sql
parseDateTimeInJodaSyntax(str, format[, timezone])
```

**Arguments**

- `str` — the String to be parsed
- `format` — the format string
- `timezone` — [Timezone](/docs/en/operations/server-configuration-parameters/settings.md/#server_configuration_parameters-timezone). Optional.

**Returned value(s)**

Returns DateTime values parsed from input string according to a Joda style format.

**Supported format specifiers**

All format specifiers listed in [formatDateTimeInJoda](/docs/en/sql-reference/functions/date-time-functions.md#date_time_functions-formatDateTime) are supported, except:
- S: fraction of second
- z: time zone
- Z: time zone offset/id

**Example**

``` sql
SELECT parseDateTimeInJodaSyntax('2023-02-24 14:53:31', 'yyyy-MM-dd HH:mm:ss', 'Europe/Minsk')

┌─parseDateTimeInJodaSyntax('2023-02-24 14:53:31', 'yyyy-MM-dd HH:mm:ss', 'Europe/Minsk')─┐
│                                                                     2023-02-24 14:53:31 │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

## parseDateTimeInJodaSyntaxOrZero

Same as for [parseDateTimeInJodaSyntax](#type_conversion_functions-parseDateTimeInJodaSyntax) except that it returns zero date when it encounters a date format that cannot be processed.

## parseDateTimeInJodaSyntaxOrNull

Same as for [parseDateTimeInJodaSyntax](#type_conversion_functions-parseDateTimeInJodaSyntax) except that it returns `NULL` when it encounters a date format that cannot be processed.

## parseDateTimeBestEffort
## parseDateTime32BestEffort

Converts a date and time in the [String](/docs/en/sql-reference/data-types/string.md) representation to [DateTime](/docs/en/sql-reference/data-types/datetime.md/#data_type-datetime) data type.

The function parses [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601), [RFC 1123 - 5.2.14 RFC-822 Date and Time Specification](https://tools.ietf.org/html/rfc1123#page-55), ClickHouse’s and some other date and time formats.

**Syntax**

``` sql
parseDateTimeBestEffort(time_string [, time_zone])
```

**Arguments**

- `time_string` — String containing a date and time to convert. [String](/docs/en/sql-reference/data-types/string.md).
- `time_zone` — Time zone. The function parses `time_string` according to the time zone. [String](/docs/en/sql-reference/data-types/string.md).

**Supported non-standard formats**

- A string containing 9..10 digit [unix timestamp](https://en.wikipedia.org/wiki/Unix_time).
- A string with a date and a time component: `YYYYMMDDhhmmss`, `DD/MM/YYYY hh:mm:ss`, `DD-MM-YY hh:mm`, `YYYY-MM-DD hh:mm:ss`, etc.
- A string with a date, but no time component: `YYYY`, `YYYYMM`, `YYYY*MM`, `DD/MM/YYYY`, `DD-MM-YY` etc.
- A string with a day and time: `DD`, `DD hh`, `DD hh:mm`. In this case `MM` is substituted by `01`.
- A string that includes the date and time along with time zone offset information: `YYYY-MM-DD hh:mm:ss ±h:mm`, etc. For example, `2020-12-12 17:36:00 -5:00`.
- A [syslog timestamp](https://datatracker.ietf.org/doc/html/rfc3164#section-4.1.2): `Mmm dd hh:mm:ss`. For example, `Jun  9 14:20:32`.

For all of the formats with separator the function parses months names expressed by their full name or by the first three letters of a month name. Examples: `24/DEC/18`, `24-Dec-18`, `01-September-2018`.
If the year is not specified, it is considered to be equal to the current year. If the resulting DateTime happen to be in the future (even by a second after the current moment), then the current year is substituted by the previous year.

**Returned value**

- `time_string` converted to the [DateTime](/docs/en/sql-reference/data-types/datetime.md) data type.

**Examples**

Query:

``` sql
SELECT parseDateTimeBestEffort('23/10/2020 12:12:57')
AS parseDateTimeBestEffort;
```

Result:

```response
┌─parseDateTimeBestEffort─┐
│     2020-10-23 12:12:57 │
└─────────────────────────┘
```

Query:

``` sql
SELECT parseDateTimeBestEffort('Sat, 18 Aug 2018 07:22:16 GMT', 'Asia/Istanbul')
AS parseDateTimeBestEffort;
```

Result:

```response
┌─parseDateTimeBestEffort─┐
│     2018-08-18 10:22:16 │
└─────────────────────────┘
```

Query:

``` sql
SELECT parseDateTimeBestEffort('1284101485')
AS parseDateTimeBestEffort;
```

Result:

```response
┌─parseDateTimeBestEffort─┐
│     2015-07-07 12:04:41 │
└─────────────────────────┘
```

Query:

``` sql
SELECT parseDateTimeBestEffort('2018-10-23 10:12:12')
AS parseDateTimeBestEffort;
```

Result:

```response
┌─parseDateTimeBestEffort─┐
│     2018-10-23 10:12:12 │
└─────────────────────────┘
```

Query:

``` sql
SELECT toYear(now()) as year, parseDateTimeBestEffort('10 20:19');
```

Result:

```response
┌─year─┬─parseDateTimeBestEffort('10 20:19')─┐
│ 2023 │                 2023-01-10 20:19:00 │
└──────┴─────────────────────────────────────┘
```

Query:

``` sql
WITH
    now() AS ts_now,
    formatDateTime(ts_around, '%b %e %T') AS syslog_arg
SELECT
    ts_now,
    syslog_arg,
    parseDateTimeBestEffort(syslog_arg)
FROM (SELECT arrayJoin([ts_now - 30, ts_now + 30]) AS ts_around);
```

Result:

```response
┌──────────────ts_now─┬─syslog_arg──────┬─parseDateTimeBestEffort(syslog_arg)─┐
│ 2023-06-30 23:59:30 │ Jun 30 23:59:00 │                 2023-06-30 23:59:00 │
│ 2023-06-30 23:59:30 │ Jul  1 00:00:00 │                 2022-07-01 00:00:00 │
└─────────────────────┴─────────────────┴─────────────────────────────────────┘
```

**See Also**

- [RFC 1123](https://datatracker.ietf.org/doc/html/rfc1123)
- [toDate](#todate)
- [toDateTime](#todatetime)
- [ISO 8601 announcement by @xkcd](https://xkcd.com/1179/)
- [RFC 3164](https://datatracker.ietf.org/doc/html/rfc3164#section-4.1.2)

## parseDateTimeBestEffortUS

This function behaves like [parseDateTimeBestEffort](#parsedatetimebesteffort) for ISO date formats, e.g. `YYYY-MM-DD hh:mm:ss`, and other date formats where the month and date components can be unambiguously extracted, e.g. `YYYYMMDDhhmmss`, `YYYY-MM`, `DD hh`, or `YYYY-MM-DD hh:mm:ss ±h:mm`. If the month and the date components cannot be unambiguously extracted, e.g. `MM/DD/YYYY`, `MM-DD-YYYY`, or `MM-DD-YY`, it prefers the US date format instead of `DD/MM/YYYY`, `DD-MM-YYYY`, or `DD-MM-YY`. As an exception from the latter, if the month is bigger than 12 and smaller or equal than 31, this function falls back to the behavior of [parseDateTimeBestEffort](#parsedatetimebesteffort), e.g. `15/08/2020` is parsed as `2020-08-15`.

## parseDateTimeBestEffortOrNull
## parseDateTime32BestEffortOrNull

Same as for [parseDateTimeBestEffort](#parsedatetimebesteffort) except that it returns `NULL` when it encounters a date format that cannot be processed.

## parseDateTimeBestEffortOrZero
## parseDateTime32BestEffortOrZero

Same as for [parseDateTimeBestEffort](#parsedatetimebesteffort) except that it returns zero date or zero date time when it encounters a date format that cannot be processed.

## parseDateTimeBestEffortUSOrNull

Same as [parseDateTimeBestEffortUS](#parsedatetimebesteffortUS) function except that it returns `NULL` when it encounters a date format that cannot be processed.

## parseDateTimeBestEffortUSOrZero

Same as [parseDateTimeBestEffortUS](#parsedatetimebesteffortUS) function except that it returns zero date (`1970-01-01`) or zero date with time (`1970-01-01 00:00:00`) when it encounters a date format that cannot be processed.

## parseDateTime64BestEffort

Same as [parseDateTimeBestEffort](#parsedatetimebesteffort) function but also parse milliseconds and microseconds and returns [DateTime](/docs/en/sql-reference/functions/type-conversion-functions.md/#data_type-datetime) data type.

**Syntax**

``` sql
parseDateTime64BestEffort(time_string [, precision [, time_zone]])
```

**Parameters**

- `time_string` — String containing a date or date with time to convert. [String](/docs/en/sql-reference/data-types/string.md).
- `precision` — Required precision. `3` — for milliseconds, `6` — for microseconds. Default — `3`. Optional. [UInt8](/docs/en/sql-reference/data-types/int-uint.md).
- `time_zone` — [Timezone](/docs/en/operations/server-configuration-parameters/settings.md/#server_configuration_parameters-timezone). The function parses `time_string` according to the timezone. Optional. [String](/docs/en/sql-reference/data-types/string.md).

**Returned value**

- `time_string` converted to the [DateTime](/docs/en/sql-reference/data-types/datetime.md) data type.

**Examples**

Query:

```sql
SELECT parseDateTime64BestEffort('2021-01-01') AS a, toTypeName(a) AS t
UNION ALL
SELECT parseDateTime64BestEffort('2021-01-01 01:01:00.12346') AS a, toTypeName(a) AS t
UNION ALL
SELECT parseDateTime64BestEffort('2021-01-01 01:01:00.12346',6) AS a, toTypeName(a) AS t
UNION ALL
SELECT parseDateTime64BestEffort('2021-01-01 01:01:00.12346',3,'Asia/Istanbul') AS a, toTypeName(a) AS t
FORMAT PrettyCompactMonoBlock;
```

Result:

```
┌──────────────────────────a─┬─t──────────────────────────────┐
│ 2021-01-01 01:01:00.123000 │ DateTime64(3)                  │
│ 2021-01-01 00:00:00.000000 │ DateTime64(3)                  │
│ 2021-01-01 01:01:00.123460 │ DateTime64(6)                  │
│ 2020-12-31 22:01:00.123000 │ DateTime64(3, 'Asia/Istanbul') │
└────────────────────────────┴────────────────────────────────┘
```

## parseDateTime64BestEffortUS

Same as for [parseDateTime64BestEffort](#parsedatetime64besteffort), except that this function prefers US date format (`MM/DD/YYYY` etc.) in case of ambiguity.

## parseDateTime64BestEffortOrNull

Same as for [parseDateTime64BestEffort](#parsedatetime64besteffort) except that it returns `NULL` when it encounters a date format that cannot be processed.

## parseDateTime64BestEffortOrZero

Same as for [parseDateTime64BestEffort](#parsedatetime64besteffort) except that it returns zero date or zero date time when it encounters a date format that cannot be processed.

## parseDateTime64BestEffortUSOrNull

Same as for [parseDateTime64BestEffort](#parsedatetime64besteffort), except that this function prefers US date format (`MM/DD/YYYY` etc.) in case of ambiguity and returns `NULL` when it encounters a date format that cannot be processed.

## parseDateTime64BestEffortUSOrZero

Same as for [parseDateTime64BestEffort](#parsedatetime64besteffort), except that this function prefers US date format (`MM/DD/YYYY` etc.) in case of ambiguity and returns zero date or zero date time when it encounters a date format that cannot be processed.

## toLowCardinality

Converts input parameter to the [LowCardinality](/docs/en/sql-reference/data-types/lowcardinality.md) version of same data type.

To convert data from the `LowCardinality` data type use the [CAST](#type_conversion_function-cast) function. For example, `CAST(x as String)`.

**Syntax**

```sql
toLowCardinality(expr)
```

**Arguments**

- `expr` — [Expression](/docs/en/sql-reference/syntax.md/#syntax-expressions) resulting in one of the [supported data types](/docs/en/sql-reference/data-types/index.md/#data_types).

**Returned values**

- Result of `expr`.

Type: `LowCardinality(expr_result_type)`

**Example**

Query:

```sql
SELECT toLowCardinality('1');
```

Result:

```response
┌─toLowCardinality('1')─┐
│ 1                     │
└───────────────────────┘
```

## toUnixTimestamp64Milli

## toUnixTimestamp64Micro

## toUnixTimestamp64Nano

Converts a `DateTime64` to a `Int64` value with fixed sub-second precision. Input value is scaled up or down appropriately depending on it precision.

:::note
The output value is a timestamp in UTC, not in the timezone of `DateTime64`.
:::

**Syntax**

```sql
toUnixTimestamp64Milli(value)
toUnixTimestamp64Micro(value)
toUnixTimestamp64Nano(value)
```

**Arguments**

- `value` — DateTime64 value with any precision.

**Returned value**

- `value` converted to the `Int64` data type.

**Examples**

Query:

```sql
WITH toDateTime64('2019-09-16 19:20:12.345678910', 6) AS dt64
SELECT toUnixTimestamp64Milli(dt64);
```

Result:

```response
┌─toUnixTimestamp64Milli(dt64)─┐
│                1568650812345 │
└──────────────────────────────┘
```

Query:

``` sql
WITH toDateTime64('2019-09-16 19:20:12.345678910', 6) AS dt64
SELECT toUnixTimestamp64Nano(dt64);
```

Result:

```response
┌─toUnixTimestamp64Nano(dt64)─┐
│         1568650812345678000 │
└─────────────────────────────┘
```

## fromUnixTimestamp64Milli

## fromUnixTimestamp64Micro

## fromUnixTimestamp64Nano

Converts an `Int64` to a `DateTime64` value with fixed sub-second precision and optional timezone. Input value is scaled up or down appropriately depending on it’s precision. Please note that input value is treated as UTC timestamp, not timestamp at given (or implicit) timezone.

**Syntax**

``` sql
fromUnixTimestamp64Milli(value [, timezone])
fromUnixTimestamp64Micro(value [, timezone])
fromUnixTimestamp64Nano(value [, timezone])
```

**Arguments**

- `value` — `Int64` value with any precision.
- `timezone` — `String` (optional) timezone name of the result.

**Returned value**

- `value` converted to the `DateTime64` data type.

**Example**

Query:

``` sql
WITH CAST(1234567891011, 'Int64') AS i64
SELECT fromUnixTimestamp64Milli(i64, 'UTC');
```

Result:

```response
┌─fromUnixTimestamp64Milli(i64, 'UTC')─┐
│              2009-02-13 23:31:31.011 │
└──────────────────────────────────────┘
```

## formatRow

Converts arbitrary expressions into a string via given format.

**Syntax**

``` sql
formatRow(format, x, y, ...)
```

**Arguments**

- `format` — Text format. For example, [CSV](/docs/en/interfaces/formats.md/#csv), [TSV](/docs/en/interfaces/formats.md/#tabseparated).
- `x`,`y`, ... — Expressions.

**Returned value**

- A formatted string. (for text formats it's usually terminated with the new line character).

**Example**

Query:

``` sql
SELECT formatRow('CSV', number, 'good')
FROM numbers(3);
```

Result:

```response
┌─formatRow('CSV', number, 'good')─┐
│ 0,"good"
                         │
│ 1,"good"
                         │
│ 2,"good"
                         │
└──────────────────────────────────┘
```

**Note**: If format contains suffix/prefix, it will be written in each row.

**Example**

Query:

``` sql
SELECT formatRow('CustomSeparated', number, 'good')
FROM numbers(3)
SETTINGS format_custom_result_before_delimiter='<prefix>\n', format_custom_result_after_delimiter='<suffix>'
```

Result:

```response
┌─formatRow('CustomSeparated', number, 'good')─┐
│ <prefix>
0	good
<suffix>                   │
│ <prefix>
1	good
<suffix>                   │
│ <prefix>
2	good
<suffix>                   │
└──────────────────────────────────────────────┘
```

Note: Only row-based formats are supported in this function.

## formatRowNoNewline

Converts arbitrary expressions into a string via given format. Differs from formatRow in that this function trims the last `\n` if any.

**Syntax**

``` sql
formatRowNoNewline(format, x, y, ...)
```

**Arguments**

- `format` — Text format. For example, [CSV](/docs/en/interfaces/formats.md/#csv), [TSV](/docs/en/interfaces/formats.md/#tabseparated).
- `x`,`y`, ... — Expressions.

**Returned value**

- A formatted string.

**Example**

Query:

``` sql
SELECT formatRowNoNewline('CSV', number, 'good')
FROM numbers(3);
```

Result:

```response
┌─formatRowNoNewline('CSV', number, 'good')─┐
│ 0,"good"                                  │
│ 1,"good"                                  │
│ 2,"good"                                  │
└───────────────────────────────────────────┘
```

## snowflakeToDateTime

Extracts the timestamp component of a [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) in [DateTime](/docs/en/sql-reference/data-types/datetime.md) format.

**Syntax**

``` sql
snowflakeToDateTime(value[, time_zone])
```

**Parameters**

- `value` — Snowflake ID. [Int64](/docs/en/sql-reference/data-types/int-uint.md).
- `time_zone` — [Timezone](/docs/en/operations/server-configuration-parameters/settings.md/#server_configuration_parameters-timezone). The function parses `time_string` according to the timezone. Optional. [String](/docs/en/sql-reference/data-types/string.md).

**Returned value**

- The timestamp component of `value` as a [DateTime](/docs/en/sql-reference/data-types/datetime.md) value.

**Example**

Query:

``` sql
SELECT snowflakeToDateTime(CAST('1426860702823350272', 'Int64'), 'UTC');
```

Result:

```response

┌─snowflakeToDateTime(CAST('1426860702823350272', 'Int64'), 'UTC')─┐
│                                              2021-08-15 10:57:56 │
└──────────────────────────────────────────────────────────────────┘
```

## snowflakeToDateTime64

Extracts the timestamp component of a [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) in [DateTime64](/docs/en/sql-reference/data-types/datetime64.md) format.

**Syntax**

``` sql
snowflakeToDateTime64(value[, time_zone])
```

**Parameters**

- `value` — Snowflake ID. [Int64](/docs/en/sql-reference/data-types/int-uint.md).
- `time_zone` — [Timezone](/docs/en/operations/server-configuration-parameters/settings.md/#server_configuration_parameters-timezone). The function parses `time_string` according to the timezone. Optional. [String](/docs/en/sql-reference/data-types/string.md).

**Returned value**

- The timestamp component of `value` as a [DateTime64](/docs/en/sql-reference/data-types/datetime64.md) with scale = 3, i.e. millisecond precision.

**Example**

Query:

``` sql
SELECT snowflakeToDateTime64(CAST('1426860802823350272', 'Int64'), 'UTC');
```

Result:

```response

┌─snowflakeToDateTime64(CAST('1426860802823350272', 'Int64'), 'UTC')─┐
│                                            2021-08-15 10:58:19.841 │
└────────────────────────────────────────────────────────────────────┘
```

## dateTimeToSnowflake

Converts a [DateTime](/docs/en/sql-reference/data-types/datetime.md) value to the first [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) at the giving time.

**Syntax**

``` sql
dateTimeToSnowflake(value)
```

**Parameters**

- `value` — Date with time. [DateTime](/docs/en/sql-reference/data-types/datetime.md).

**Returned value**

- Input value converted to the [Int64](/docs/en/sql-reference/data-types/int-uint.md) data type as the first Snowflake ID at that time.

**Example**

Query:

``` sql
WITH toDateTime('2021-08-15 18:57:56', 'Asia/Shanghai') AS dt SELECT dateTimeToSnowflake(dt);
```

Result:

```response
┌─dateTimeToSnowflake(dt)─┐
│     1426860702823350272 │
└─────────────────────────┘
```

## dateTime64ToSnowflake

Convert a [DateTime64](/docs/en/sql-reference/data-types/datetime64.md) to the first [Snowflake ID](https://en.wikipedia.org/wiki/Snowflake_ID) at the giving time.

**Syntax**

``` sql
dateTime64ToSnowflake(value)
```

**Parameters**

- `value` — Date with time. [DateTime64](/docs/en/sql-reference/data-types/datetime64.md).

**Returned value**

- Input value converted to the [Int64](/docs/en/sql-reference/data-types/int-uint.md) data type as the first Snowflake ID at that time.

**Example**

Query:

``` sql
WITH toDateTime64('2021-08-15 18:57:56.492', 3, 'Asia/Shanghai') AS dt64 SELECT dateTime64ToSnowflake(dt64);
```

Result:

```response
┌─dateTime64ToSnowflake(dt64)─┐
│         1426860704886947840 │
└─────────────────────────────┘
```
