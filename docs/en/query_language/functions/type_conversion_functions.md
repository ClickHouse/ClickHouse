
# Type Conversion Functions

## toInt(8|16|32|64)

Converts an input value to the [Int](../../data_types/int_uint.md) data type. This functions family includes:

* `toInt8(expr)` — Results in `Int8` data type.
* `toInt16(expr)` — Results in `Int16` data type.
* `toInt32(expr)` — Results in `Int32` data type.
* `toInt64(expr)` — Results in `Int64` data type.

**Parameters**

- `expr` — [Expression](../syntax.md#syntax-expressions) returning a number or a string with decimal representation of a number. Binary, octal, and hexadecimal representations of numbers are not supported. Leading zeroes are stripped.

**Returned value**

Integer value in `Int8`, `Int16`, `Int32` or `Int64` data type.

Functions use [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), they truncate fraction digits of numbers.

The behaviour of functions for the [NaN and Inf](../../data_types/float.md#data_type-float-nan-inf) arguments is undefined.

**Example**

```sql
SELECT toInt64(nan), toInt32(32), toInt16('16'), toInt8(8.8)
```
```text
┌─────────toInt64(nan)─┬─toInt32(32)─┬─toInt16('16')─┬─toInt8(8.8)─┐
│ -9223372036854775808 │          32 │            16 │           8 │
└──────────────────────┴─────────────┴───────────────┴─────────────┘
```

## toInt(8|16|32|64)OrZero

## toInt(8|16|32|64)OrNull

## toUInt(8|16|32|64)

Converts an input value to the [UInt](../../data_types/int_uint.md) data type. This functions family includes:

* `toUInt8(expr)` — Results in `UInt8` data type.
* `toUInt16(expr)` — Results in `UInt16` data type.
* `toUInt32(expr)` — Results in `UInt32` data type.
* `toUInt64(expr)` — Results in `UInt64` data type.

**Parameters**

- `expr` — [Expression](../syntax.md#syntax-expressions) returning a number or a string with decimal representation of a number. Binary, octal, and hexadecimal representations of numbers are not supported. Leading zeroes are stripped.

**Returned value**

Integer value in `UInt8`, `UInt16`, `UInt32` or `UInt64` data type.

Functions use [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), they truncate fraction digits of numbers.

The behaviour of functions for negative agruments and for the [NaN and Inf](../../data_types/float.md#data_type-float-nan-inf) arguments is undefined. If you pass the string with negative number, for example `'-32'`, ClickHouse rises an exception.

**Example**

```sql
SELECT toUInt64(nan), toUInt32(-32), toUInt16('16'), toUInt8(8.8)
```
```text
┌───────toUInt64(nan)─┬─toUInt32(-32)─┬─toUInt16('16')─┬─toUInt8(8.8)─┐
│ 9223372036854775808 │    4294967264 │             16 │            8 │
└─────────────────────┴───────────────┴────────────────┴──────────────┘
```

## toUInt(8|16|32|64)OrZero

## toUInt(8|16|32|64)OrNull

## toFloat(32|64)

## toFloat(32|64)OrZero

## toFloat(32|64)OrNull

## toDate

## toDateOrZero

## toDateOrNull

## toDateTime

## toDateTimeOrZero

## toDateTimeOrNull

## toDecimal(32|64|128)

Converts `value` to the [Decimal](../../data_types/decimal.md) data type with precision of `S`. The `value` can be a number or a string. The `S` (scale) parameter specifies the number of decimal places.

- `toDecimal32(value, S)`
- `toDecimal64(value, S)`
- `toDecimal128(value, S)`

## toDecimal(32|64|128)OrNull

Converts an input string to a [Nullable(Decimal(P,S))](../../data_types/decimal.md) data type value. This family of functions include:

- `toDecimal32OrNull(expr, S)` — Results in `Nullable(Decimal32(S))` data type.
- `toDecimal64OrNull(expr, S)` — Results in `Nullable(Decimal64(S))` data type.
- `toDecimal128OrNull(expr, S)` — Results in `Nullable(Decimal128(S))` data type.

These functions should be used instead of `toDecimal*()` functions, if you prefer to get a `NULL` value instead of an exception in the event of an input value parsing error.

**Parameters**

- `expr` — [Expression](../syntax.md#syntax-expressions), returns a value in the [String](../../data_types/string.md) data type. ClickHouse expects the textual representation of the decimal number. For example, `'1.111'`.
- `S` — Scale, the number of decimal places in the resulting value.

**Returned value**

A value in the `Nullable(Decimal(P,S))` data type. The value contains:

- Number with `S` decimal places, if ClickHouse interprets the input string as a number.
- `NULL`, if ClickHouse can't interpret the input string as a number or if the input number contains more than `S` decimal places.

**Examples**

```sql
SELECT toDecimal32OrNull(toString(-1.111), 5) AS val, toTypeName(val)
```
```text
┌──────val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 5))─┐
│ -1.11100 │ Nullable(Decimal(9, 5))                            │
└──────────┴────────────────────────────────────────────────────┘
```
```sql
SELECT toDecimal32OrNull(toString(-1.111), 2) AS val, toTypeName(val)
```
```text
┌──val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 2))─┐
│ ᴺᵁᴸᴸ │ Nullable(Decimal(9, 2))                            │
└──────┴────────────────────────────────────────────────────┘
```


## toDecimal(32|64|128)OrZero

Converts an input value to the [Decimal(P,S)](../../data_types/decimal.md) data type. This family of functions include:

- `toDecimal32OrZero( expr, S)` — Results in `Decimal32(S)` data type.
- `toDecimal64OrZero( expr, S)` — Results in `Decimal64(S)` data type.
- `toDecimal128OrZero( expr, S)` — Results in `Decimal128(S)` data type.

These functions should be used instead of `toDecimal*()` functions, if you prefer to get a `0` value instead of an exception in the event of an input value parsing error.

**Parameters**

- `expr` — [Expression](../syntax.md#syntax-expressions), returns a value in the [String](../../data_types/string.md) data type. ClickHouse expects the textual representation of the decimal number. For example, `'1.111'`.
- `S` — Scale, the number of decimal places in the resulting value.

**Returned value**

A value in the `Nullable(Decimal(P,S))` data type. The value contains:

- Number with `S` decimal places, if ClickHouse interprets the input string as a number.
- 0 with `S` decimal places, if ClickHouse can't interpret the input string as a number or if the input number contains more than `S` decimal places.

**Example**

```sql
SELECT toDecimal32OrZero(toString(-1.111), 5) AS val, toTypeName(val)
```
```text
┌──────val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 5))─┐
│ -1.11100 │ Decimal(9, 5)                                      │
└──────────┴────────────────────────────────────────────────────┘
```
```sql
SELECT toDecimal32OrZero(toString(-1.111), 2) AS val, toTypeName(val)
```
```text
┌──val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 2))─┐
│ 0.00 │ Decimal(9, 2)                                      │
└──────┴────────────────────────────────────────────────────┘
```

## toString

Functions for converting between numbers, strings (but not fixed strings), dates, and dates with times.
All these functions accept one argument.

When converting to or from a string, the value is formatted or parsed using the same rules as for the TabSeparated format (and almost all other text formats). If the string can't be parsed, an exception is thrown and the request is canceled.

When converting dates to numbers or vice versa, the date corresponds to the number of days since the beginning of the Unix epoch.
When converting dates with times to numbers or vice versa, the date with time corresponds to the number of seconds since the beginning of the Unix epoch.

The date and date-with-time formats for the toDate/toDateTime functions are defined as follows:

```
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

As an exception, if converting from UInt32, Int32, UInt64, or Int64 numeric types to Date, and if the number is greater than or equal to 65536, the number is interpreted as a Unix timestamp (and not as the number of days) and is rounded to the date. This allows support for the common occurrence of writing 'toDate(unix_timestamp)', which otherwise would be an error and would require writing the more cumbersome 'toDate(toDateTime(unix_timestamp))'.

Conversion between a date and date with time is performed the natural way: by adding a null time or dropping the time.

Conversion between numeric types uses the same rules as assignments between different numeric types in C++.

Additionally, the toString function of the DateTime argument can take a second String argument containing the name of the time zone. Example: `Asia/Yekaterinburg` In this case, the time is formatted according to the specified time zone.

``` sql
SELECT
    now() AS now_local,
    toString(now(), 'Asia/Yekaterinburg') AS now_yekat
```

```
┌───────────now_local─┬─now_yekat───────────┐
│ 2016-06-15 00:11:21 │ 2016-06-15 02:11:21 │
└─────────────────────┴─────────────────────┘
```

Also see the `toUnixTimestamp` function.

## toFixedString(s, N)

Converts a String type argument to a FixedString(N) type (a string with fixed length N). N must be a constant.
If the string has fewer bytes than N, it is passed with null bytes to the right. If the string has more bytes than N, an exception is thrown.

## toStringCutToZero(s)

Accepts a String or FixedString argument. Returns the String with the content truncated at the first zero byte found.

Example:

``` sql
SELECT toFixedString('foo', 8) AS s, toStringCutToZero(s) AS s_cut
```

```
┌─s─────────────┬─s_cut─┐
│ foo\0\0\0\0\0 │ foo   │
└───────────────┴───────┘
```

``` sql
SELECT toFixedString('foo\0bar', 8) AS s, toStringCutToZero(s) AS s_cut
```

```
┌─s──────────┬─s_cut─┐
│ foo\0bar\0 │ foo   │
└────────────┴───────┘
```

## reinterpretAsUInt(8|16|32|64)

## reinterpretAsInt(8|16|32|64)

## reinterpretAsFloat(32|64)

## reinterpretAsDate

## reinterpretAsDateTime

These functions accept a string and interpret the bytes placed at the beginning of the string as a number in host order (little endian). If the string isn't long enough, the functions work as if the string is padded with the necessary number of null bytes. If the string is longer than needed, the extra bytes are ignored. A date is interpreted as the number of days since the beginning of the Unix Epoch, and a date with time is interpreted as the number of seconds since the beginning of the Unix Epoch.

## reinterpretAsString {#type_conversion_functions-reinterpretAsString}

This function accepts a number or date or date with time, and returns a string containing bytes representing the corresponding value in host order (little endian). Null bytes are dropped from the end. For example, a UInt32 type value of 255 is a string that is one byte long.

## reinterpretAsFixedString

This function accepts a number or date or date with time, and returns a FixedString containing bytes representing the corresponding value in host order (little endian). Null bytes are dropped from the end. For example, a UInt32 type value of 255 is a FixedString that is one byte long.

## CAST(x, t) {#type_conversion_function-cast}

Converts 'x' to the 't' data type. The syntax CAST(x AS t) is also supported.

Example:

``` sql
SELECT
    '2016-06-15 23:00:00' AS timestamp,
    CAST(timestamp AS DateTime) AS datetime,
    CAST(timestamp AS Date) AS date,
    CAST(timestamp, 'String') AS string,
    CAST(timestamp, 'FixedString(22)') AS fixed_string
```

```
┌─timestamp───────────┬────────────datetime─┬───────date─┬─string──────────────┬─fixed_string──────────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00\0\0\0 │
└─────────────────────┴─────────────────────┴────────────┴─────────────────────┴───────────────────────────┘
```

Conversion to FixedString(N) only works for arguments of type String or FixedString(N).

Type conversion to [Nullable](../../data_types/nullable.md) and back is supported. Example:

```
SELECT toTypeName(x) FROM t_null

┌─toTypeName(x)─┐
│ Int8          │
│ Int8          │
└───────────────┘

SELECT toTypeName(CAST(x, 'Nullable(UInt16)')) FROM t_null

┌─toTypeName(CAST(x, 'Nullable(UInt16)'))─┐
│ Nullable(UInt16)                        │
│ Nullable(UInt16)                        │
└─────────────────────────────────────────┘
```

## toInterval(Year|Quarter|Month|Week|Day|Hour|Minute|Second)

Converts a Number type argument to a Interval type (duration).
The interval type is actually very useful, you can use this type of data to perform arithmetic operations directly with Date or DateTime. At the same time, ClickHouse provides a more convenient syntax for declaring Interval type data. For example:

```sql
WITH
    toDate('2019-01-01') AS date,
    INTERVAL 1 WEEK AS interval_week,
    toIntervalWeek(1) AS interval_to_week
SELECT
    date + interval_week,
    date + interval_to_week
```

```
┌─plus(date, interval_week)─┬─plus(date, interval_to_week)─┐
│                2019-01-08 │                   2019-01-08 │
└───────────────────────────┴──────────────────────────────┘
```

## parseDateTimeBestEffort {#type_conversion_functions-parsedatetimebesteffort}

Parse a number type argument to a Date or DateTime type.
different from toDate and toDateTime, parseDateTimeBestEffort can progress more complex date format.
For more information, see the link: [Complex Date Format](https://xkcd.com/1179/)

## parseDateTimeBestEffortOrNull

Same as for [parseDateTimeBestEffort](#type_conversion_functions-parsedatetimebesteffort) except that it returns null when it encounters a date format that cannot be processed.

## parseDateTimeBestEffortOrZero

Same as for [parseDateTimeBestEffort](#type_conversion_functions-parsedatetimebesteffort) except that it returns zero date or zero date time when it encounters a date format that cannot be processed.

[Original article](https://clickhouse.yandex/docs/en/query_language/functions/type_conversion_functions/) <!--hide-->
