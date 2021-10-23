---
toc_priority: 38
toc_title: Type Conversion
---

# Type Conversion Functions {#type-conversion-functions}

## Common Issues of Numeric Conversions {#numeric-conversion-issues}

When you convert a value from one to another data type, you should remember that in common case, it is an unsafe operation that can lead to a data loss. A data loss can occur if you try to fit value from a larger data type to a smaller data type, or if you convert values between different data types.

ClickHouse has the [same behavior as C++ programs](https://en.cppreference.com/w/cpp/language/implicit_conversion).

## toInt(8\|16\|32\|64\|128\|256) {#toint8163264128256}

Converts an input value to the [Int](../../sql-reference/data-types/int-uint.md) data type. This function family includes:

-   `toInt8(expr)` — Results in the `Int8` data type.
-   `toInt16(expr)` — Results in the `Int16` data type.
-   `toInt32(expr)` — Results in the `Int32` data type.
-   `toInt64(expr)` — Results in the `Int64` data type.
-   `toInt128(expr)` — Results in the `Int128` data type.
-   `toInt256(expr)` — Results in the `Int256` data type.

**Arguments**

-   `expr` — [Expression](../../sql-reference/syntax.md#syntax-expressions) returning a number or a string with the decimal representation of a number. Binary, octal, and hexadecimal representations of numbers are not supported. Leading zeroes are stripped.

**Returned value**

Integer value in the `Int8`, `Int16`, `Int32`, `Int64`, `Int128` or `Int256` data type.

Functions use [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning they truncate fractional digits of numbers.

The behavior of functions for the [NaN and Inf](../../sql-reference/data-types/float.md#data_type-float-nan-inf) arguments is undefined. Remember about [numeric convertions issues](#numeric-conversion-issues), when using the functions.

**Example**

Query:

``` sql
SELECT toInt64(nan), toInt32(32), toInt16('16'), toInt8(8.8);
```

Result:

``` text
┌─────────toInt64(nan)─┬─toInt32(32)─┬─toInt16('16')─┬─toInt8(8.8)─┐
│ -9223372036854775808 │          32 │            16 │           8 │
└──────────────────────┴─────────────┴───────────────┴─────────────┘
```

## toInt(8\|16\|32\|64\|128\|256)OrZero {#toint8163264orzero}

It takes an argument of type String and tries to parse it into Int (8 \| 16 \| 32 \| 64 \| 128 \| 256). If failed, returns 0.

**Example**

Query:

``` sql
SELECT toInt64OrZero('123123'), toInt8OrZero('123qwe123');
```

Result:

``` text
┌─toInt64OrZero('123123')─┬─toInt8OrZero('123qwe123')─┐
│                  123123 │                         0 │
└─────────────────────────┴───────────────────────────┘
```

## toInt(8\|16\|32\|64\|128\|256)OrNull {#toint8163264128256ornull}

It takes an argument of type String and tries to parse it into Int (8 \| 16 \| 32 \| 64 \| 128 \| 256). If failed, returns NULL.

**Example**

Query:

``` sql
SELECT toInt64OrNull('123123'), toInt8OrNull('123qwe123');
```

Result:

``` text
┌─toInt64OrNull('123123')─┬─toInt8OrNull('123qwe123')─┐
│                  123123 │                      ᴺᵁᴸᴸ │
└─────────────────────────┴───────────────────────────┘
```

## toUInt(8\|16\|32\|64\|256) {#touint8163264256}

Converts an input value to the [UInt](../../sql-reference/data-types/int-uint.md) data type. This function family includes:

-   `toUInt8(expr)` — Results in the `UInt8` data type.
-   `toUInt16(expr)` — Results in the `UInt16` data type.
-   `toUInt32(expr)` — Results in the `UInt32` data type.
-   `toUInt64(expr)` — Results in the `UInt64` data type.
-   `toUInt256(expr)` — Results in the `UInt256` data type.

**Arguments**

-   `expr` — [Expression](../../sql-reference/syntax.md#syntax-expressions) returning a number or a string with the decimal representation of a number. Binary, octal, and hexadecimal representations of numbers are not supported. Leading zeroes are stripped.

**Returned value**

Integer value in the `UInt8`, `UInt16`, `UInt32`, `UInt64` or `UInt256` data type.

Functions use [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), meaning they truncate fractional digits of numbers.

The behavior of functions for negative agruments and for the [NaN and Inf](../../sql-reference/data-types/float.md#data_type-float-nan-inf) arguments is undefined. If you pass a string with a negative number, for example `'-32'`, ClickHouse raises an exception. Remember about [numeric convertions issues](#numeric-conversion-issues), when using the functions.

**Example**

Query:

``` sql
SELECT toUInt64(nan), toUInt32(-32), toUInt16('16'), toUInt8(8.8);
```

Result:

``` text
┌───────toUInt64(nan)─┬─toUInt32(-32)─┬─toUInt16('16')─┬─toUInt8(8.8)─┐
│ 9223372036854775808 │    4294967264 │             16 │            8 │
└─────────────────────┴───────────────┴────────────────┴──────────────┘
```

## toUInt(8\|16\|32\|64\|256)OrZero {#touint8163264256orzero}

## toUInt(8\|16\|32\|64\|256)OrNull {#touint8163264256ornull}

## toFloat(32\|64) {#tofloat3264}

## toFloat(32\|64)OrZero {#tofloat3264orzero}

## toFloat(32\|64)OrNull {#tofloat3264ornull}

## toDate {#todate}

Alias: `DATE`.

## toDateOrZero {#todateorzero}

## toDateOrNull {#todateornull}

## toDateTime {#todatetime}

## toDateTimeOrZero {#todatetimeorzero}

## toDateTimeOrNull {#todatetimeornull}

## toDecimal(32\|64\|128\|256) {#todecimal3264128256}

Converts `value` to the [Decimal](../../sql-reference/data-types/decimal.md) data type with precision of `S`. The `value` can be a number or a string. The `S` (scale) parameter specifies the number of decimal places.

-   `toDecimal32(value, S)`
-   `toDecimal64(value, S)`
-   `toDecimal128(value, S)`
-   `toDecimal256(value, S)`

## toDecimal(32\|64\|128\|256)OrNull {#todecimal3264128256ornull}

Converts an input string to a [Nullable(Decimal(P,S))](../../sql-reference/data-types/decimal.md) data type value. This family of functions include:

-   `toDecimal32OrNull(expr, S)` — Results in `Nullable(Decimal32(S))` data type.
-   `toDecimal64OrNull(expr, S)` — Results in `Nullable(Decimal64(S))` data type.
-   `toDecimal128OrNull(expr, S)` — Results in `Nullable(Decimal128(S))` data type.
-   `toDecimal256OrNull(expr, S)` — Results in `Nullable(Decimal256(S))` data type.

These functions should be used instead of `toDecimal*()` functions, if you prefer to get a `NULL` value instead of an exception in the event of an input value parsing error.

**Arguments**

-   `expr` — [Expression](../../sql-reference/syntax.md#syntax-expressions), returns a value in the [String](../../sql-reference/data-types/string.md) data type. ClickHouse expects the textual representation of the decimal number. For example, `'1.111'`.
-   `S` — Scale, the number of decimal places in the resulting value.

**Returned value**

A value in the `Nullable(Decimal(P,S))` data type. The value contains:

-   Number with `S` decimal places, if ClickHouse interprets the input string as a number.
-   `NULL`, if ClickHouse can’t interpret the input string as a number or if the input number contains more than `S` decimal places.

**Examples**

Query:

``` sql
SELECT toDecimal32OrNull(toString(-1.111), 5) AS val, toTypeName(val);
```

Result:

``` text
┌──────val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 5))─┐
│ -1.11100 │ Nullable(Decimal(9, 5))                            │
└──────────┴────────────────────────────────────────────────────┘
```

Query:

``` sql
SELECT toDecimal32OrNull(toString(-1.111), 2) AS val, toTypeName(val);
```

Result:

``` text
┌──val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 2))─┐
│ ᴺᵁᴸᴸ │ Nullable(Decimal(9, 2))                            │
└──────┴────────────────────────────────────────────────────┘
```

## toDecimal(32\|64\|128\|256)OrZero {#todecimal3264128256orzero}

Converts an input value to the [Decimal(P,S)](../../sql-reference/data-types/decimal.md) data type. This family of functions include:

-   `toDecimal32OrZero( expr, S)` — Results in `Decimal32(S)` data type.
-   `toDecimal64OrZero( expr, S)` — Results in `Decimal64(S)` data type.
-   `toDecimal128OrZero( expr, S)` — Results in `Decimal128(S)` data type.
-   `toDecimal256OrZero( expr, S)` — Results in `Decimal256(S)` data type.

These functions should be used instead of `toDecimal*()` functions, if you prefer to get a `0` value instead of an exception in the event of an input value parsing error.

**Arguments**

-   `expr` — [Expression](../../sql-reference/syntax.md#syntax-expressions), returns a value in the [String](../../sql-reference/data-types/string.md) data type. ClickHouse expects the textual representation of the decimal number. For example, `'1.111'`.
-   `S` — Scale, the number of decimal places in the resulting value.

**Returned value**

A value in the `Nullable(Decimal(P,S))` data type. The value contains:

-   Number with `S` decimal places, if ClickHouse interprets the input string as a number.
-   0 with `S` decimal places, if ClickHouse can’t interpret the input string as a number or if the input number contains more than `S` decimal places.

**Example**

Query:

``` sql
SELECT toDecimal32OrZero(toString(-1.111), 5) AS val, toTypeName(val);
```

Result:

``` text
┌──────val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 5))─┐
│ -1.11100 │ Decimal(9, 5)                                      │
└──────────┴────────────────────────────────────────────────────┘
```

Query:

``` sql
SELECT toDecimal32OrZero(toString(-1.111), 2) AS val, toTypeName(val);
```

Result:

``` text
┌──val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 2))─┐
│ 0.00 │ Decimal(9, 2)                                      │
└──────┴────────────────────────────────────────────────────┘
```

## toString {#tostring}

Functions for converting between numbers, strings (but not fixed strings), dates, and dates with times.
All these functions accept one argument.

When converting to or from a string, the value is formatted or parsed using the same rules as for the TabSeparated format (and almost all other text formats). If the string can’t be parsed, an exception is thrown and the request is canceled.

When converting dates to numbers or vice versa, the date corresponds to the number of days since the beginning of the Unix epoch.
When converting dates with times to numbers or vice versa, the date with time corresponds to the number of seconds since the beginning of the Unix epoch.

The date and date-with-time formats for the toDate/toDateTime functions are defined as follows:

``` text
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

As an exception, if converting from UInt32, Int32, UInt64, or Int64 numeric types to Date, and if the number is greater than or equal to 65536, the number is interpreted as a Unix timestamp (and not as the number of days) and is rounded to the date. This allows support for the common occurrence of writing ‘toDate(unix_timestamp)’, which otherwise would be an error and would require writing the more cumbersome ‘toDate(toDateTime(unix_timestamp))’.

Conversion between a date and date with time is performed the natural way: by adding a null time or dropping the time.

Conversion between numeric types uses the same rules as assignments between different numeric types in C++.

Additionally, the toString function of the DateTime argument can take a second String argument containing the name of the time zone. Example: `Asia/Yekaterinburg` In this case, the time is formatted according to the specified time zone.

**Example**

Query:

``` sql
SELECT
    now() AS now_local,
    toString(now(), 'Asia/Yekaterinburg') AS now_yekat;
```

Result:

``` text
┌───────────now_local─┬─now_yekat───────────┐
│ 2016-06-15 00:11:21 │ 2016-06-15 02:11:21 │
└─────────────────────┴─────────────────────┘
```

Also see the `toUnixTimestamp` function.

## toFixedString(s, N) {#tofixedstrings-n}

Converts a String type argument to a FixedString(N) type (a string with fixed length N). N must be a constant.
If the string has fewer bytes than N, it is padded with null bytes to the right. If the string has more bytes than N, an exception is thrown.

## toStringCutToZero(s) {#tostringcuttozeros}

Accepts a String or FixedString argument. Returns the String with the content truncated at the first zero byte found.

**Example**

Query:

``` sql
SELECT toFixedString('foo', 8) AS s, toStringCutToZero(s) AS s_cut;
```

Result:

``` text
┌─s─────────────┬─s_cut─┐
│ foo\0\0\0\0\0 │ foo   │
└───────────────┴───────┘
```

Query:

``` sql
SELECT toFixedString('foo\0bar', 8) AS s, toStringCutToZero(s) AS s_cut;
```

Result:

``` text
┌─s──────────┬─s_cut─┐
│ foo\0bar\0 │ foo   │
└────────────┴───────┘
```

## reinterpretAsUInt(8\|16\|32\|64) {#reinterpretasuint8163264}

## reinterpretAsInt(8\|16\|32\|64) {#reinterpretasint8163264}

## reinterpretAsFloat(32\|64) {#reinterpretasfloat3264}

## reinterpretAsDate {#reinterpretasdate}

## reinterpretAsDateTime {#reinterpretasdatetime}

These functions accept a string and interpret the bytes placed at the beginning of the string as a number in host order (little endian). If the string isn’t long enough, the functions work as if the string is padded with the necessary number of null bytes. If the string is longer than needed, the extra bytes are ignored. A date is interpreted as the number of days since the beginning of the Unix Epoch, and a date with time is interpreted as the number of seconds since the beginning of the Unix Epoch.

## reinterpretAsString {#type_conversion_functions-reinterpretAsString}

This function accepts a number or date or date with time, and returns a string containing bytes representing the corresponding value in host order (little endian). Null bytes are dropped from the end. For example, a UInt32 type value of 255 is a string that is one byte long.

## reinterpretAsFixedString {#reinterpretasfixedstring}

This function accepts a number or date or date with time, and returns a FixedString containing bytes representing the corresponding value in host order (little endian). Null bytes are dropped from the end. For example, a UInt32 type value of 255 is a FixedString that is one byte long.

## reinterpretAsUUID {#reinterpretasuuid}

Accepts 16 bytes string and returns UUID containing bytes representing the corresponding value in network byte order (big-endian). If the string isn't long enough, the function works as if the string is padded with the necessary number of null bytes to the end. If the string longer than 16 bytes, the extra bytes at the end are ignored. 

**Syntax**

``` sql
reinterpretAsUUID(fixed_string)
```

**Arguments**

-   `fixed_string` — Big-endian byte string. [FixedString](../../sql-reference/data-types/fixedstring.md#fixedstring).

**Returned value**

-   The UUID type value. [UUID](../../sql-reference/data-types/uuid.md#uuid-data-type).

**Examples**

String to UUID.

Query:

``` sql
SELECT reinterpretAsUUID(reverse(unhex('000102030405060708090a0b0c0d0e0f')));
```

Result:

``` text
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

``` text
┌─equals(uuid, uuid2)─┐
│                   1 │
└─────────────────────┘
```

## reinterpret(x, T) {#type_conversion_function-reinterpret}

Uses the same source in-memory bytes sequence for `x` value and reinterprets it to destination type.

**Syntax**

``` sql
reinterpret(x, type)
```

**Arguments**

-   `x` — Any type. 
-   `type` — Destination type. [String](../../sql-reference/data-types/string.md). 

**Returned value**

-   Destination type value.

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

## CAST(x, T) {#type_conversion_function-cast}

Converts input value `x` to the `T` data type. Unlike to `reinterpret` function, type conversion is performed in a natural way.

The syntax `CAST(x AS t)` is also supported.

!!! note "Note"
    If value `x` does not fit the bounds of type `T`, the function overflows. For example, `CAST(-1, 'UInt8')` returns `255`.

**Syntax**

``` sql
CAST(x, T)
```

**Arguments**

-   `x` — Any type. 
-   `T` — Destination type. [String](../../sql-reference/data-types/string.md).  

**Returned value**

-   Destination type value.

**Examples**

Query:

```sql
SELECT
    CAST(toInt8(-1), 'UInt8') AS cast_int_to_uint,
    CAST(toInt8(1), 'Float32') AS cast_int_to_float,
    CAST('1', 'UInt32') AS cast_string_to_int;
```

Result:

```
┌─cast_int_to_uint─┬─cast_int_to_float─┬─cast_string_to_int─┐
│              255 │                 1 │                  1 │
└──────────────────┴───────────────────┴────────────────────┘
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

``` text
┌─timestamp───────────┬────────────datetime─┬───────date─┬─string──────────────┬─fixed_string──────────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00\0\0\0 │
└─────────────────────┴─────────────────────┴────────────┴─────────────────────┴───────────────────────────┘
```

Conversion to FixedString(N) only works for arguments of type [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).

Type conversion to [Nullable](../../sql-reference/data-types/nullable.md) and back is supported. 

**Example**

Query:

``` sql
SELECT toTypeName(x) FROM t_null;
```

Result:

``` text
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

``` text
┌─toTypeName(CAST(x, 'Nullable(UInt16)'))─┐
│ Nullable(UInt16)                        │
│ Nullable(UInt16)                        │
└─────────────────────────────────────────┘
```

**See also**

-   [cast_keep_nullable](../../operations/settings/settings.md#cast_keep_nullable) setting

## accurateCast(x, T) {#type_conversion_function-accurate-cast}

Converts `x` to the `T` data type. 

The difference from [cast(x, T)](#type_conversion_function-cast) is that `accurateCast` does not allow overflow of numeric types during cast if type value `x` does not fit the bounds of type `T`. For example, `accurateCast(-1, 'UInt8')` throws an exception.

**Example**

Query:

``` sql
SELECT cast(-1, 'UInt8') as uint8;
```

Result:

``` text
┌─uint8─┐
│   255 │
└───────┘
```

Query:

```sql
SELECT accurateCast(-1, 'UInt8') as uint8;
```

Result:

``` text
Code: 70. DB::Exception: Received from localhost:9000. DB::Exception: Value in column Int8 cannot be safely converted into type UInt8: While processing accurateCast(-1, 'UInt8') AS uint8.
```

## accurateCastOrNull(x, T) {#type_conversion_function-accurate-cast_or_null}

Converts input value `x` to the specified data type `T`. Always returns [Nullable](../../sql-reference/data-types/nullable.md) type and returns [NULL](../../sql-reference/syntax.md#null-literal) if the casted value is not representable in the target type.

**Syntax**

```sql
accurateCastOrNull(x, T)
```

**Parameters**

-   `x` — Input value.
-   `T` — The name of the returned data type.

**Returned value**

-   The value, converted to the specified data type `T`.

**Example**

Query:

``` sql
SELECT toTypeName(accurateCastOrNull(5, 'UInt8'));
```

Result:

``` text
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

``` text
┌─uint8─┬─int8─┬─fixed_string─┐
│  ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │
└───────┴──────┴──────────────┘
```

## toInterval(Year\|Quarter\|Month\|Week\|Day\|Hour\|Minute\|Second) {#function-tointerval}

Converts a Number type argument to an [Interval](../../sql-reference/data-types/special-data-types/interval.md) data type.

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

-   `number` — Duration of interval. Positive integer number.

**Returned values**

-   The value in `Interval` data type.

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

``` text
┌─plus(date, interval_week)─┬─plus(date, interval_to_week)─┐
│                2019-01-08 │                   2019-01-08 │
└───────────────────────────┴──────────────────────────────┘
```

## parseDateTimeBestEffort {#parsedatetimebesteffort}
## parseDateTime32BestEffort {#parsedatetime32besteffort}

Converts a date and time in the [String](../../sql-reference/data-types/string.md) representation to [DateTime](../../sql-reference/data-types/datetime.md#data_type-datetime) data type.

The function parses [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601), [RFC 1123 - 5.2.14 RFC-822 Date and Time Specification](https://tools.ietf.org/html/rfc1123#page-55), ClickHouse’s and some other date and time formats.

**Syntax**

``` sql
parseDateTimeBestEffort(time_string [, time_zone])
```

**Arguments**

-   `time_string` — String containing a date and time to convert. [String](../../sql-reference/data-types/string.md).
-   `time_zone` — Time zone. The function parses `time_string` according to the time zone. [String](../../sql-reference/data-types/string.md).

**Supported non-standard formats**

-   A string containing 9..10 digit [unix timestamp](https://en.wikipedia.org/wiki/Unix_time).
-   A string with a date and a time component: `YYYYMMDDhhmmss`, `DD/MM/YYYY hh:mm:ss`, `DD-MM-YY hh:mm`, `YYYY-MM-DD hh:mm:ss`, etc.
-   A string with a date, but no time component: `YYYY`, `YYYYMM`, `YYYY*MM`, `DD/MM/YYYY`, `DD-MM-YY` etc.
-   A string with a day and time: `DD`, `DD hh`, `DD hh:mm`. In this case `YYYY-MM` are substituted as `2000-01`.
-   A string that includes the date and time along with time zone offset information: `YYYY-MM-DD hh:mm:ss ±h:mm`, etc. For example, `2020-12-12 17:36:00 -5:00`.

For all of the formats with separator the function parses months names expressed by their full name or by the first three letters of a month name. Examples: `24/DEC/18`, `24-Dec-18`, `01-September-2018`.

**Returned value**

-   `time_string` converted to the `DateTime` data type.

**Examples**

Query:

``` sql
SELECT parseDateTimeBestEffort('12/12/2020 12:12:57')
AS parseDateTimeBestEffort;
```

Result:

``` text
┌─parseDateTimeBestEffort─┐
│     2020-12-12 12:12:57 │
└─────────────────────────┘
```

Query:

``` sql
SELECT parseDateTimeBestEffort('Sat, 18 Aug 2018 07:22:16 GMT', 'Europe/Moscow')
AS parseDateTimeBestEffort;
```

Result:

``` text
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

``` text
┌─parseDateTimeBestEffort─┐
│     2015-07-07 12:04:41 │
└─────────────────────────┘
```

Query:

``` sql
SELECT parseDateTimeBestEffort('2018-12-12 10:12:12')
AS parseDateTimeBestEffort;
```

Result:

``` text
┌─parseDateTimeBestEffort─┐
│     2018-12-12 10:12:12 │
└─────────────────────────┘
```

Query:

``` sql
SELECT parseDateTimeBestEffort('10 20:19');
```

Result:

``` text
┌─parseDateTimeBestEffort('10 20:19')─┐
│                 2000-01-10 20:19:00 │
└─────────────────────────────────────┘
```

**See Also**

-   [ISO 8601 announcement by @xkcd](https://xkcd.com/1179/)
-   [RFC 1123](https://tools.ietf.org/html/rfc1123)
-   [toDate](#todate)
-   [toDateTime](#todatetime)

## parseDateTimeBestEffortUS {#parsedatetimebesteffortUS}

This function is similar to [parseDateTimeBestEffort](#parsedatetimebesteffort), the only difference is that this function prefers US date format (`MM/DD/YYYY` etc.) in case of ambiguity.

**Syntax**

``` sql
parseDateTimeBestEffortUS(time_string [, time_zone])
```

**Arguments**

-   `time_string` — String containing a date and time to convert. [String](../../sql-reference/data-types/string.md).
-   `time_zone` — Time zone. The function parses `time_string` according to the time zone. [String](../../sql-reference/data-types/string.md).

**Supported non-standard formats**

-   A string containing 9..10 digit [unix timestamp](https://en.wikipedia.org/wiki/Unix_time).
-   A string with a date and a time component: `YYYYMMDDhhmmss`, `MM/DD/YYYY hh:mm:ss`, `MM-DD-YY hh:mm`, `YYYY-MM-DD hh:mm:ss`, etc.
-   A string with a date, but no time component: `YYYY`, `YYYYMM`, `YYYY*MM`, `MM/DD/YYYY`, `MM-DD-YY` etc.
-   A string with a day and time: `DD`, `DD hh`, `DD hh:mm`. In this case, `YYYY-MM` are substituted as `2000-01`.
-   A string that includes the date and time along with time zone offset information: `YYYY-MM-DD hh:mm:ss ±h:mm`, etc. For example, `2020-12-12 17:36:00 -5:00`.

**Returned value**

-   `time_string` converted to the `DateTime` data type.

**Examples**

Query:

``` sql
SELECT parseDateTimeBestEffortUS('09/12/2020 12:12:57')
AS parseDateTimeBestEffortUS;
```

Result:

``` text
┌─parseDateTimeBestEffortUS─┐
│     2020-09-12 12:12:57   │
└─────────────────────────——┘
```

Query:

``` sql
SELECT parseDateTimeBestEffortUS('09-12-2020 12:12:57')
AS parseDateTimeBestEffortUS;
```

Result:

``` text
┌─parseDateTimeBestEffortUS─┐
│     2020-09-12 12:12:57   │
└─────────────────────────——┘
```

Query:

``` sql
SELECT parseDateTimeBestEffortUS('09.12.2020 12:12:57')
AS parseDateTimeBestEffortUS;
```

Result:

``` text
┌─parseDateTimeBestEffortUS─┐
│     2020-09-12 12:12:57   │
└─────────────────────────——┘
```

## parseDateTimeBestEffortOrNull {#parsedatetimebesteffortornull}
## parseDateTime32BestEffortOrNull {#parsedatetime32besteffortornull}

Same as for [parseDateTimeBestEffort](#parsedatetimebesteffort) except that it returns `NULL` when it encounters a date format that cannot be processed.

## parseDateTimeBestEffortOrZero {#parsedatetimebesteffortorzero}
## parseDateTime32BestEffortOrZero {#parsedatetime32besteffortorzero}

Same as for [parseDateTimeBestEffort](#parsedatetimebesteffort) except that it returns zero date or zero date time when it encounters a date format that cannot be processed.

## parseDateTimeBestEffortUSOrNull {#parsedatetimebesteffortusornull}

Same as [parseDateTimeBestEffortUS](#parsedatetimebesteffortUS) function except that it returns `NULL` when it encounters a date format that cannot be processed.

**Syntax**

``` sql
parseDateTimeBestEffortUSOrNull(time_string[, time_zone])
```

**Parameters**

-   `time_string` — String containing a date or date with time to convert. The date must be in the US date format (`MM/DD/YYYY`, etc). [String](../../sql-reference/data-types/string.md).
-   `time_zone` — [Timezone](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone). The function parses `time_string` according to the timezone. Optional. [String](../../sql-reference/data-types/string.md).

**Supported non-standard formats**

-   A string containing 9..10 digit [unix timestamp](https://en.wikipedia.org/wiki/Unix_time).
-   A string with a date and a time components: `YYYYMMDDhhmmss`, `MM/DD/YYYY hh:mm:ss`, `MM-DD-YY hh:mm`, `YYYY-MM-DD hh:mm:ss`, etc.
-   A string with a date, but no time component: `YYYY`, `YYYYMM`, `YYYY*MM`, `MM/DD/YYYY`, `MM-DD-YY`, etc.
-   A string with a day and time: `DD`, `DD hh`, `DD hh:mm`. In this case, `YYYY-MM` are substituted with `2000-01`.
-   A string that includes date and time along with timezone offset information: `YYYY-MM-DD hh:mm:ss ±h:mm`, etc. For example, `2020-12-12 17:36:00 -5:00`.

**Returned values**

-   `time_string` converted to the [DateTime](../../sql-reference/data-types/datetime.md) data type.
-   `NULL` if the input string cannot be converted to the `DateTime` data type.

**Examples**

Query:

``` sql
SELECT parseDateTimeBestEffortUSOrNull('02/10/2021 21:12:57') AS parseDateTimeBestEffortUSOrNull;
```

Result:

``` text
┌─parseDateTimeBestEffortUSOrNull─┐
│             2021-02-10 21:12:57 │
└─────────────────────────────────┘
```

Query:

``` sql
SELECT parseDateTimeBestEffortUSOrNull('02-10-2021 21:12:57 GMT', 'Europe/Moscow') AS parseDateTimeBestEffortUSOrNull;
```

Result:

``` text
┌─parseDateTimeBestEffortUSOrNull─┐
│             2021-02-11 00:12:57 │
└─────────────────────────────────┘
```

Query:

``` sql
SELECT parseDateTimeBestEffortUSOrNull('02.10.2021') AS parseDateTimeBestEffortUSOrNull;
```

Result:

``` text
┌─parseDateTimeBestEffortUSOrNull─┐
│             2021-02-10 00:00:00 │
└─────────────────────────────────┘
```

Query:

``` sql
SELECT parseDateTimeBestEffortUSOrNull('10.2021') AS parseDateTimeBestEffortUSOrNull;
```

Result:

``` text
┌─parseDateTimeBestEffortUSOrNull─┐
│                            ᴺᵁᴸᴸ │
└─────────────────────────────────┘
```

## parseDateTimeBestEffortUSOrZero {#parsedatetimebesteffortusorzero}

Same as [parseDateTimeBestEffortUS](#parsedatetimebesteffortUS) function except that it returns zero date (`1970-01-01`) or zero date with time (`1970-01-01 00:00:00`) when it encounters a date format that cannot be processed.

**Syntax**

``` sql
parseDateTimeBestEffortUSOrZero(time_string[, time_zone])
```

**Parameters**

-   `time_string` — String containing a date or date with time to convert. The date must be in the US date format (`MM/DD/YYYY`, etc). [String](../../sql-reference/data-types/string.md).
-   `time_zone` — [Timezone](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone). The function parses `time_string` according to the timezone. Optional. [String](../../sql-reference/data-types/string.md).

**Supported non-standard formats**

-   A string containing 9..10 digit [unix timestamp](https://en.wikipedia.org/wiki/Unix_time).
-   A string with a date and a time components: `YYYYMMDDhhmmss`, `MM/DD/YYYY hh:mm:ss`, `MM-DD-YY hh:mm`, `YYYY-MM-DD hh:mm:ss`, etc.
-   A string with a date, but no time component: `YYYY`, `YYYYMM`, `YYYY*MM`, `MM/DD/YYYY`, `MM-DD-YY`, etc.
-   A string with a day and time: `DD`, `DD hh`, `DD hh:mm`. In this case, `YYYY-MM` are substituted with `2000-01`.
-   A string that includes date and time along with timezone offset information: `YYYY-MM-DD hh:mm:ss ±h:mm`, etc. For example, `2020-12-12 17:36:00 -5:00`.

**Returned values**

-   `time_string` converted to the [DateTime](../../sql-reference/data-types/datetime.md) data type.
-   Zero date or zero date with time if the input string cannot be converted to the `DateTime` data type.

**Examples**

Query:

``` sql
SELECT parseDateTimeBestEffortUSOrZero('02/10/2021 21:12:57') AS parseDateTimeBestEffortUSOrZero;
```

Result:

``` text
┌─parseDateTimeBestEffortUSOrZero─┐
│             2021-02-10 21:12:57 │
└─────────────────────────────────┘
```

Query:

``` sql
SELECT parseDateTimeBestEffortUSOrZero('02-10-2021 21:12:57 GMT', 'Europe/Moscow') AS parseDateTimeBestEffortUSOrZero;
```

Result:

``` text
┌─parseDateTimeBestEffortUSOrZero─┐
│             2021-02-11 00:12:57 │
└─────────────────────────────────┘
```

Query:

``` sql
SELECT parseDateTimeBestEffortUSOrZero('02.10.2021') AS parseDateTimeBestEffortUSOrZero;
```

Result:

``` text
┌─parseDateTimeBestEffortUSOrZero─┐
│             2021-02-10 00:00:00 │
└─────────────────────────────────┘
```

Query:

``` sql
SELECT parseDateTimeBestEffortUSOrZero('02.2021') AS parseDateTimeBestEffortUSOrZero;
```

Result:

``` text
┌─parseDateTimeBestEffortUSOrZero─┐
│             1970-01-01 00:00:00 │
└─────────────────────────────────┘
```

## parseDateTime64BestEffort {#parsedatetime64besteffort}

Same as [parseDateTimeBestEffort](#parsedatetimebesteffort) function but also parse milliseconds and microseconds and returns [DateTime](../../sql-reference/functions/type-conversion-functions.md#data_type-datetime) data type.

**Syntax**

``` sql
parseDateTime64BestEffort(time_string [, precision [, time_zone]])
```

**Parameters**

-   `time_string` — String containing a date or date with time to convert. [String](../../sql-reference/data-types/string.md).
-   `precision` — Required precision. `3` — for milliseconds, `6` — for microseconds. Default — `3`. Optional. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `time_zone` — [Timezone](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone). The function parses `time_string` according to the timezone. Optional. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   `time_string` converted to the [DateTime](../../sql-reference/data-types/datetime.md) data type.

**Examples**

Query:

```sql
SELECT parseDateTime64BestEffort('2021-01-01') AS a, toTypeName(a) AS t
UNION ALL
SELECT parseDateTime64BestEffort('2021-01-01 01:01:00.12346') AS a, toTypeName(a) AS t
UNION ALL
SELECT parseDateTime64BestEffort('2021-01-01 01:01:00.12346',6) AS a, toTypeName(a) AS t
UNION ALL
SELECT parseDateTime64BestEffort('2021-01-01 01:01:00.12346',3,'Europe/Moscow') AS a, toTypeName(a) AS t
FORMAT PrettyCompactMonoBlock;
```

Result:

```
┌──────────────────────────a─┬─t──────────────────────────────┐
│ 2021-01-01 01:01:00.123000 │ DateTime64(3)                  │
│ 2021-01-01 00:00:00.000000 │ DateTime64(3)                  │
│ 2021-01-01 01:01:00.123460 │ DateTime64(6)                  │
│ 2020-12-31 22:01:00.123000 │ DateTime64(3, 'Europe/Moscow') │
└────────────────────────────┴────────────────────────────────┘
```

## parseDateTime64BestEffortOrNull {#parsedatetime32besteffortornull}

Same as for [parseDateTime64BestEffort](#parsedatetime64besteffort) except that it returns `NULL` when it encounters a date format that cannot be processed.

## parseDateTime64BestEffortOrZero {#parsedatetime64besteffortorzero}

Same as for [parseDateTime64BestEffort](#parsedatetimebesteffort) except that it returns zero date or zero date time when it encounters a date format that cannot be processed.


## toLowCardinality {#tolowcardinality}

Converts input parameter to the [LowCardianlity](../../sql-reference/data-types/lowcardinality.md) version of same data type.

To convert data from the `LowCardinality` data type use the [CAST](#type_conversion_function-cast) function. For example, `CAST(x as String)`.

**Syntax**

```sql
toLowCardinality(expr)
```

**Arguments**

-   `expr` — [Expression](../../sql-reference/syntax.md#syntax-expressions) resulting in one of the [supported data types](../../sql-reference/data-types/index.md#data_types).

**Returned values**

-   Result of `expr`.

Type: `LowCardinality(expr_result_type)`

**Example**

Query:

```sql
SELECT toLowCardinality('1');
```

Result:

``` text
┌─toLowCardinality('1')─┐
│ 1                     │
└───────────────────────┘
```

## toUnixTimestamp64Milli {#tounixtimestamp64milli}

## toUnixTimestamp64Micro {#tounixtimestamp64micro}

## toUnixTimestamp64Nano {#tounixtimestamp64nano}

Converts a `DateTime64` to a `Int64` value with fixed sub-second precision. Input value is scaled up or down appropriately depending on it precision. 

!!! info "Note"
    The output value is a timestamp in UTC, not in the timezone of `DateTime64`.

**Syntax**

```sql
toUnixTimestamp64Milli(value)
```

**Arguments**

-   `value` — DateTime64 value with any precision.

**Returned value**

-   `value` converted to the `Int64` data type.

**Examples**

Query:

```sql
WITH toDateTime64('2019-09-16 19:20:12.345678910', 6) AS dt64
SELECT toUnixTimestamp64Milli(dt64);
```

Result:

``` text
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

``` text
┌─toUnixTimestamp64Nano(dt64)─┐
│         1568650812345678000 │
└─────────────────────────────┘
```

## fromUnixTimestamp64Milli {#fromunixtimestamp64milli}

## fromUnixTimestamp64Micro {#fromunixtimestamp64micro}

## fromUnixTimestamp64Nano {#fromunixtimestamp64nano}

Converts an `Int64` to a `DateTime64` value with fixed sub-second precision and optional timezone. Input value is scaled up or down appropriately depending on it’s precision. Please note that input value is treated as UTC timestamp, not timestamp at given (or implicit) timezone.

**Syntax**

``` sql
fromUnixTimestamp64Milli(value [, ti])
```

**Arguments**

-   `value` — `Int64` value with any precision.
-   `timezone` — `String` (optional) timezone name of the result.

**Returned value**

-   `value` converted to the `DateTime64` data type.

**Example**

Query:

``` sql
WITH CAST(1234567891011, 'Int64') AS i64
SELECT fromUnixTimestamp64Milli(i64, 'UTC');
```

Result:

``` text
┌─fromUnixTimestamp64Milli(i64, 'UTC')─┐
│              2009-02-13 23:31:31.011 │
└──────────────────────────────────────┘
```

## formatRow {#formatrow}

Converts arbitrary expressions into a string via given format.

**Syntax**

``` sql
formatRow(format, x, y, ...)
```

**Arguments**

-   `format` — Text format. For example, [CSV](../../interfaces/formats.md#csv), [TSV](../../interfaces/formats.md#tabseparated).
-   `x`,`y`, ... — Expressions.

**Returned value**

-   A formatted string (for text formats it's usually terminated with the new line character).

**Example**

Query:

``` sql
SELECT formatRow('CSV', number, 'good')
FROM numbers(3);
```

Result:

``` text
┌─formatRow('CSV', number, 'good')─┐
│ 0,"good"
                         │
│ 1,"good"
                         │
│ 2,"good"
                         │
└──────────────────────────────────┘
```

## formatRowNoNewline {#formatrownonewline}

Converts arbitrary expressions into a string via given format. The function trims the last `\n` if any.

**Syntax**

``` sql
formatRowNoNewline(format, x, y, ...)
```

**Arguments**

-   `format` — Text format. For example, [CSV](../../interfaces/formats.md#csv), [TSV](../../interfaces/formats.md#tabseparated).
-   `x`,`y`, ... — Expressions.

**Returned value**

-   A formatted string.

**Example**

Query:

``` sql
SELECT formatRowNoNewline('CSV', number, 'good')
FROM numbers(3);
```

Result:

``` text
┌─formatRowNoNewline('CSV', number, 'good')─┐
│ 0,"good"                                  │
│ 1,"good"                                  │
│ 2,"good"                                  │
└───────────────────────────────────────────┘
```
