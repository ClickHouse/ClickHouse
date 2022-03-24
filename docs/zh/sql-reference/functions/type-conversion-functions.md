---
toc_priority: 38
toc_title: 类型转换函数
---

# 类型转换函数 {#lei-xing-zhuan-huan-han-shu}

## 数值类型转换常见的问题 {#numeric-conversion-issues}

当你把一个值从一个类型转换为另外一个类型的时候，你需要注意的是这是一个不安全的操作，可能导致数据的丢失。数据丢失一般发生在你将一个大的数据类型转换为小的数据类型的时候，或者你把两个不同的数据类型相互转换的时候。

ClickHouse和[C++](https://en.cppreference.com/w/cpp/language/implicit_conversion)有相同的类型转换行为。

## toInt(8\|16\|32\|64) {#touint8-touint16-touint32-touint64}

转换一个输入值为[Int](../../sql-reference/data-types/int-uint.md)类型。这个函数包括：

-   `toInt8(expr)` — 结果为`Int8`数据类型。
-   `toInt16(expr)` — 结果为`Int16`数据类型。
-   `toInt32(expr)` — 结果为`Int32`数据类型。
-   `toInt64(expr)` — 结果为`Int64`数据类型。

**参数**

-   `expr` — [表达式](../syntax.md#syntax-expressions)返回一个数字或者代表数值类型的字符串。不支持二进制、八进制、十六进制的数字形式，有效数字之前的0也会被忽略。

**返回值**

整形在`Int8`, `Int16`, `Int32`，或者 `Int64` 的数据类型。

函数使用[rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)原则，这意味着会截断丢弃小数部分的数值。

[NaN and Inf](../../sql-reference/data-types/float.md#data_type-float-nan-inf)转换是不确定的。具体使用的时候，请参考[数值类型转换常见的问题](#numeric-conversion-issues)。

**例子**

``` sql
SELECT toInt64(nan), toInt32(32), toInt16('16'), toInt8(8.8)
```

``` text
┌─────────toInt64(nan)─┬─toInt32(32)─┬─toInt16('16')─┬─toInt8(8.8)─┐
│ -9223372036854775808 │          32 │            16 │           8 │
└──────────────────────┴─────────────┴───────────────┴─────────────┘
```

## toInt(8\|16\|32\|64)OrZero {#toint8163264orzero}

这个函数需要一个字符类型的入参，然后尝试把它转为`Int (8 | 16 | 32 | 64)`，如果转换失败直接返回0。

**例子**

``` sql
select toInt64OrZero('123123'), toInt8OrZero('123qwe123')
```

``` text
┌─toInt64OrZero('123123')─┬─toInt8OrZero('123qwe123')─┐
│                  123123 │                         0 │
└─────────────────────────┴───────────────────────────┘
```
## toInt(8\|16\|32\|64)OrNull {#toint8163264ornull}

这个函数需要一个字符类型的入参，然后尝试把它转为`Int (8 | 16 | 32 | 64)`，如果转换失败直接返回`NULL`。

**例子**

``` sql
select toInt64OrNull('123123'), toInt8OrNull('123qwe123')
```

``` text
┌─toInt64OrNull('123123')─┬─toInt8OrNull('123qwe123')─┐
│                  123123 │                      ᴺᵁᴸᴸ │
└─────────────────────────┴───────────────────────────┘
```

## toUInt(8\|16\|32\|64) {#touint8163264}

转换一个输入值到[UInt](../../sql-reference/data-types/int-uint.md)类型。 这个函数包括：

-   `toUInt8(expr)` — 结果为`UInt8`数据类型。
-   `toUInt16(expr)` — 结果为`UInt16`数据类型。
-   `toUInt32(expr)` — 结果为`UInt32`数据类型。
-   `toUInt64(expr)` — 结果为`UInt64`数据类型。

**参数**

-   `expr` — [表达式](../syntax.md#syntax-expressions)返回一个数字或者代表数值类型的字符串。不支持二进制、八进制、十六进制的数字形式，有效数字之前的0也会被忽略。

**返回值**

整形在`UInt8`, `UInt16`, `UInt32`，或者 `UInt64` 的数据类型。

函数使用[rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero)原则，这意味着会截断丢弃小数部分的数值。

对于负数和[NaN and Inf](../../sql-reference/data-types/float.md#data_type-float-nan-inf)来说转换的结果是不确定的。如果你传入一个负数，比如：`'-32'`，ClickHouse会抛出异常。具体使用的时候，请参考[数值类型转换常见的问题](#numeric-conversion-issues)。

**例子**

``` sql
SELECT toUInt64(nan), toUInt32(-32), toUInt16('16'), toUInt8(8.8)
```

``` text
┌───────toUInt64(nan)─┬─toUInt32(-32)─┬─toUInt16('16')─┬─toUInt8(8.8)─┐
│ 9223372036854775808 │    4294967264 │             16 │            8 │
└─────────────────────┴───────────────┴────────────────┴──────────────┘
```

## toUInt(8\|16\|32\|64)OrZero {#touint8163264orzero}

## toUInt(8\|16\|32\|64)OrNull {#touint8163264ornull}

## toFloat(32\|64) {#tofloat3264}

## toFloat(32\|64)OrZero {#tofloat3264orzero}

## toFloat(32\|64)OrNull {#tofloat3264ornull}

## toDate {#todate}

## toDateOrZero {#todateorzero}

## toDateOrNull {#todateornull}

## toDateTime {#todatetime}

## toDateTimeOrZero {#todatetimeorzero}

## toDateTimeOrNull {#todatetimeornull}

## toDecimal(32\|64\|128) {#todecimal3264128}

转换 `value` 到[Decimal](../../sql-reference/data-types/decimal.md)类型的值，其中精度为`S`。`value`可以是一个数字或者一个字符串。`S` 指定小数位的精度。

-   `toDecimal32(value, S)`
-   `toDecimal64(value, S)`
-   `toDecimal128(value, S)`

## toDecimal(32\|64\|128)OrNull {#todecimal3264128ornull}

转换一个输入的字符到[Nullable(Decimal(P,S))](../../sql-reference/data-types/decimal.md)类型的数据。这个函数包括：

-   `toDecimal32OrNull(expr, S)` — 结果为`Nullable(Decimal32(S))`数据类型。
-   `toDecimal64OrNull(expr, S)` — 结果为`Nullable(Decimal64(S))`数据类型。
-   `toDecimal128OrNull(expr, S)` — 结果为`Nullable(Decimal128(S))`数据类型。

如果在解析输入值发生错误的时候你希望得到一个`NULL`值而不是抛出异常，你可以使用该函数。

**参数**

-   `expr` — [表达式](../syntax.md#syntax-expressions)返回一个[String](../../sql-reference/data-types/string.md)类型的数据。 ClickHouse倾向于文本类型的表示带小数类型的数值，比如`'1.111'`。
-   `S` — 小数位的精度。

**返回值**

`Nullable(Decimal(P,S))`类型的数据，包括：

-   如果有的话，小数位`S`。
-   如果解析错误或者输入的数字的小数位多于`S`,那结果为`NULL`。

**例子**

``` sql
SELECT toDecimal32OrNull(toString(-1.111), 5) AS val, toTypeName(val)
```

``` text
┌────val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 5))─┐
│ -1.111 │ Nullable(Decimal(9, 5))                            │
└────────┴────────────────────────────────────────────────────┘
```

``` sql
SELECT toDecimal32OrNull(toString(-1.111), 2) AS val, toTypeName(val)
```

``` text
┌──val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 2))─┐
│ ᴺᵁᴸᴸ │ Nullable(Decimal(9, 2))                            │
└──────┴────────────────────────────────────────────────────┘
```

## toDecimal(32\|64\|128)OrZero {#todecimal3264128orzero}

转换输入值为[Decimal(P,S)](../../sql-reference/data-types/decimal.md)类型数据。这个函数包括：

-   `toDecimal32OrZero( expr, S)` — 结果为`Decimal32(S)` 数据类型。
-   `toDecimal64OrZero( expr, S)` — 结果为`Decimal64(S)` 数据类型。
-   `toDecimal128OrZero( expr, S)` — 结果为`Decimal128(S)` 数据类型。

当解析错误的时候，你不需要抛出异常而希望得到`0`值，你可以使用该函数。

**参数**

-   `expr` — [表达式](../syntax.md#syntax-expressions)返回一个[String](../../sql-reference/data-types/string.md)类型的数据。 ClickHouse倾向于文本类型的表示带小数类型的数值，比如`'1.111'`。
-   `S` — 小数位的精度。

**返回值**

A value in the `Nullable(Decimal(P,S))` data type. The value contains:

-   如果有的话，小数位`S`。
-   如果解析错误或者输入的数字的小数位多于`S`,那结果为小数位精度为`S`的`0`。
**例子**

``` sql
SELECT toDecimal32OrZero(toString(-1.111), 5) AS val, toTypeName(val)
```

``` text
┌────val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 5))─┐
│ -1.111 │ Decimal(9, 5)                                      │
└────────┴────────────────────────────────────────────────────┘
```

``` sql
SELECT toDecimal32OrZero(toString(-1.111), 2) AS val, toTypeName(val)
```

``` text
┌──val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 2))─┐
│ 0.00 │ Decimal(9, 2)                                      │
└──────┴────────────────────────────────────────────────────┘
```


## toString {#tostring}

这些函数用于在数字、字符串（不包含FixedString）、Date以及DateTime之间互相转换。
所有的函数都接受一个参数。

当将其他类型转换到字符串或从字符串转换到其他类型时，使用与TabSeparated格式相同的规则对字符串的值进行格式化或解析。如果无法解析字符串则抛出异常并取消查询。

当将Date转换为数字或反之，Date对应Unix时间戳的天数。
将DataTime转换为数字或反之，DateTime对应Unix时间戳的秒数。

toDate/toDateTime函数的日期和日期时间格式定义如下：

    YYYY-MM-DD
    YYYY-MM-DD hh:mm:ss

例外的是，如果将UInt32、Int32、UInt64或Int64类型的数值转换为Date类型，并且其对应的值大于等于65536，则该数值将被解析成unix时间戳（而不是对应的天数）。这意味着允许写入’toDate(unix_timestamp)‘这种常见情况，否则这将是错误的，并且需要便携更加繁琐的’toDate(toDateTime(unix_timestamp))’。

Date与DateTime之间的转换以更为自然的方式进行：通过添加空的time或删除time。

数值类型之间的转换与C++中不同数字类型之间的赋值相同的规则。

此外，DateTime参数的toString函数可以在第二个参数中包含时区名称。 例如：`Asia/Yekaterinburg`在这种情况下，时间根据指定的时区进行格式化。

``` sql
SELECT
    now() AS now_local,
    toString(now(), 'Asia/Yekaterinburg') AS now_yekat
```

    ┌───────────now_local─┬─now_yekat───────────┐
    │ 2016-06-15 00:11:21 │ 2016-06-15 02:11:21 │
    └─────────────────────┴─────────────────────┘

另请参阅`toUnixTimestamp`函数。

## toFixedString(s,N) {#tofixedstrings-n}

将String类型的参数转换为FixedString(N)类型的值（具有固定长度N的字符串）。N必须是一个常量。
如果字符串的字节数少于N，则向右填充空字节。如果字符串的字节数多于N，则抛出异常。

## toStringCutToZero(s) {#tostringcuttozeros}

接受String或FixedString参数。返回String，其内容在找到的第一个零字节处被截断。

示例:

``` sql
SELECT toFixedString('foo', 8) AS s, toStringCutToZero(s) AS s_cut
```

    ┌─s─────────────┬─s_cut─┐
    │ foo\0\0\0\0\0 │ foo   │
    └───────────────┴───────┘

``` sql
SELECT toFixedString('foo\0bar', 8) AS s, toStringCutToZero(s) AS s_cut
```

    ┌─s──────────┬─s_cut─┐
    │ foo\0bar\0 │ foo   │
    └────────────┴───────┘

## reinterpretAsUInt(8\|16\|32\|64) {#reinterpretasuint8163264}

## reinterpretAsInt(8\|16\|32\|64) {#reinterpretasint8163264}

## reinterpretAsFloat(32\|64) {#reinterpretasfloat3264}

## reinterpretAsDate {#reinterpretasdate}

## reinterpretAsDateTime {#reinterpretasdatetime}

这些函数接受一个字符串，并将放在字符串开头的字节解释为主机顺序中的数字（little endian）。如果字符串不够长，则函数就像使用必要数量的空字节填充字符串一样。如果字符串比需要的长，则忽略额外的字节。Date被解释为Unix时间戳的天数，DateTime被解释为Unix时间戳。

## reinterpretAsString {#reinterpretasstring}

此函数接受数字、Date或DateTime，并返回一个字符串，其中包含表示主机顺序（小端）的相应值的字节。从末尾删除空字节。例如，UInt32类型值255是一个字节长的字符串。

## reinterpretAsFixedString {#reinterpretasfixedstring}

此函数接受数字、Date或DateTime，并返回包含表示主机顺序（小端）的相应值的字节的FixedString。从末尾删除空字节。例如，UInt32类型值255是一个长度为一个字节的FixedString。

## CAST(x, T) {#type_conversion_function-cast}

将’x’转换为’t’数据类型。还支持语法CAST（x AS t）

示例:

``` sql
SELECT
    '2016-06-15 23:00:00' AS timestamp,
    CAST(timestamp AS DateTime) AS datetime,
    CAST(timestamp AS Date) AS date,
    CAST(timestamp, 'String') AS string,
    CAST(timestamp, 'FixedString(22)') AS fixed_string
```

    ┌─timestamp───────────┬────────────datetime─┬───────date─┬─string──────────────┬─fixed_string──────────────┐
    │ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00\0\0\0 │
    └─────────────────────┴─────────────────────┴────────────┴─────────────────────┴───────────────────────────┘

将参数转换为FixedString(N)，仅适用于String或FixedString(N)类型的参数。

支持将数据转换为[可为空](../../sql-reference/functions/type-conversion-functions.md)。例如：

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

## toInterval(Year\|Quarter\|Month\|Week\|Day\|Hour\|Minute\|Second) {#function-tointerval}

把一个数值类型的值转换为[Interval](../../sql-reference/data-types/special-data-types/interval.md)类型的数据。

**语法**

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

**参数**

-   `number` — 正整数，持续的时间。

**返回值**

-   时间的`Interval`值。

**例子**

``` sql
WITH
    toDate('2019-01-01') AS date,
    INTERVAL 1 WEEK AS interval_week,
    toIntervalWeek(1) AS interval_to_week
SELECT
    date + interval_week,
    date + interval_to_week
```

``` text
┌─plus(date, interval_week)─┬─plus(date, interval_to_week)─┐
│                2019-01-08 │                   2019-01-08 │
└───────────────────────────┴──────────────────────────────┘
```

## parseDateTimeBestEffort {#parsedatetimebesteffort}

把[String](../../sql-reference/data-types/string.md)类型的时间日期转换为[DateTime](../../sql-reference/data-types/datetime.md#data_type-datetime)数据类型。

该函数可以解析[ISO 8601](https://en.wikipedia.org/wiki/ISO_8601)，[RFC 1123 - 5.2.14 RFC-822 Date and Time Specification](https://tools.ietf.org/html/rfc1123#page-55)或者ClickHouse的一些别的时间日期格式。

**语法**

``` sql
parseDateTimeBestEffort(time_string [, time_zone]);
```

**参数**

-   `time_string` — 字符类型的时间和日期。
-   `time_zone` — 字符类型的时区。

**非标准格式的支持**

-   9位或者10位的数字时间，[unix timestamp](https://en.wikipedia.org/wiki/Unix_time).
-   时间和日期组成的字符串： `YYYYMMDDhhmmss`, `DD/MM/YYYY hh:mm:ss`, `DD-MM-YY hh:mm`, `YYYY-MM-DD hh:mm:ss`等。
-   只有日期的字符串： `YYYY`, `YYYYMM`, `YYYY*MM`, `DD/MM/YYYY`, `DD-MM-YY` 等。
-   只有天和时间： `DD`, `DD hh`, `DD hh:mm`。这种情况下 `YYYY-MM` 默认为 `2000-01`。
-   包含时间日期以及时区信息： `YYYY-MM-DD hh:mm:ss ±h:mm`等。例如： `2020-12-12 17:36:00 -5:00`。

对于所有的格式来说，这个函数通过全称或者第一个三个字符的月份名称来解析月份，比如：`24/DEC/18`, `24-Dec-18`, `01-September-2018`。

**返回值**

-   `DateTime`类型数据。

**例子**

查询:

``` sql
SELECT parseDateTimeBestEffort('12/12/2020 12:12:57')
AS parseDateTimeBestEffort;
```

结果:

``` text
┌─parseDateTimeBestEffort─┐
│     2020-12-12 12:12:57 │
└─────────────────────────┘
```

查询:

``` sql
SELECT parseDateTimeBestEffort('Sat, 18 Aug 2018 07:22:16 GMT', 'Asia/Istanbul')
AS parseDateTimeBestEffort
```

结果:

``` text
┌─parseDateTimeBestEffort─┐
│     2018-08-18 10:22:16 │
└─────────────────────────┘
```

查询:

``` sql
SELECT parseDateTimeBestEffort('1284101485')
AS parseDateTimeBestEffort
```

结果:

``` text
┌─parseDateTimeBestEffort─┐
│     2015-07-07 12:04:41 │
└─────────────────────────┘
```

查询:

``` sql
SELECT parseDateTimeBestEffort('2018-12-12 10:12:12')
AS parseDateTimeBestEffort
```

结果:

``` text
┌─parseDateTimeBestEffort─┐
│     2018-12-12 10:12:12 │
└─────────────────────────┘
```

查询:

``` sql
SELECT parseDateTimeBestEffort('10 20:19')
```

结果:

``` text
┌─parseDateTimeBestEffort('10 20:19')─┐
│                 2000-01-10 20:19:00 │
└─────────────────────────────────────┘
```

**除此之外**

-   [ISO 8601 announcement by @xkcd](https://xkcd.com/1179/)
-   [RFC 1123](https://tools.ietf.org/html/rfc1123)
-   [toDate](#todate)
-   [toDateTime](#todatetime)

## parseDateTimeBestEffortOrNull {#parsedatetimebesteffortornull}

这个函数和[parseDateTimeBestEffort](#parsedatetimebesteffort)基本一致，除了无法解析返回结果为`NULL`。

## parseDateTimeBestEffortOrZero {#parsedatetimebesteffortorzero}

这个函数和[parseDateTimeBestEffort](#parsedatetimebesteffort)基本一致，除了无法解析返回结果为`0`。

## toLowCardinality {#tolowcardinality}

把输入值转换为[LowCardianlity](../data-types/lowcardinality.md)的相同类型的数据。

如果要把`LowCardinality`类型的数据转换为其他类型，使用[CAST](#type_conversion_function-cast)函数。比如：`CAST(x as String)`。

**语法**

```sql
toLowCardinality(expr)
```

**参数**

- `expr` — [表达式](../syntax.md#syntax-expressions)为[支持的数据类型](../data-types/index.md#data_types)的一种。


**返回值**

- `expr`的结果。

类型： `LowCardinality(expr_result_type)`

**例子**

查询:

```sql
SELECT toLowCardinality('1')
```

结果:

```text
┌─toLowCardinality('1')─┐
│ 1                     │
└───────────────────────┘
```


## toUnixTimestamp64Milli
## toUnixTimestamp64Micro
## toUnixTimestamp64Nano

把一个`DateTime64`类型的数据转换为`Int64`类型的数据，结果包含固定亚秒的精度。输入的值是变大还是变低依赖于输入的精度。需要注意的是输出的值是一个UTC的时间戳, 不是同一个时区的`DateTime64`值。

**语法**

``` sql
toUnixTimestamp64Milli(value)
```

**参数**

-   `value` — 任何精度的DateTime64类型的数据。

**返回值**

-   `value` `Int64`类型数据。

**例子**

查询:

``` sql
WITH toDateTime64('2019-09-16 19:20:12.345678910', 6) AS dt64
SELECT toUnixTimestamp64Milli(dt64)
```

结果:

``` text
┌─toUnixTimestamp64Milli(dt64)─┐
│                1568650812345 │
└──────────────────────────────┘
```

``` sql
WITH toDateTime64('2019-09-16 19:20:12.345678910', 6) AS dt64
SELECT toUnixTimestamp64Nano(dt64)
```

结果:

``` text
┌─toUnixTimestamp64Nano(dt64)─┐
│         1568650812345678000 │
└─────────────────────────────┘
```

## fromUnixTimestamp64Milli
## fromUnixTimestamp64Micro
## fromUnixTimestamp64Nano

把`Int64`类型的数据转换为`DateTime64`类型的数据，结果包含固定的亚秒精度和可选的时区。 输入的值是变大还是变低依赖于输入的精度。需要注意的是输入的值是一个UTC的时间戳, 不是一个包含时区的时间戳。


**语法**

``` sql
fromUnixTimestamp64Milli(value [, ti])
```

**参数**

-   `value` — `Int64`类型的数据，可以是任意精度。
-   `timezone` — `String`类型的时区

**返回值**

-   `value` DateTime64`类型的数据。

**例子**

``` sql
WITH CAST(1234567891011, 'Int64') AS i64
SELECT fromUnixTimestamp64Milli(i64, 'UTC')
```

``` text
┌─fromUnixTimestamp64Milli(i64, 'UTC')─┐
│              2009-02-13 23:31:31.011 │
└──────────────────────────────────────┘
```

[来源文章](https://clickhouse.com/docs/en/query_language/functions/type_conversion_functions/) <!--hide-->
