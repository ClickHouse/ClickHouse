# 类型转换函数 {#lei-xing-zhuan-huan-han-shu}

## toUInt8,toUInt16,toUInt32,toUInt64 {#touint8-touint16-touint32-touint64}

## toInt8,toInt16,toInt32,toInt64 {#toint8-toint16-toint32-toint64}

## toFloat32,toFloat64 {#tofloat32-tofloat64}

## 今天，今天 {#todate-todatetime}

## toUInt8OrZero,toUInt16OrZero,toUInt32OrZero,toUInt64OrZero,toInt8OrZero,toInt16OrZero,toInt32OrZero,toInt64OrZero,toFloat32OrZero,toFloat64OrZero,toDateOrZero,toDateTimeOrZero {#touint8orzero-touint16orzero-touint32orzero-touint64orzero-toint8orzero-toint16orzero-toint32orzero-toint64orzero-tofloat32orzero-tofloat64orzero-todateorzero-todatetimeorzero}

## toUInt8OrNull,toUInt16OrNull,toUInt32OrNull,toUInt64OrNull,toInt8OrNull,toInt16OrNull,toInt32OrNull,toInt64OrNull,toFloat32OrNull,toFloat64OrNull,toDateOrNull,toDateTimeOrNull {#touint8ornull-touint16ornull-touint32ornull-touint64ornull-toint8ornull-toint16ornull-toint32ornull-toint64ornull-tofloat32ornull-tofloat64ornull-todateornull-todatetimeornull}

## toString {#tostring}

这些函数用于在数字、字符串（不包含FixedString）、Date以及DateTime之间互相转换。
所有的函数都接受一个参数。

当将其他类型转换到字符串或从字符串转换到其他类型时，使用与TabSeparated格式相同的规则对字符串的值进行格式化或解析。如果无法解析字符串则抛出异常并取消查询。

当将Date转换为数字或反之，Date对应Unix时间戳的天数。
将DataTime转换为数字或反之，DateTime对应Unix时间戳的秒数。

toDate/toDateTime函数的日期和日期时间格式定义如下：

    YYYY-MM-DD
    YYYY-MM-DD hh:mm:ss

例外的是，如果将UInt32、Int32、UInt64或Int64类型的数值转换为Date类型，并且其对应的值大于等于65536，则该数值将被解析成unix时间戳（而不是对应的天数）。这意味着允许写入’toDate(unix\_timestamp)‘这种常见情况，否则这将是错误的，并且需要便携更加繁琐的’toDate(toDateTime(unix\_timestamp))’。

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

## toDecimal32(value,S),toDecimal64(value,S),toDecimal128(value,S) {#todecimal32value-s-todecimal64value-s-todecimal128value-s}

将`value`转换为精度为`S`的[十进制](../../sql-reference/functions/type-conversion-functions.md)。`value`可以是数字或字符串。`S`参数为指定的小数位数。

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

## reinterpretAsUInt8,reinterpretAsUInt16,reinterpretAsUInt32,reinterpretAsUInt64 {#reinterpretasuint8-reinterpretasuint16-reinterpretasuint32-reinterpretasuint64}

## reinterpretAsInt8,reinterpretAsInt16,reinterpretAsInt32,reinterpretAsInt64 {#reinterpretasint8-reinterpretasint16-reinterpretasint32-reinterpretasint64}

## reinterpretAsFloat32,reinterpretAsFloat64 {#reinterpretasfloat32-reinterpretasfloat64}

## 重新解释日期，重新解释日期时间 {#reinterpretasdate-reinterpretasdatetime}

这些函数接受一个字符串，并将放在字符串开头的字节解释为主机顺序中的数字（little endian）。如果字符串不够长，则函数就像使用必要数量的空字节填充字符串一样。如果字符串比需要的长，则忽略额外的字节。Date被解释为Unix时间戳的天数，DateTime被解释为Unix时间戳。

## 重新解释字符串 {#reinterpretasstring}

此函数接受数字、Date或DateTime，并返回一个字符串，其中包含表示主机顺序（小端）的相应值的字节。从末尾删除空字节。例如，UInt32类型值255是一个字节长的字符串。

## reinterpretAsFixedString {#reinterpretasfixedstring}

此函数接受数字、Date或DateTime，并返回包含表示主机顺序（小端）的相应值的字节的FixedString。从末尾删除空字节。例如，UInt32类型值255是一个长度为一个字节的FixedString。

## 演员(x,t) {#type_conversion_function-cast}

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

## 每天每天每天每天每天每天每天每天每天每天每天每天每天每天每天每天每天每天每天每天每天每天每天每天每天每天每天每天每天每天每天每 {#function-tointerval}

将数字类型参数转换为Interval类型（时间区间）。
Interval类型实际上是非常有用的，您可以使用此类型的数据直接与Date或DateTime执行算术运算。同时，ClickHouse为Interval类型数据的声明提供了更方便的语法。例如：

``` sql
WITH
    toDate('2019-01-01') AS date,
    INTERVAL 1 WEEK AS interval_week,
    toIntervalWeek(1) AS interval_to_week
SELECT
    date + interval_week,
    date + interval_to_week
```

    ┌─plus(date, interval_week)─┬─plus(date, interval_to_week)─┐
    │                2019-01-08 │                   2019-01-08 │
    └───────────────────────────┴──────────────────────────────┘

## parsedatetimebestefort {#type_conversion_functions-parsedatetimebesteffort}

将数字类型参数解析为Date或DateTime类型。
与toDate和toDateTime不同，parseDateTimeBestEffort可以进行更复杂的日期格式。
有关详细信息，请参阅链接：[复杂日期格式](https://xkcd.com/1179/)。

## parsedatetimebestefortornull {#parsedatetimebesteffortornull}

与[parsedatetimebestefort](#type_conversion_functions-parsedatetimebesteffort)相同，但它遇到无法处理的日期格式时返回null。

## parsedatetimebestefortorzero {#parsedatetimebesteffortorzero}

与[parsedatetimebestefort](#type_conversion_functions-parsedatetimebesteffort)相同，但它遇到无法处理的日期格式时返回零Date或零DateTime。

[来源文章](https://clickhouse.tech/docs/en/query_language/functions/type_conversion_functions/) <!--hide-->
