---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
sidebar_position: 49
sidebar_label: DateTime64
---

# Datetime64 {#data_type-datetime64}

此类型允许以日期（date）加时间（time）的形式来存储一个时刻的时间值，具有定义的亚秒精度

时间刻度大小（精度）：10<sup>-精度</sup> 秒

语法:

``` sql
DateTime64(precision, [timezone])
```

在内部，此类型以Int64类型将数据存储为自Linux纪元开始(1970-01-01 00:00:00UTC)的时间刻度数（ticks）。时间刻度的分辨率由precision参数确定。此外，`DateTime64` 类型可以像存储其他数据列一样存储时区信息，时区会影响 `DateTime64` 类型的值如何以文本格式显示，以及如何解析以字符串形式指定的时间数据 (‘2020-01-01 05:00:01.000’)。时区不存储在表的行中（也不在resultset中），而是存储在列的元数据中。详细信息请参考 [DateTime](datetime.md) 数据类型.

值的范围: \[1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999\] (注意: 最大值的精度是8)。

## 示例 {#examples}

**1.** 创建一个具有 `DateTime64` 类型列的表，并向其中插入数据:

``` sql
CREATE TABLE dt
(
    `timestamp` DateTime64(3, 'Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = TinyLog
```

``` sql
INSERT INTO dt Values (1546300800000, 1), ('2019-01-01 00:00:00', 2)
```

``` sql
SELECT * FROM dt
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.000 │        1 │
│ 2019-01-01 00:00:00.000 │        2 │
└─────────────────────────┴──────────┘
```

-   将日期时间作为integer类型插入时，它会被视为适当缩放的Unix时间戳(UTC)。`1546300800000` （精度为3）表示 `'2019-01-01 00:00:00'` UTC. 不过，因为 `timestamp` 列指定了 `Asia/Istanbul` （UTC+3）的时区，当作为字符串输出时，它将显示为 `'2019-01-01 03:00:00'`
-   当把字符串作为日期时间插入时，它会被赋予时区信息。 `'2019-01-01 00:00:00'` 将被认为处于 `Asia/Istanbul` 时区并被存储为 `1546290000000`.

**2.** 过滤 `DateTime64` 类型的值

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Asia/Istanbul')
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        2 │
└─────────────────────────┴──────────┘
```

与 `DateTime` 不同, `DateTime64` 类型的值不会自动从 `String` 类型的值转换过来

**3.** 获取 `DateTime64` 类型值的时区信息:

``` sql
SELECT toDateTime64(now(), 3, 'Asia/Istanbul') AS column, toTypeName(column) AS x
```

``` text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2019-10-16 04:12:04.000 │ DateTime64(3, 'Asia/Istanbul') │
└─────────────────────────┴────────────────────────────────┘
```

**4.** 时区转换

``` sql
SELECT
toDateTime64(timestamp, 3, 'Europe/London') as lon_time,
toDateTime64(timestamp, 3, 'Asia/Istanbul') as mos_time
FROM dt
```

``` text
┌───────────────lon_time──┬────────────────mos_time─┐
│ 2019-01-01 00:00:00.000 │ 2019-01-01 03:00:00.000 │
│ 2018-12-31 21:00:00.000 │ 2019-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘
```

## 另请参阅 {#see-also}

-   [类型转换函数](../../sql-reference/functions/type-conversion-functions.md)
-   [用于处理日期和时间的函数](../../sql-reference/functions/date-time-functions.md)
-   [用于处理数组的函数](../../sql-reference/functions/array-functions.md)
-   [`date_time_input_format` 配置](../../operations/settings/settings.md#settings-date_time_input_format)
-   [`date_time_output_format` 配置](../../operations/settings/settings.md#settings-date_time_output_format)
-   [`timezone` 服务器配置参数](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone)
-   [用于处理日期和时间的算子](../../sql-reference/operators/index.md#operators-datetime)
-   [`Date` 数据类型](date.md)
-   [`DateTime` 数据类型](datetime.md)
