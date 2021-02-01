---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 49
toc_title: DateTime64
---

# Datetime64 {#data_type-datetime64}

允许存储时间instant间，可以表示为日历日期和一天中的时间，具有定义的亚秒精度

刻度尺寸（精度）：10<sup>-精度</sup> 秒

语法:

``` sql
DateTime64(precision, [timezone])
```

在内部，存储数据作为一些 ‘ticks’ 自纪元开始(1970-01-01 00:00:00UTC)作为Int64. 刻度分辨率由precision参数确定。 此外，该 `DateTime64` 类型可以存储时区是相同的整个列，影响如何的值 `DateTime64` 类型值以文本格式显示，以及如何解析指定为字符串的值 (‘2020-01-01 05:00:01.000’). 时区不存储在表的行中（或resultset中），而是存储在列元数据中。 查看详细信息 [日期时间](datetime.md).

## 例 {#examples}

**1.** 创建一个表 `DateTime64`-输入列并将数据插入其中:

``` sql
CREATE TABLE dt
(
    `timestamp` DateTime64(3, 'Europe/Moscow'),
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

-   将日期时间作为整数插入时，将其视为适当缩放的Unix时间戳(UTC)。 `1546300800000` （精度为3）表示 `'2019-01-01 00:00:00'` UTC. 然而，作为 `timestamp` 列有 `Europe/Moscow` （UTC+3）指定的时区，当输出为字符串时，该值将显示为 `'2019-01-01 03:00:00'`
-   当插入字符串值作为日期时间时，它被视为处于列时区。 `'2019-01-01 00:00:00'` 将被视为 `Europe/Moscow` 时区并存储为 `1546290000000`.

**2.** 过滤 `DateTime64` 值

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Europe/Moscow')
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        2 │
└─────────────────────────┴──────────┘
```

不像 `DateTime`, `DateTime64` 值不转换为 `String` 自动

**3.** 获取一个时区 `DateTime64`-类型值:

``` sql
SELECT toDateTime64(now(), 3, 'Europe/Moscow') AS column, toTypeName(column) AS x
```

``` text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2019-10-16 04:12:04.000 │ DateTime64(3, 'Europe/Moscow') │
└─────────────────────────┴────────────────────────────────┘
```

**4.** 时区转换

``` sql
SELECT
toDateTime64(timestamp, 3, 'Europe/London') as lon_time,
toDateTime64(timestamp, 3, 'Europe/Moscow') as mos_time
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
-   [该 `date_time_input_format` 设置](../../operations/settings/settings.md#settings-date_time_input_format)
-   [该 `timezone` 服务器配置参数](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone)
-   [使用日期和时间的操作员](../../sql-reference/operators/index.md#operators-datetime)
-   [`Date` 数据类型](date.md)
-   [`DateTime` 数据类型](datetime.md)
