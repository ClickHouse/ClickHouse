---
description: 'ClickHouse 中 日期时间 数据类型的文档，该类型以秒级精度存储
  时间戳'
sidebar_label: '日期时间'
sidebar_position: 16
slug: /sql-reference/data-types/datetime
title: '日期时间'
doc_type: 'reference'
---

用于存储某一时刻，可表示为日历日期和一天中的时间。

语法：

```sql
DateTime([timezone])
```

支持的取值范围：[1970-01-01 00:00:00, 2106-02-07 06:28:15]。

精度：1 秒。

<div id="speed">
  ## 速度
</div>

在&#95;大多数&#95;情况下，`Date` 数据类型比 `DateTime` 更快。

`Date` 类型需要 2 字节存储空间，而 `DateTime` 需要 4 字节。不过，在压缩时，`Date` 和 `DateTime` 之间的大小差异会更加明显。这是因为 `DateTime` 中的分钟和秒可压缩性较低。对 `Date` 进行过滤和聚合也比对 `DateTime` 更快。

<div id="usage-remarks">
  ## 使用注意事项
</div>

时间点会保存为 [Unix 时间戳](https://en.wikipedia.org/wiki/Unix_time)，与时区或夏令时无关。时区会影响 `DateTime` 类型的值如何以文本格式显示，以及以字符串形式指定的值如何被解析 (`'2020-01-01 05:00:01'`) 。

表中存储的是与时区无关的 Unix 时间戳；时区用于在数据导入/导出期间将其转换为文本格式或从文本格式还原，也用于对这些值执行日历计算 (例如：`toDate`、`toHour` 等函数) 。时区不会存储在表的行中 (或结果集中) ，而是存储在列元数据中。

支持的时区列表可在 [IANA Time Zone Database](https://www.iana.org/time-zones) 中找到，也可以通过 `SELECT * FROM system.time_zones` 查询。[该列表](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) 在 Wikipedia 上也可查看。

创建表时，可以为 `DateTime` 类型的列显式设置时区。示例：`DateTime('UTC')`。如果未设置时区，ClickHouse 会使用 ClickHouse server 启动时服务器设置中的 [timezone](../../operations/server-configuration-parameters/settings.md#timezone) 参数值，或操作系统设置中的值。

如果在初始化数据类型时未显式设置时区，[ClickHouse 客户端](../../interfaces/client.md) 默认会使用服务器时区。要使用客户端时区，请使用 `--use_client_time_zone` 参数运行 `clickhouse-client`。

ClickHouse 会根据 [date&#95;time&#95;output&#95;format](../../operations/settings/settings-formats.md#date_time_output_format) 设置的值输出相应的结果。默认文本格式为 `YYYY-MM-DD hh:mm:ss`。此外，你还可以使用 [formatDateTime](../../sql-reference/functions/date-time-functions.md#formatDateTime) 函数更改输出格式。

向 ClickHouse 插入数据时，可以根据 [date&#95;time&#95;input&#95;format](../../operations/settings/settings-formats.md#date_time_input_format) 设置的值，使用不同格式的日期和时间字符串。

<div id="examples">
  ## 示例
</div>

**1.** 创建一个带有 `DateTime` 类型列的表，并向其中插入数据：

```sql
CREATE TABLE dt
(
    `timestamp` DateTime('Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse DateTime
-- - from string,
-- - from integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt VALUES ('2019-01-01 00:00:00', 1), (1546300800, 2);

SELECT * FROM dt;
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
│ 2019-01-01 03:00:00 │        2 │
└─────────────────────┴──────────┘
```

* 将 datetime 以整数形式插入时，会被视为 Unix 时间戳 (UTC) 。`1546300800` 表示 UTC 的 `'2019-01-01 00:00:00'`。但是，由于 `timestamp` 列指定了 `Asia/Istanbul` (UTC+3) 时区，因此在以字符串形式输出时，该值会显示为 `'2019-01-01 03:00:00'`
* 将字符串值作为 datetime 插入时，会被视为使用列时区。`'2019-01-01 00:00:00'` 会被视为 `Asia/Istanbul` 时区的时间，并保存为 `1546290000`。

**2.** 对 `DateTime` 值进行筛选

```sql
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Asia/Istanbul')
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
└─────────────────────┴──────────┘
```

可以在 `WHERE` 条件中使用字符串值来过滤 `DateTime` 列中的值；它会自动转换为 `DateTime`：

```sql
SELECT * FROM dt WHERE timestamp = '2019-01-01 00:00:00'
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
└─────────────────────┴──────────┘
```

**3.** 获取 `DateTime` 类型列的时区：

```sql
SELECT toDateTime(now(), 'Asia/Istanbul') AS column, toTypeName(column) AS x
```

```text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Asia/Istanbul') │
└─────────────────────┴───────────────────────────┘
```

**4.** 时区转换

```sql
SELECT
toDateTime(timestamp, 'Europe/London') AS lon_time,
toDateTime(timestamp, 'Asia/Istanbul') AS istanbul_time
FROM dt
```

```text
┌───────────lon_time──┬───────istanbul_time─┐
│ 2019-01-01 00:00:00 │ 2019-01-01 03:00:00 │
│ 2018-12-31 21:00:00 │ 2019-01-01 00:00:00 │
└─────────────────────┴─────────────────────┘
```

由于时区转换只会更改元数据，因此该操作不产生计算开销。

<div id="limitations-on-time-zones-support">
  ## 时区支持的限制
</div>

某些时区可能无法获得完整支持，主要包括以下几种情况：

如果相对 UTC 的偏移量不是 15 分钟的整数倍，小时和分钟的计算可能会不准确。例如，利比里亚蒙罗维亚时区在 1972 年 1 月 7 日之前的偏移量为 UTC -0:44:30。如果你要对 Monrovia 时区中的历史时间进行计算，时间处理函数可能会给出错误结果。不过，1972 年 1 月 7 日之后的结果仍然是正确的。

如果时间切换 (由于夏令时或其他原因) 发生在一个并非 15 分钟整数倍的时刻，那么在这一天的这个特定时间点，你也可能得到错误结果。

非单调的日历日期。例如，在 Happy Valley - Goose Bay，时间曾于 2010 年 11 月 7 日 00:01:00 回拨 1 小时 (即午夜过后 1 分钟) 。因此，在 11 月 6 日结束后，当地先经历了 11 月 7 日的整整 1 分钟，随后时间又被调回到 11 月 6 日 23:01，再过 59 分钟后，11 月 7 日才再次开始。ClickHouse (暂时) 还不支持这种特殊情况。在这些日期内，时间处理函数的结果可能会有轻微误差。

2010 年南极 Casey 测站也存在类似问题。他们在 3 月 5 日 02:00 将时间回拨了 3 小时。如果你在南极测站工作，也不用担心无法使用 ClickHouse。只要确保将时区设置为 UTC，或者了解其中可能存在的误差即可。

跨越多天的时间移位。一些太平洋岛屿将其相对 UTC 的时区偏移量从 UTC+14 改为 UTC-12。这本身没有问题，但如果你使用这些时区对切换当日的历史时间点进行计算，仍可能出现一些误差。

<div id="handling-daylight-saving-time-dst">
  ## 处理夏令时 (DST)
</div>

带时区的 ClickHouse 日期时间 类型在夏令时 (DST) 切换期间可能会出现异常行为，尤其是在以下情况下：

* [`date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format) 设置为 `simple`。
* 时钟回拨 (“Fall Back”) 时，会产生一小时的重叠。
* 时钟前拨 (“Spring Forward”) 时，会产生一小时的缺口。

默认情况下，ClickHouse 总是选择重叠时间中较早的那一次，并且在时钟前拨时，可能会将实际上不存在的时间解释为有效时间。

例如，考虑下面这个从夏令时 (DST) 切换到标准时间的情况。

* 在 2023 年 10 月 29 日 02:00:00，时钟从 02:00:00 回拨到 01:00:00 (BST → GMT) 。
* 01:00:00 – 01:59:59 这一小时会出现两次 (一次是 BST，一次是 GMT) 。
* ClickHouse 总是选择第一次出现的时间 (BST) ，这会在添加时间间隔时导致异常结果。

```sql
SELECT '2023-10-29 01:30:00'::DateTime('Europe/London') AS time, time + toIntervalHour(1) AS one_hour_later

┌────────────────time─┬──────one_hour_later─┐
│ 2023-10-29 01:30:00 │ 2023-10-29 01:30:00 │
└─────────────────────┴─────────────────────┘
```

类似地，在从标准时间切换到夏令时的过程中，也可能会看起来像是跳过了一个小时。

例如：

* 在 2023 年 3 月 26 日的 `00:59:59`，时钟会直接跳到 02:00:00 (GMT → BST) 。
* `01:00:00` – `01:59:59` 这一小时并不存在。

```sql
SELECT '2023-03-26 01:30:00'::DateTime('Europe/London') AS time, time + toIntervalHour(1) AS one_hour_later

┌────────────────time─┬──────one_hour_later─┐
│ 2023-03-26 00:30:00 │ 2023-03-26 02:30:00 │
└─────────────────────┴─────────────────────┘
```

在这种情况下，ClickHouse 会将不存在的时间 `2023-03-26 01:30:00` 移位回 `2023-03-26 00:30:00`。

<div id="see-also">
  ## 另请参见
</div>

* [类型转换函数](../../sql-reference/functions/type-conversion-functions.md)
* [处理日期和时间的函数](../../sql-reference/functions/date-time-functions.md)
* [处理数组的函数](../../sql-reference/functions/array-functions.md)
* [`date_time_input_format` 设置](../../operations/settings/settings-formats.md#date_time_input_format)
* [`date_time_output_format` 设置](../../operations/settings/settings-formats.md#date_time_output_format)
* [`timezone` 服务器配置参数](../../operations/server-configuration-parameters/settings.md#timezone)
* [`session_timezone` 设置](../../operations/settings/settings.md#session_timezone)
* [处理日期和时间的运算符](../../sql-reference/operators#operators-for-working-with-dates-and-times)
* [`Date` 数据类型](../../sql-reference/data-types/date.md)