# 时间日期函数 {#shi-jian-ri-qi-han-shu}

支持时区。

所有的时间日期函数都可以在第二个可选参数中接受时区参数。示例：Asia / Yekaterinburg。在这种情况下，它们使用指定的时区而不是本地（默认）时区。

``` sql
SELECT
    toDateTime('2016-06-15 23:00:00') AS time,
    toDate(time) AS date_local,
    toDate(time, 'Asia/Yekaterinburg') AS date_yekat,
    toString(time, 'US/Samoa') AS time_samoa
```

``` text
┌────────────────time─┬─date_local─┬─date_yekat─┬─time_samoa──────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-16 │ 2016-06-15 09:00:00 │
└─────────────────────┴────────────┴────────────┴─────────────────────┘
```

仅支持与UTC相差一整小时的时区。

## timeZone {#timezone}

返回服务器的时区。
如果它在分布式表的上下文中执行，那么它会生成一个普通列，其中包含与每个分片相关的值。否则它会产生一个常数值。

**语法**

``` sql
timeZone()
```

别名：`timezone`。

**返回值**

-   时区。

类型为： [String](../../sql-reference/data-types/string.md)。


## toTimeZone {#totimezone}

将Date或DateTime转换为指定的时区。 时区是Date/DateTime类型的属性。 表字段或结果集的列的内部值（秒数）不会更改，列的类型会更改，并且其字符串表示形式也会相应更改。

**语法**

``` sql
toTimezone(value, timezone)
```

别名：`toTimezone`。

**参数**

-   `value` — 时间或日期和时间。类型为[DateTime64](../../sql-reference/data-types/datetime64.md)。
-   `timezone` — 返回值的时区。类型为 [String](../../sql-reference/data-types/string.md)。 这个参数是一个常量，因为 `toTimezone` 改变了列的时区（时区是 `DateTime` 类型的属性）。

**返回值**

-   日期和时间。

类型为： [DateTime](../../sql-reference/data-types/datetime.md)。

**示例**

查询语句：

```sql
SELECT
    toDateTime('2019-01-01 00:00:00', 'UTC') AS time_utc,
    toTypeName(time_utc) AS type_utc,
    toInt32(time_utc) AS int32utc,
    toTimeZone(time_utc, 'Asia/Yekaterinburg') AS time_yekat,
    toTypeName(time_yekat) AS type_yekat,
    toInt32(time_yekat) AS int32yekat,
    toTimeZone(time_utc, 'US/Samoa') AS time_samoa,
    toTypeName(time_samoa) AS type_samoa,
    toInt32(time_samoa) AS int32samoa
FORMAT Vertical;
```

```text
Row 1:
──────
time_utc:   2019-01-01 00:00:00
type_utc:   DateTime('UTC')
int32utc:   1546300800
time_yekat: 2019-01-01 05:00:00
type_yekat: DateTime('Asia/Yekaterinburg')
int32yekat: 1546300800
time_samoa: 2018-12-31 13:00:00
type_samoa: DateTime('US/Samoa')
int32samoa: 1546300800
```

`toTimeZone(time_utc, 'Asia/Yekaterinburg')` 把 `DateTime('UTC')` 类型转换为 `DateTime('Asia/Yekaterinburg')`. 内部值 (Unixtimestamp) 1546300800 保持不变, 但是字符串表示(toString() 函数的结果值) 由 `time_utc:   2019-01-01 00:00:00` 转换为o `time_yekat: 2019-01-01 05:00:00`.

## timeZoneOf {#timezoneof}

返回[DateTime](../../sql-reference/data-types/datetime.md)或者[DateTime64](../../sql-reference/data-types/datetime64.md)数据类型的时区名称。

**语法**

``` sql
timeZoneOf(value)
```

别名： `timezoneOf`。

**参数**

-   `value` — 日期和时间。类型为[DateTime](../../sql-reference/data-types/datetime.md)或者[DateTime64](../../sql-reference/data-types/datetime64.md)。

**返回值**

-   时区名称。

类型为：[String](../../sql-reference/data-types/string.md)。

**示例**

查询语句:
``` sql
SELECT timezoneOf(now());
```

结果:
``` text
┌─timezoneOf(now())─┐
│ Etc/UTC           │
└───────────────────┘
```

## timeZoneOffset {#timezoneoffset}

返回从[UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time)开始到现在以秒为单位的时区偏移量。该函数考虑到[夏时令](https://en.wikipedia.org/wiki/Daylight_saving_time)并在指定日期和时间更改历史时区。
[IANA timezone database](https://www.iana.org/time-zones)用于计算偏移量。

**语法**

``` sql
timeZoneOffset(value)
```

别名： `timezoneOffset`。

**参数**

-   `value` — 日期和时间。类型为[DateTime](../../sql-reference/data-types/datetime.md)或者[DateTime64](../../sql-reference/data-types/datetime64.md)。

**返回值**

-   以秒为单位的UTC偏移量。

类型为： [Int32](../../sql-reference/data-types/int-uint.md)。

**示例**

查询语句:

``` sql
SELECT toDateTime('2021-04-21 10:20:30', 'America/New_York') AS Time, toTypeName(Time) AS Type,
       timeZoneOffset(Time) AS Offset_in_seconds, (Offset_in_seconds / 3600) AS Offset_in_hours;
```

结果:

``` text
┌────────────────Time─┬─Type─────────────────────────┬─Offset_in_seconds─┬─Offset_in_hours─┐
│ 2021-04-21 10:20:30 │ DateTime('America/New_York') │            -14400 │              -4 │
└─────────────────────┴──────────────────────────────┴───────────────────┴─────────────────┘
```

## toYear {#toyear}

将Date或DateTime转换为包含年份编号（AD）的UInt16类型的数字。

别名为：`YEAR`。

## toQuarter {#toquarter}

将Date或DateTime转换为包含季度编号的UInt8类型的数字。

别名为：`QUARTER`。

## toMonth {#tomonth}

将Date或DateTime转换为包含月份编号（1-12）的UInt8类型的数字。

别名为：`MONTH`。

## toDayOfYear {#todayofyear}

将Date或DateTime转换为包含一年中的某一天的编号的UInt16（1-366）类型的数字。

别名为: `DAYOFYEAR`。

## toDayOfMonth {#todayofmonth}

将Date或DateTime转换为包含一月中的某一天的编号的UInt8（1-31）类型的数字。

别名为：`DAYOFMONTH`，`DAY`。

## toDayOfWeek {#todayofweek}

将Date或DateTime转换为包含一周中的某一天的编号的UInt8（周一是1, 周日是7）类型的数字。

别名为：`DAYOFWEEK`。

## toHour {#tohour}

将DateTime转换为包含24小时制（0-23）小时数的UInt8数字。
这个函数假设如果时钟向前移动，它是一个小时，发生在凌晨2点，如果时钟被移回，它是一个小时，发生在凌晨3点（这并非总是如此 - 即使在莫斯科时钟在不同的时间两次改变）。

别名为： `HOUR`。

## toMinute {#tominute}

将DateTime转换为包含一小时中分钟数（0-59）的UInt8数字。

别名为： `MINUTE`。

## toSecond {#tosecond}

将DateTime转换为包含一分钟中秒数（0-59）的UInt8数字。
闰秒不计算在内。

别名为： `SECOND`。

## toUnixTimestamp {#to-unix-timestamp}

对于DateTime参数：将值转换为UInt32类型的数字-Unix时间戳（https://en.wikipedia.org/wiki/Unix_time）。
对于String参数：根据时区将输入字符串转换为日期时间（可选的第二个参数，默认使用服务器时区），并返回相应的unix时间戳。

**语法**

``` sql
toUnixTimestamp(datetime)
toUnixTimestamp(str, [timezone])
```

**返回值**

-   返回 unix timestamp.

类型: `UInt32`.

**示例**

查询:

``` sql
SELECT toUnixTimestamp('2017-11-05 08:07:47', 'Asia/Tokyo') AS unix_timestamp
```

结果:

``` text
┌─unix_timestamp─┐
│     1509836867 │
└────────────────┘
```

:::注意
下面描述的返回类型 `toStartOf` 函数是 `Date` 或 `DateTime`。尽管这些函数可以将 `DateTime64` 作为参数，但将超出正常范围（1900年-2299年）的 `DateTime64` 传递给它们会给出不正确的结果。
:::

## toStartOfYear {#tostartofyear}

将Date或DateTime向前取整到本年的第一天。
返回Date类型。

## toStartOfISOYear {#tostartofisoyear}

将Date或DateTime向前取整到ISO本年的第一天。
返回Date类型。

## toStartOfQuarter {#tostartofquarter}

将Date或DateTime向前取整到本季度的第一天。
返回Date类型。

## toStartOfMonth {#tostartofmonth}

将Date或DateTime向前取整到本月的第一天。
返回Date类型。

!!! 注意 "注意"
        解析不正确日期的行为是特定于实现的。 ClickHouse可能会返回零日期，抛出异常或执行«natural»溢出。

## toMonday {#tomonday}

将Date或DateTime向前取整到本周的星期一。
返回Date类型。

## toStartOfWeek(t\[,mode\]) {#tostartofweek}

按mode将Date或DateTime向前取整到最近的星期日或星期一。
返回Date类型。
mode参数的工作方式与toWeek()的mode参数完全相同。 对于单参数语法，mode使用默认值0。

## toStartOfDay {#tostartofday}

将DateTime向前取整到今天的开始。

## toStartOfHour {#tostartofhour}

将DateTime向前取整到当前小时的开始。

## toStartOfMinute {#tostartofminute}

将DateTime向前取整到当前分钟的开始。

## toStartOfSecond {#tostartofsecond}

将DateTime向前取整到当前秒数的开始。

**语法**

``` sql
toStartOfSecond(value[, timezone])
```

**参数**

-   `value` — 时间和日期[DateTime64](../../sql-reference/data-types/datetime64.md).
-   `timezone` — 返回值的[Timezone](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) (可选参数)。 如果未指定将使用 `value` 参数的时区。 [String](../../sql-reference/data-types/string.md)。

**返回值**

-   输入值毫秒部分为零。

类型: [DateTime64](../../sql-reference/data-types/datetime64.md).

**示例**

不指定时区查询:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999', 3) AS dt64
SELECT toStartOfSecond(dt64);
```

结果:

``` text
┌───toStartOfSecond(dt64)─┐
│ 2020-01-01 10:20:30.000 │
└─────────────────────────┘
```

指定时区查询:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999', 3) AS dt64
SELECT toStartOfSecond(dt64, 'Asia/Istanbul');
```

结果:

``` text
┌─toStartOfSecond(dt64, 'Asia/Istanbul')─┐
│                2020-01-01 13:20:30.000 │
└────────────────────────────────────────┘
```

**参考**

-   [Timezone](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) 服务器配置选项。

## toStartOfFiveMinutes {#tostartoffiveminutes}

将DateTime以五分钟为单位向前取整到最接近的时间点。

## toStartOfTenMinutes {#tostartoftenminutes}

将DateTime以十分钟为单位向前取整到最接近的时间点。

## toStartOfFifteenMinutes {#tostartoffifteenminutes}

将DateTime以十五分钟为单位向前取整到最接近的时间点。

## toStartOfInterval(time_or_data,间隔x单位\[,time_zone\]) {#tostartofintervaltime-or-data-interval-x-unit-time-zone}

这是名为`toStartOf*`的所有函数的通用函数。例如，
`toStartOfInterval（t，INTERVAL 1 year）`返回与`toStartOfYear（t）`相同的结果，
`toStartOfInterval（t，INTERVAL 1 month）`返回与`toStartOfMonth（t）`相同的结果，
`toStartOfInterval（t，INTERVAL 1 day）`返回与`toStartOfDay（t）`相同的结果，
`toStartOfInterval（t，INTERVAL 15 minute）`返回与`toStartOfFifteenMinutes（t）`相同的结果。

## toTime {#totime}

将DateTime中的日期转换为一个固定的日期，同时保留时间部分。

## toRelativeYearNum {#torelativeyearnum}

将Date或DateTime转换为年份的编号，从过去的某个固定时间点开始。

## toRelativeQuarterNum {#torelativequarternum}

将Date或DateTime转换为季度的数字，从过去的某个固定时间点开始。

## toRelativeMonthNum {#torelativemonthnum}

将Date或DateTime转换为月份的编号，从过去的某个固定时间点开始。

## toRelativeWeekNum {#torelativeweeknum}

将Date或DateTime转换为星期数，从过去的某个固定时间点开始。

## toRelativeDayNum {#torelativedaynum}

将Date或DateTime转换为当天的编号，从过去的某个固定时间点开始。

## toRelativeHourNum {#torelativehournum}

将DateTime转换为小时数，从过去的某个固定时间点开始。

## toRelativeMinuteNum {#torelativeminutenum}

将DateTime转换为分钟数，从过去的某个固定时间点开始。

## toRelativeSecondNum {#torelativesecondnum}

将DateTime转换为秒数，从过去的某个固定时间点开始。

## toISOYear {#toisoyear}

将Date或DateTime转换为包含ISO年份的UInt16类型的编号。

## toISOWeek {#toisoweek}

将Date或DateTime转换为包含ISO周数的UInt8类型的编号。

## toWeek(date\[,mode\]) {#toweekdatemode}

返回Date或DateTime的周数。两个参数形式可以指定星期是从星期日还是星期一开始，以及返回值应在0到53还是从1到53的范围内。如果省略了mode参数，则默认 模式为0。
`toISOWeek()`是一个兼容函数，等效于`toWeek(date,3)`。
下表描述了mode参数的工作方式。

| Mode | First day of week | Range | Week 1 is the first week …    |
|------|-------------------|-------|-------------------------------|
| 0    | Sunday            | 0-53  | with a Sunday in this year    |
| 1    | Monday            | 0-53  | with 4 or more days this year |
| 2    | Sunday            | 1-53  | with a Sunday in this year    |
| 3    | Monday            | 1-53  | with 4 or more days this year |
| 4    | Sunday            | 0-53  | with 4 or more days this year |
| 5    | Monday            | 0-53  | with a Monday in this year    |
| 6    | Sunday            | 1-53  | with 4 or more days this year |
| 7    | Monday            | 1-53  | with a Monday in this year    |
| 8    | Sunday            | 1-53  | contains January 1            |
| 9    | Monday            | 1-53  | contains January 1            |

对于象“with 4 or more days this year,”的mode值，根据ISO 8601：1988对周进行编号：

-   如果包含1月1日的一周在后一年度中有4天或更多天，则为第1周。

-   否则，它是上一年的最后一周，下周是第1周。

对于像“contains January 1”的mode值, 包含1月1日的那周为本年度的第1周。

``` sql
toWeek(date, [, mode][, Timezone])
```

**参数**

-   `date` – Date 或 DateTime.
-   `mode` – 可选参数, 取值范围 \[0,9\]， 默认0。
-   `Timezone` – 可选参数， 可其他时间日期转换参数的行为一致。

**示例**

``` sql
SELECT toDate('2016-12-27') AS date, toWeek(date) AS week0, toWeek(date,1) AS week1, toWeek(date,9) AS week9;
```

``` text
┌───────date─┬─week0─┬─week1─┬─week9─┐
│ 2016-12-27 │    52 │    52 │     1 │
└────────────┴───────┴───────┴───────┘
```

## toYearWeek(date\[,mode\]) {#toyearweekdatemode}

返回Date的年和周。 结果中的年份可能因为Date为该年份的第一周和最后一周而于Date的年份不同。

mode参数的工作方式与toWeek()的mode参数完全相同。 对于单参数语法，mode使用默认值0。

`toISOYear()`是一个兼容函数，等效于`intDiv(toYearWeek(date,3),100)`.

**示例**

``` sql
SELECT toDate('2016-12-27') AS date, toYearWeek(date) AS yearWeek0, toYearWeek(date,1) AS yearWeek1, toYearWeek(date,9) AS yearWeek9;
```

``` text
┌───────date─┬─yearWeek0─┬─yearWeek1─┬─yearWeek9─┐
│ 2016-12-27 │    201652 │    201652 │    201701 │
└────────────┴───────────┴───────────┴───────────┘
```

## date_trunc {#date_trunc}

将Date或DateTime按指定的单位向前取整到最接近的时间点。

**语法**

``` sql
date_trunc(unit, value[, timezone])
```

别名: `dateTrunc`.

**参数**

-   `unit` — 单位. [String](../syntax.md#syntax-string-literal).
    可选值:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

-   `value` — [DateTime](../../sql-reference/data-types/datetime.md) 或者 [DateTime64](../../sql-reference/data-types/datetime64.md).
-   `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) 返回值的时区(可选值)。如果未指定将使用`value`的时区。 [String](../../sql-reference/data-types/string.md).

**返回值**

-   按指定的单位向前取整后的DateTime。

类型: [Datetime](../../sql-reference/data-types/datetime.md).

**示例**

不指定时区查询:

``` sql
SELECT now(), date_trunc('hour', now());
```

结果:

``` text
┌───────────────now()─┬─date_trunc('hour', now())─┐
│ 2020-09-28 10:40:45 │       2020-09-28 10:00:00 │
└─────────────────────┴───────────────────────────┘
```

指定时区查询:

```sql
SELECT now(), date_trunc('hour', now(), 'Asia/Istanbul');
```

结果:

```text
┌───────────────now()─┬─date_trunc('hour', now(), 'Asia/Istanbul')─┐
│ 2020-09-28 10:46:26 │                        2020-09-28 13:00:00 │
└─────────────────────┴────────────────────────────────────────────┘
```

**参考**

-   [toStartOfInterval](#tostartofintervaltime-or-data-interval-x-unit-time-zone)

## date_add {#date_add}

将时间间隔或日期间隔添加到提供的日期或带时间的日期。

**语法**

``` sql
date_add(unit, value, date)
```

别名为：`dateAdd`, `DATE_ADD`。

**参数**

-   `unit` — `value`对应的时间单位。类型为[String](../../sql-reference/data-types/string.md)。
    可能的值：

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

-   `value` — 要添加的间隔值。类型为[Int](../../sql-reference/data-types/int-uint.md)。
-   `date` — 添加`value`的日期或日期。类型为[Date](../../sql-reference/data-types/date.md)或者[DateTime](../../sql-reference/data-types/datetime.md)。

**返回值**

通过将 `value` 以`unit` 表示，添加到`date` 获得的日期或带时间的日期。

类型为： [Date](../../sql-reference/data-types/date.md)或[DateTime](../../sql-reference/data-types/datetime.md)。

**示例**

查询语句：

```sql
SELECT date_add(YEAR, 3, toDate('2018-01-01'));
```

结果：

```text
┌─plus(toDate('2018-01-01'), toIntervalYear(3))─┐
│                                    2021-01-01 │
└───────────────────────────────────────────────┘
```

## date_diff {#date_diff}

返回两个日期或具有时间值的日期之间的差值。

**语法**

``` sql
date_diff('unit', startdate, enddate, [timezone])
```

别名为： `dateDiff`, `DATE_DIFF`。

**参数**

-   `unit` — `value`对应的时间单位。类型为[String](../../sql-reference/data-types/string.md)。
    可能的值：

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

-   `startdate` — 要减去的第一个时间值（减数）。类型为[Date](../../sql-reference/data-types/date.md)或者[DateTime](../../sql-reference/data-types/datetime.md)。

-   `enddate` — 要减去的第二个时间值（被减数）。类型为[Date](../../sql-reference/data-types/date.md)或者[DateTime](../../sql-reference/data-types/datetime.md)。

-   `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) (可选项)。如果指定，它适用于 `startdate` 和 `enddate`。如果未指定，则使用 `startdate` 和 `enddate` 的时区。如果它们不相同，则结果未指定。类型为[String](../../sql-reference/data-types/string.md)。

**返回值**

以 `unit` 表示的 `enddate` 和 `startdate` 之间的区别。

类型为: [Int](../../sql-reference/data-types/int-uint.md)。

**示例**

查询语句：

``` sql
SELECT dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'));
```

结果：

``` text
┌─dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'))─┐
│                                                                                     25 │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

## date_sub {#date_sub}

从提供的日期或带时间的日期中减去时间间隔或日期间隔。

**语法**

``` sql
date_sub(unit, value, date)
```

别名为： `dateSub`, `DATE_SUB`.

**参数**

-   `unit` — `value`对应的时间单位。类型为[String](../../sql-reference/data-types/string.md)。
    可能的值：

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

-   `value` — 要减去的时间。类型为[Int](../../sql-reference/data-types/int-uint.md)。
-   `date` — 被减去`value`的日期或日期。类型为[Date](../../sql-reference/data-types/date.md)或者[DateTime](../../sql-reference/data-types/datetime.md)。

**返回值**

从 `date` 中减去以`unit` 表示的`value` 得到的日期或带时间的日期。

类型为：[Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md)。

**示例**

查询语句：

``` sql
SELECT date_sub(YEAR, 3, toDate('2018-01-01'));
```

结果：

``` text
┌─minus(toDate('2018-01-01'), toIntervalYear(3))─┐
│                                     2015-01-01 │
└────────────────────────────────────────────────┘
```

## timestamp_add {#timestamp_add}

将指定的时间值与提供的日期或日期时间值相加。

**语法**

``` sql
timestamp_add(date, INTERVAL value unit)
```

别名为： `timeStampAdd`, `TIMESTAMP_ADD`.

**参数**

-   `date` — 日期或日期与时间。类型为[Date](../../sql-reference/data-types/date.md)或者[DateTime](../../sql-reference/data-types/datetime.md)。
-   `value` — 要添加的间隔值。类型为[Int](../../sql-reference/data-types/int-uint.md)。
-   `unit` — `value`对应的时间单位。类型为[String](../../sql-reference/data-types/string.md)。
    可能的值：

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

**返回值**

以`unit`表示的指定`value`的日期或带时间的日期添加到`date`。

类型为：[Date](../../sql-reference/data-types/date.md)或者[DateTime](../../sql-reference/data-types/datetime.md)。

**示例**

查询语句：

```sql
select timestamp_add(toDate('2018-01-01'), INTERVAL 3 MONTH);
```

结果：

```text
┌─plus(toDate('2018-01-01'), toIntervalMonth(3))─┐
│                                     2018-04-01 │
└────────────────────────────────────────────────┘
```

## timestamp_sub {#timestamp_sub}

从提供的日期或带时间的日期中减去时间间隔。

**语法**

``` sql
timestamp_sub(unit, value, date)
```

别名为： `timeStampSub`, `TIMESTAMP_SUB`。

**参数**

-   `unit` — `value`对应的时间单位。类型为[String](../../sql-reference/data-types/string.md)。
    可能的值:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

-   `value` — 要减去的间隔值。类型为[Int](../../sql-reference/data-types/int-uint.md)。
-   `date` — 日期或日期与时间。类型为[Date](../../sql-reference/data-types/date.md)或者[DateTime](../../sql-reference/data-types/datetime.md)。

**返回值**

从 `date` 中减去以`unit` 表示的`value` 得到的日期或带时间的日期。

类型为： [Date](../../sql-reference/data-types/date.md)或者[DateTime](../../sql-reference/data-types/datetime.md)。

**示例**

查询语句：

```sql
select timestamp_sub(MONTH, 5, toDateTime('2018-12-18 01:02:03'));
```

结果：

```text
┌─minus(toDateTime('2018-12-18 01:02:03'), toIntervalMonth(5))─┐
│                                          2018-07-18 01:02:03 │
└──────────────────────────────────────────────────────────────┘
```

# now {#now}

返回当前日期和时间。

**语法**

``` sql
now([timezone])
```

**参数**

-   `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) 返回结果的时区(可先参数). [String](../../sql-reference/data-types/string.md).

**返回值**

-   当前日期和时间。

类型: [Datetime](../../sql-reference/data-types/datetime.md).

**示例**

不指定时区查询:

``` sql
SELECT now();
```

结果:

``` text
┌───────────────now()─┐
│ 2020-10-17 07:42:09 │
└─────────────────────┘
```

指定时区查询:

``` sql
SELECT now('Asia/Istanbul');
```

结果:

``` text
┌─now('Asia/Istanbul')─┐
│  2020-10-17 10:42:23 │
└──────────────────────┘
```

## today {#today}

不接受任何参数并在请求执行时的某一刻返回当前日期(Date)。
其功能与’toDate（now()）’相同。

## yesterday {#yesterday}

不接受任何参数并在请求执行时的某一刻返回昨天的日期(Date)。
其功能与’today() - 1’相同。

## timeSlot {#timeslot}

将时间向前取整半小时。
此功能用于Yandex.Metrica，因为如果跟踪标记显示单个用户的连续综合浏览量在时间上严格超过此数量，则半小时是将会话分成两个会话的最短时间。这意味着（tag id，user id，time slot）可用于搜索相应会话中包含的综合浏览量。

## toYYYYMM {#toyyyymm}

将Date或DateTime转换为包含年份和月份编号的UInt32类型的数字（YYYY \* 100 + MM）。

## toYYYYMMDD {#toyyyymmdd}

将Date或DateTime转换为包含年份和月份编号的UInt32类型的数字（YYYY \* 10000 + MM \* 100 + DD）。

## toYYYYMMDDhhmmss {#toyyyymmddhhmmss}

将Date或DateTime转换为包含年份和月份编号的UInt64类型的数字（YYYY \* 10000000000 + MM \* 100000000 + DD \* 1000000 + hh \* 10000 + mm \* 100 + ss）。

## addYears, addMonths, addWeeks, addDays, addHours, addMinutes, addSeconds, addQuarters {#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters}

函数将一段时间间隔添加到Date/DateTime，然后返回Date/DateTime。例如：

``` sql
WITH
    toDate('2018-01-01') AS date,
    toDateTime('2018-01-01 00:00:00') AS date_time
SELECT
    addYears(date, 1) AS add_years_with_date,
    addYears(date_time, 1) AS add_years_with_date_time
```

    ┌─add_years_with_date─┬─add_years_with_date_time─┐
    │          2019-01-01 │      2019-01-01 00:00:00 │
    └─────────────────────┴──────────────────────────┘

## subtractYears,subtractMonths,subtractWeeks,subtractDays,subtractours,subtractMinutes,subtractSeconds,subtractQuarters {#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters}

函数将Date/DateTime减去一段时间间隔，然后返回Date/DateTime。例如：

``` sql
WITH
    toDate('2019-01-01') AS date,
    toDateTime('2019-01-01 00:00:00') AS date_time
SELECT
    subtractYears(date, 1) AS subtract_years_with_date,
    subtractYears(date_time, 1) AS subtract_years_with_date_time
```

    ┌─subtract_years_with_date─┬─subtract_years_with_date_time─┐
    │               2018-01-01 │           2018-01-01 00:00:00 │
    └──────────────────────────┴───────────────────────────────┘

## timeSlots(StartTime, Duration,\[, Size\]) {#timeslotsstarttime-duration-size}

它返回一个时间数组，其中包括从从«StartTime»开始到«StartTime + Duration 秒»内的所有符合«size»（以秒为单位）步长的时间点。其中«size»是一个可选参数，默认为1800。
例如，`timeSlots(toDateTime('2012-01-01 12:20:00')，600) = [toDateTime（'2012-01-01 12:00:00'），toDateTime（'2012-01-01 12:30:00' ）]`。
这对于搜索在相应会话中综合浏览量是非常有用的。

## formatDateTime {#formatdatetime}

函数根据给定的格式字符串来格式化时间。请注意：格式字符串必须是常量表达式，例如：单个结果列不能有多种格式字符串。

**语法**

``` sql
formatDateTime(Time, Format\[, Timezone\])
```

**返回值**

根据指定格式返回的日期和时间。

**支持的格式修饰符**

使用格式修饰符来指定结果字符串的样式。«Example» 列是对`2018-01-02 22:33:44`的格式化结果。

| 修饰符 | 描述                                                                                                                                                                                                | 示例       |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------|
| %C     | 年除以100并截断为整数(00-99)                                                                                                                                                                        | 20         |
| %d     | 月中的一天，零填充（01-31)                                                                                                                                                                          | 02         |
| %D     | 短MM/DD/YY日期，相当于%m/%d/%y                                                                                                                                                                      | 01/02/2018 |
| %e     | 月中的一天，空格填充（ 1-31)                                                                                                                                                                         |  2          |
| %F     | 短YYYY-MM-DD日期，相当于%Y-%m-%d                                                                                                                                                                    | 2018-01-02 |
| %G     | ISO周号的四位数年份格式， 从基于周的年份[由ISO 8601定义](https://en.wikipedia.org/wiki/ISO_8601#Week_dates) 标准计算得出，通常仅对％V有用 | 2018       |
| %g     | 两位数的年份格式，与ISO 8601一致，四位数表示法的缩写                                                                                                                    | 18         |
| %H     | 24小时格式（00-23)                                                                                                                                                                                  | 22         |
| %I     | 12小时格式（01-12)                                                                                                                                                                                 | 10         |
| %j     | 一年中的一天 (001-366)                                                                                                                                                                                       | 002        |
| %m     | 月份为十进制数（01-12)                                                                                                                                                                              | 01         |
| %M     | 分钟(00-59)                                                                                                                                                                                         | 33         |
| %n     | 换行符(")                                                                                                                                                                                           |            |
| %p     | AM或PM指定                                                                                                                                                                                          | PM         |
| %Q     | 季度（1-4)                                                                                                                                                                          | 1          |
| %R     | 24小时HH:MM时间，相当于%H:%M                                                                                                                                                                        | 22:33      |
| %S     | 秒 (00-59)                                                                                                                                                                                         | 44         |
| %t     | 水平制表符(’)                                                                                                                                                                                       |            |
| %T     | ISO8601时间格式(HH:MM:SS)，相当于%H:%M:%S                                                                                                                                                           | 22:33:44   |
| %u     | ISO8601工作日为数字，星期一为1(1-7)                                                                                                                                                                   | 2          |
| %V     | ISO8601周编号(01-53)                                                                                                                                                                                | 01         |
| %w     | 工作日为十进制数，周日为0(0-6)                                                                                                                                                                        | 2          |
| %y     | 年份，最后两位数字（00-99)                                                                                                                                                                          | 18         |
| %Y     | 年                                                                                                                                                                                                  | 2018       |
| %%     | %符号                                                                                                                                                                                               | %          |

**示例**

查询:

``` sql
SELECT formatDateTime(toDate('2010-01-04'), '%g')
```

结果:

```
┌─formatDateTime(toDate('2010-01-04'), '%g')─┐
│ 10                                         │
└────────────────────────────────────────────┘
```

## dateName {#dataname}

返回日期的指定部分。

**语法**

``` sql
dateName(date_part, date)
```

**参数**

-   `date_part` — 日期部分。可能的值为：'year', 'quarter', 'month', 'week', 'dayofyear', 'day', 'weekday', 'hour', 'minute', 'second'。类型为[String](../../sql-reference/data-types/string.md)。
-   `date` — 日期。类型为[Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md)或者[DateTime64](../../sql-reference/data-types/datetime64.md)。
-   `timezone` — 时区（可选项）。类型为[String](../../sql-reference/data-types/string.md)。

**返回值**

-   日期的指定部分。

类型为： [String](../../sql-reference/data-types/string.md#string)。

**示例**

查询语句：

```sql
WITH toDateTime('2021-04-14 11:22:33') AS date_value
SELECT dateName('year', date_value), dateName('month', date_value), dateName('day', date_value);
```

结果：

```text
┌─dateName('year', date_value)─┬─dateName('month', date_value)─┬─dateName('day', date_value)─┐
│ 2021                         │ April                         │ 14                          │
└──────────────────────────────┴───────────────────────────────┴─────────────────────────────
```

## FROM_UNIXTIME

当只有单个整数类型的参数时，它的作用与`toDateTime`相同，并返回[DateTime](../../sql-reference/data-types/datetime.md)类型。

例如:

```sql
SELECT FROM_UNIXTIME(423543535)
```

```text
┌─FROM_UNIXTIME(423543535)─┐
│      1983-06-04 10:58:55 │
└──────────────────────────┘
```

当有两个参数时，第一个是整型或DateTime，第二个是常量格式字符串，它的作用与`formatDateTime`相同，并返回`String`类型。

例如:

```sql
SELECT FROM_UNIXTIME(1234334543, '%Y-%m-%d %R:%S') AS DateTime
```

```text
┌─DateTime────────────┐
│ 2009-02-11 14:42:23 │
└─────────────────────┘
```

## toModifiedJulianDay {#tomodifiedjulianday}

将文本形式 `YYYY-MM-DD` 的 [Proleptic Gregorian calendar](https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar) 日期转换为 Int32 中的 [Modified Julian Day](https://en.wikipedia.org/wiki/Julian_day#Variants) 数字。此功能支持从`0000-01-01`到`9999-12-31`的日期。如果无法将参数解析为日期或日期无效，则会引发异常。

**语法**

``` sql
toModifiedJulianDay(date)
```

**参数**

-   `date` — 文本形式的日期。类型为[String](../../sql-reference/data-types/string.md)或者[FixedString](../../sql-reference/data-types/fixedstring.md)。

**返回值**

-   转换的儒略日数。

类型为： [Int32](../../sql-reference/data-types/int-uint.md)。

**示例**

查询语句：

``` sql
SELECT toModifiedJulianDay('2020-01-01');
```

结果：

``` text
┌─toModifiedJulianDay('2020-01-01')─┐
│                             58849 │
└───────────────────────────────────┘
```

## toModifiedJulianDayOrNull {#tomodifiedjuliandayornull}

类似于[toModifiedJulianDay()](#tomodifiedjulianday)，但它不会引发异常，而是返回 `NULL`。

**语法**

``` sql
toModifiedJulianDayOrNull(date)
```

**参数**

-   `date` — 文本形式的日期。类型为[String](../../sql-reference/data-types/string.md)或者[FixedString](../../sql-reference/data-types/fixedstring.md)。

**返回值**

-   转换的儒略日数。

类型为： [Nullable(Int32)](../../sql-reference/data-types/int-uint.md)。

**示例**

查询语句：

``` sql
SELECT toModifiedJulianDayOrNull('2020-01-01');
```

结果：

``` text
┌─toModifiedJulianDayOrNull('2020-01-01')─┐
│                                   58849 │
└─────────────────────────────────────────┘
```

## fromModifiedJulianDay {#frommodifiedjulianday}

将 [Modified Julian Day](https://en.wikipedia.org/wiki/Julian_day#Variants) 数字转换为 `YYYY-MM-DD` 文本格式的 [Proleptic Gregorian calendar](https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar) 日期。该函数支持从 `-678941` 到 `2973119` 的天数（分别代表 0000-01-01 和 9999-12-31）。如果天数超出支持范围，则会引发异常。

**语法**

``` sql
fromModifiedJulianDay(day)
```

**参数**

-   `day` — 需要转换的儒略日数。类型为[Any integral types](../../sql-reference/data-types/int-uint.md)。

**返回值**

-   文本形式的日期。

类型为： [String](../../sql-reference/data-types/string.md)。

**示例**

查询语句：

``` sql
SELECT fromModifiedJulianDay(58849);
```

结果：

``` text
┌─fromModifiedJulianDay(58849)─┐
│ 2020-01-01                   │
└──────────────────────────────┘
```

## fromModifiedJulianDayOrNull {#frommodifiedjuliandayornull}

类似于[fromModifiedJulianDayOrNull()](#frommodifiedjuliandayornull)，但它不会引发异常，而是返回 `NULL`。

**语法**

``` sql
fromModifiedJulianDayOrNull(day)
```

**参数**

-   `day` —  需要转换的儒略日数。类型为[Any integral types](../../sql-reference/data-types/int-uint.md)。

**返回值**

-   文本形式的日期。

类型为： [Nullable(String)](../../sql-reference/data-types/string.md)。

**示例**

查询语句：

``` sql
SELECT fromModifiedJulianDayOrNull(58849);
```

结果：

``` text
┌─fromModifiedJulianDayOrNull(58849)─┐
│ 2020-01-01                         │
└────────────────────────────────────┘
```

[Original article](https://clickhouse.com/docs/en/query_language/functions/date_time_functions/) <!--hide-->
