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

    ┌────────────────time─┬─date_local─┬─date_yekat─┬─time_samoa──────────┐
    │ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-16 │ 2016-06-15 09:00:00 │
    └─────────────────────┴────────────┴────────────┴─────────────────────┘

仅支持与UTC相差一整小时的时区。

## toTimeZone {#totimezone}

将Date或DateTime转换为指定的时区。

## toYear {#toyear}

将Date或DateTime转换为包含年份编号（AD）的UInt16类型的数字。

## toQuarter {#toquarter}

将Date或DateTime转换为包含季度编号的UInt8类型的数字。

## toMonth {#tomonth}

将Date或DateTime转换为包含月份编号（1-12）的UInt8类型的数字。

## 今天一年 {#todayofyear}

将Date或DateTime转换为包含一年中的某一天的编号的UInt16（1-366）类型的数字。

## 今天月 {#todayofmonth}

将Date或DateTime转换为包含一月中的某一天的编号的UInt8（1-31）类型的数字。

## 今天一周 {#todayofweek}

将Date或DateTime转换为包含一周中的某一天的编号的UInt8（周一是1, 周日是7）类型的数字。

## toHour {#tohour}

将DateTime转换为包含24小时制（0-23）小时数的UInt8数字。
这个函数假设如果时钟向前移动，它是一个小时，发生在凌晨2点，如果时钟被移回，它是一个小时，发生在凌晨3点（这并非总是如此 - 即使在莫斯科时钟在不同的时间两次改变）。

## toMinute {#tominute}

将DateTime转换为包含一小时中分钟数（0-59）的UInt8数字。

## 秒 {#tosecond}

将DateTime转换为包含一分钟中秒数（0-59）的UInt8数字。
闰秒不计算在内。

## toUnixTimestamp {#tounixtimestamp}

将DateTime转换为unix时间戳。

## 开始一年 {#tostartofyear}

将Date或DateTime向前取整到本年的第一天。
返回Date类型。

## 今年开始 {#tostartofisoyear}

将Date或DateTime向前取整到ISO本年的第一天。
返回Date类型。

## 四分之一开始 {#tostartofquarter}

将Date或DateTime向前取整到本季度的第一天。
返回Date类型。

## 到月份开始 {#tostartofmonth}

将Date或DateTime向前取整到本月的第一天。
返回Date类型。

!!! 注意 "注意"
        解析不正确日期的行为是特定于实现的。 ClickHouse可能会返回零日期，抛出异常或执行«natural»溢出。

## toMonday {#tomonday}

将Date或DateTime向前取整到本周的星期一。
返回Date类型。

## 今天开始 {#tostartofday}

将DateTime向前取整到当日的开始。

## 开始一小时 {#tostartofhour}

将DateTime向前取整到当前小时的开始。

## to startofminute {#tostartofminute}

将DateTime向前取整到当前分钟的开始。

## to startoffiveminute {#tostartoffiveminute}

将DateTime以五分钟为单位向前取整到最接近的时间点。

## 开始分钟 {#tostartoftenminutes}

将DateTime以十分钟为单位向前取整到最接近的时间点。

## 开始几分钟 {#tostartoffifteenminutes}

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

## 现在 {#now}

不接受任何参数并在请求执行时的某一刻返回当前时间（DateTime）。
此函数返回一个常量，即时请求需要很长时间能够完成。

## 今天 {#today}

不接受任何参数并在请求执行时的某一刻返回当前日期(Date)。
其功能与’toDate（now（））’相同。

## 昨天 {#yesterday}

不接受任何参数并在请求执行时的某一刻返回昨天的日期(Date)。
其功能与’today（） - 1’相同。

## 时隙 {#timeslot}

将时间向前取整半小时。
此功能用于Yandex.Metrica，因为如果跟踪标记显示单个用户的连续综合浏览量在时间上严格超过此数量，则半小时是将会话分成两个会话的最短时间。这意味着（tag id，user id，time slot）可用于搜索相应会话中包含的综合浏览量。

## toyyymm {#toyyyymm}

将Date或DateTime转换为包含年份和月份编号的UInt32类型的数字（YYYY \* 100 + MM）。

## toyyymmdd {#toyyyymmdd}

将Date或DateTime转换为包含年份和月份编号的UInt32类型的数字（YYYY \* 10000 + MM \* 100 + DD）。

## toYYYYMMDDhhmmss {#toyyyymmddhhmmss}

将Date或DateTime转换为包含年份和月份编号的UInt64类型的数字（YYYY \* 10000000000 + MM \* 100000000 + DD \* 1000000 + hh \* 10000 + mm \* 100 + ss）。

## 隆隆隆隆路虏脢,,陇,貌,垄拢卢虏禄quar陇,貌路,隆拢脳枚脢虏,麓脢,脱,,,录,禄庐戮,utes, {#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters}

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

## dateDiff(‘unit’,t1,t2,\[时区\]) {#datediffunit-t1-t2-timezone}

返回以’unit’为单位表示的两个时间之间的差异，例如`'hours'`。 ‘t1’和’t2’可以是Date或DateTime，如果指定’timezone’，它将应用于两个参数。如果不是，则使用来自数据类型’t1’和’t2’的时区。如果时区不相同，则结果将是未定义的。

支持的单位值:

| 单位 |
|------|
| 第二 |
| 分钟 |
| 小时 |
| 日   |
| 周   |
| 月   |
| 季   |
| 年   |

## 时隙（开始时间，持续时间，\[，大小\]) {#timeslotsstarttime-duration-size}

它返回一个时间数组，其中包括从从«StartTime»开始到«StartTime + Duration 秒»内的所有符合«size»（以秒为单位）步长的时间点。其中«size»是一个可选参数，默认为1800。
例如，`timeSlots(toDateTime('2012-01-01 12:20:00')，600) = [toDateTime（'2012-01-01 12:00:00'），toDateTime（'2012-01-01 12:30:00' ）]`。
这对于搜索在相应会话中综合浏览量是非常有用的。

## formatDateTime（时间，格式\[，时区\]) {#formatdatetimetime-format-timezone}

函数根据给定的格式字符串来格式化时间。请注意：格式字符串必须是常量表达式，例如：单个结果列不能有多种格式字符串。

支持的格式修饰符：
(«Example» 列是对`2018-01-02 22:33:44`的格式化结果)

| 修饰符 | 产品描述                                  | 示例       |
|--------|-------------------------------------------|------------|
| %C     | 年除以100并截断为整数(00-99)              | 20         |
| %d     | 月中的一天，零填充（01-31)                | 02         |
| %D     | 短MM/DD/YY日期，相当于%m/%d/%y            | 01/02/2018 |
| %e     | 月中的一天，空格填充（1-31)               | 2          |
| %F     | 短YYYY-MM-DD日期，相当于%Y-%m-%d          | 2018-01-02 |
| %H     | 24小时格式（00-23)                        | 22         |
| %I     | 小时12h格式（01-12)                       | 10         |
| %j     | 一年(001-366)                             | 002        |
| %m     | 月份为十进制数（01-12)                    | 01         |
| %M     | 分钟(00-59)                               | 33         |
| %n     | 换行符(")                                 |            |
| %p     | AM或PM指定                                | PM         |
| %R     | 24小时HH:MM时间，相当于%H:%M              | 22:33      |
| %S     | 第二(00-59)                               | 44         |
| %t     | 水平制表符(’)                             |            |
| %T     | ISO8601时间格式(HH:MM:SS)，相当于%H:%M:%S | 22:33:44   |
| %u     | ISO8601平日as编号，星期一为1(1-7)         | 2          |
| %V     | ISO8601周编号(01-53)                      | 01         |
| %w     | 周日为十进制数，周日为0(0-6)              | 2          |
| %y     | 年份，最后两位数字（00-99)                | 18         |
| %Y     | 年                                        | 2018       |
| %%     | %符号                                     | %          |

[来源文章](https://clickhouse.tech/docs/en/query_language/functions/date_time_functions/) <!--hide-->
