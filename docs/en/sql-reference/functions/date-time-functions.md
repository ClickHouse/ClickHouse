---
toc_priority: 39
toc_title: Dates and Times
---

# Functions for Working with Dates and Times {#functions-for-working-with-dates-and-times}

Support for time zones

All functions for working with the date and time that have a logical use for the time zone can accept a second optional time zone argument. Example: Asia/Yekaterinburg. In this case, they use the specified time zone instead of the local (default) one.

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

Only time zones that differ from UTC by a whole number of hours are supported.

## toTimeZone {#totimezone}

Convert time or date and time to the specified time zone.

## toYear {#toyear}

Converts a date or date with time to a UInt16 number containing the year number (AD).

## toQuarter {#toquarter}

Converts a date or date with time to a UInt8 number containing the quarter number.

## toMonth {#tomonth}

Converts a date or date with time to a UInt8 number containing the month number (1-12).

## toDayOfYear {#todayofyear}

Converts a date or date with time to a UInt16 number containing the number of the day of the year (1-366).

## toDayOfMonth {#todayofmonth}

Converts a date or date with time to a UInt8 number containing the number of the day of the month (1-31).

## toDayOfWeek {#todayofweek}

Converts a date or date with time to a UInt8 number containing the number of the day of the week (Monday is 1, and Sunday is 7).

## toHour {#tohour}

Converts a date with time to a UInt8 number containing the number of the hour in 24-hour time (0-23).
This function assumes that if clocks are moved ahead, it is by one hour and occurs at 2 a.m., and if clocks are moved back, it is by one hour and occurs at 3 a.m. (which is not always true – even in Moscow the clocks were twice changed at a different time).

## toMinute {#tominute}

Converts a date with time to a UInt8 number containing the number of the minute of the hour (0-59).

## toSecond {#tosecond}

Converts a date with time to a UInt8 number containing the number of the second in the minute (0-59).
Leap seconds are not accounted for.

## toUnixTimestamp {#to-unix-timestamp}

For DateTime argument: converts value to its internal numeric representation (Unix Timestamp).
For String argument: parse datetime from string according to the timezone (optional second argument, server timezone is used by default) and returns the corresponding unix timestamp.
For Date argument: the behaviour is unspecified.

**Syntax**

``` sql
toUnixTimestamp(datetime)
toUnixTimestamp(str, [timezone])
```

**Returned value**

-   Returns the unix timestamp.

Type: `UInt32`.

**Example**

Query:

``` sql
SELECT toUnixTimestamp('2017-11-05 08:07:47', 'Asia/Tokyo') AS unix_timestamp
```

Result:

``` text
┌─unix_timestamp─┐
│     1509836867 │
└────────────────┘
```

## toStartOfYear {#tostartofyear}

Rounds down a date or date with time to the first day of the year.
Returns the date.

## toStartOfISOYear {#tostartofisoyear}

Rounds down a date or date with time to the first day of ISO year.
Returns the date.

## toStartOfQuarter {#tostartofquarter}

Rounds down a date or date with time to the first day of the quarter.
The first day of the quarter is either 1 January, 1 April, 1 July, or 1 October.
Returns the date.

## toStartOfMonth {#tostartofmonth}

Rounds down a date or date with time to the first day of the month.
Returns the date.

!!! attention "Attention"
    The behavior of parsing incorrect dates is implementation specific. ClickHouse may return zero date, throw an exception or do “natural” overflow.

## toMonday {#tomonday}

Rounds down a date or date with time to the nearest Monday.
Returns the date.

## toStartOfWeek(t\[,mode\]) {#tostartofweektmode}

Rounds down a date or date with time to the nearest Sunday or Monday by mode.
Returns the date.
The mode argument works exactly like the mode argument to toWeek(). For the single-argument syntax, a mode value of 0 is used.

## toStartOfDay {#tostartofday}

Rounds down a date with time to the start of the day.

## toStartOfHour {#tostartofhour}

Rounds down a date with time to the start of the hour.

## toStartOfMinute {#tostartofminute}

Rounds down a date with time to the start of the minute.

## toStartOfSecond {#tostartofsecond}

Truncates sub-seconds.

**Syntax**

``` sql
toStartOfSecond(value[, timezone])
```

**Parameters**

-   `value` — Date and time. [DateTime64](../../sql-reference/data-types/datetime64.md).
-   `timezone` — [Timezone](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) for the returned value (optional). If not specified, the function uses the timezone of the `value` parameter. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   Input value without sub-seconds.

Type: [DateTime64](../../sql-reference/data-types/datetime64.md).

**Examples**

Query without timezone:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999', 3) AS dt64
SELECT toStartOfSecond(dt64);
```

Result:

``` text
┌───toStartOfSecond(dt64)─┐
│ 2020-01-01 10:20:30.000 │
└─────────────────────────┘
```

Query with timezone:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999', 3) AS dt64
SELECT toStartOfSecond(dt64, 'Europe/Moscow');
```

Result:

``` text
┌─toStartOfSecond(dt64, 'Europe/Moscow')─┐
│                2020-01-01 13:20:30.000 │
└────────────────────────────────────────┘
```

**See also**

-   [Timezone](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) server configuration parameter.

## toStartOfFiveMinute {#tostartoffiveminute}

Rounds down a date with time to the start of the five-minute interval.

## toStartOfTenMinutes {#tostartoftenminutes}

Rounds down a date with time to the start of the ten-minute interval.

## toStartOfFifteenMinutes {#tostartoffifteenminutes}

Rounds down the date with time to the start of the fifteen-minute interval.

## toStartOfInterval(time\_or\_data, INTERVAL x unit \[, time\_zone\]) {#tostartofintervaltime-or-data-interval-x-unit-time-zone}

This is a generalization of other functions named `toStartOf*`. For example,
`toStartOfInterval(t, INTERVAL 1 year)` returns the same as `toStartOfYear(t)`,
`toStartOfInterval(t, INTERVAL 1 month)` returns the same as `toStartOfMonth(t)`,
`toStartOfInterval(t, INTERVAL 1 day)` returns the same as `toStartOfDay(t)`,
`toStartOfInterval(t, INTERVAL 15 minute)` returns the same as `toStartOfFifteenMinutes(t)` etc.

## toTime {#totime}

Converts a date with time to a certain fixed date, while preserving the time.

## toRelativeYearNum {#torelativeyearnum}

Converts a date with time or date to the number of the year, starting from a certain fixed point in the past.

## toRelativeQuarterNum {#torelativequarternum}

Converts a date with time or date to the number of the quarter, starting from a certain fixed point in the past.

## toRelativeMonthNum {#torelativemonthnum}

Converts a date with time or date to the number of the month, starting from a certain fixed point in the past.

## toRelativeWeekNum {#torelativeweeknum}

Converts a date with time or date to the number of the week, starting from a certain fixed point in the past.

## toRelativeDayNum {#torelativedaynum}

Converts a date with time or date to the number of the day, starting from a certain fixed point in the past.

## toRelativeHourNum {#torelativehournum}

Converts a date with time or date to the number of the hour, starting from a certain fixed point in the past.

## toRelativeMinuteNum {#torelativeminutenum}

Converts a date with time or date to the number of the minute, starting from a certain fixed point in the past.

## toRelativeSecondNum {#torelativesecondnum}

Converts a date with time or date to the number of the second, starting from a certain fixed point in the past.

## toISOYear {#toisoyear}

Converts a date or date with time to a UInt16 number containing the ISO Year number.

## toISOWeek {#toisoweek}

Converts a date or date with time to a UInt8 number containing the ISO Week number.

## toWeek(date\[,mode\]) {#toweekdatemode}

This function returns the week number for date or datetime. The two-argument form of toWeek() enables you to specify whether the week starts on Sunday or Monday and whether the return value should be in the range from 0 to 53 or from 1 to 53. If the mode argument is omitted, the default mode is 0.
`toISOWeek()`is a compatibility function that is equivalent to `toWeek(date,3)`.
The following table describes how the mode argument works.

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

For mode values with a meaning of “with 4 or more days this year,” weeks are numbered according to ISO 8601:1988:

-   If the week containing January 1 has 4 or more days in the new year, it is week 1.

-   Otherwise, it is the last week of the previous year, and the next week is week 1.

For mode values with a meaning of “contains January 1”, the week contains January 1 is week 1. It doesn’t matter how many days in the new year the week contained, even if it contained only one day.

``` sql
toWeek(date, [, mode][, Timezone])
```

**Parameters**

-   `date` – Date or DateTime.
-   `mode` – Optional parameter, Range of values is \[0,9\], default is 0.
-   `Timezone` – Optional parameter, it behaves like any other conversion function.

**Example**

``` sql
SELECT toDate('2016-12-27') AS date, toWeek(date) AS week0, toWeek(date,1) AS week1, toWeek(date,9) AS week9;
```

``` text
┌───────date─┬─week0─┬─week1─┬─week9─┐
│ 2016-12-27 │    52 │    52 │     1 │
└────────────┴───────┴───────┴───────┘
```

## toYearWeek(date\[,mode\]) {#toyearweekdatemode}

Returns year and week for a date. The year in the result may be different from the year in the date argument for the first and the last week of the year.

The mode argument works exactly like the mode argument to toWeek(). For the single-argument syntax, a mode value of 0 is used.

`toISOYear()`is a compatibility function that is equivalent to `intDiv(toYearWeek(date,3),100)`.

**Example**

``` sql
SELECT toDate('2016-12-27') AS date, toYearWeek(date) AS yearWeek0, toYearWeek(date,1) AS yearWeek1, toYearWeek(date,9) AS yearWeek9;
```

``` text
┌───────date─┬─yearWeek0─┬─yearWeek1─┬─yearWeek9─┐
│ 2016-12-27 │    201652 │    201652 │    201701 │
└────────────┴───────────┴───────────┴───────────┘
```

## now {#now}

Accepts zero arguments and returns the current time at one of the moments of request execution.
This function returns a constant, even if the request took a long time to complete.

## today {#today}

Accepts zero arguments and returns the current date at one of the moments of request execution.
The same as ‘toDate(now())’.

## yesterday {#yesterday}

Accepts zero arguments and returns yesterday’s date at one of the moments of request execution.
The same as ‘today() - 1’.

## timeSlot {#timeslot}

Rounds the time to the half hour.
This function is specific to Yandex.Metrica, since half an hour is the minimum amount of time for breaking a session into two sessions if a tracking tag shows a single user’s consecutive pageviews that differ in time by strictly more than this amount. This means that tuples (the tag ID, user ID, and time slot) can be used to search for pageviews that are included in the corresponding session.

## toYYYYMM {#toyyyymm}

Converts a date or date with time to a UInt32 number containing the year and month number (YYYY \* 100 + MM).

## toYYYYMMDD {#toyyyymmdd}

Converts a date or date with time to a UInt32 number containing the year and month number (YYYY \* 10000 + MM \* 100 + DD).

## toYYYYMMDDhhmmss {#toyyyymmddhhmmss}

Converts a date or date with time to a UInt64 number containing the year and month number (YYYY \* 10000000000 + MM \* 100000000 + DD \* 1000000 + hh \* 10000 + mm \* 100 + ss).

## addYears, addMonths, addWeeks, addDays, addHours, addMinutes, addSeconds, addQuarters {#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters}

Function adds a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime. For example:

``` sql
WITH
    toDate('2018-01-01') AS date,
    toDateTime('2018-01-01 00:00:00') AS date_time
SELECT
    addYears(date, 1) AS add_years_with_date,
    addYears(date_time, 1) AS add_years_with_date_time
```

``` text
┌─add_years_with_date─┬─add_years_with_date_time─┐
│          2019-01-01 │      2019-01-01 00:00:00 │
└─────────────────────┴──────────────────────────┘
```

## subtractYears, subtractMonths, subtractWeeks, subtractDays, subtractHours, subtractMinutes, subtractSeconds, subtractQuarters {#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters}

Function subtract a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime. For example:

``` sql
WITH
    toDate('2019-01-01') AS date,
    toDateTime('2019-01-01 00:00:00') AS date_time
SELECT
    subtractYears(date, 1) AS subtract_years_with_date,
    subtractYears(date_time, 1) AS subtract_years_with_date_time
```

``` text
┌─subtract_years_with_date─┬─subtract_years_with_date_time─┐
│               2018-01-01 │           2018-01-01 00:00:00 │
└──────────────────────────┴───────────────────────────────┘
```

## dateDiff {#datediff}

Returns the difference between two Date or DateTime values.

**Syntax**

``` sql
dateDiff('unit', startdate, enddate, [timezone])
```

**Parameters**

-   `unit` — Time unit, in which the returned value is expressed. [String](../../sql-reference/syntax.md#syntax-string-literal).

        Supported values:

        | unit   |
        | ---- |
        |second  |
        |minute  |
        |hour    |
        |day     |
        |week    |
        |month   |
        |quarter |
        |year    |

-   `startdate` — The first time value to compare. [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

-   `enddate` — The second time value to compare. [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

-   `timezone` — Optional parameter. If specified, it is applied to both `startdate` and `enddate`. If not specified, timezones of `startdate` and `enddate` are used. If they are not the same, the result is unspecified.

**Returned value**

Difference between `startdate` and `enddate` expressed in `unit`.

Type: `int`.

**Example**

Query:

``` sql
SELECT dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'));
```

Result:

``` text
┌─dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'))─┐
│                                                                                     25 │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

## timeSlots(StartTime, Duration,\[, Size\]) {#timeslotsstarttime-duration-size}

For a time interval starting at ‘StartTime’ and continuing for ‘Duration’ seconds, it returns an array of moments in time, consisting of points from this interval rounded down to the ‘Size’ in seconds. ‘Size’ is an optional parameter: a constant UInt32, set to 1800 by default.
For example, `timeSlots(toDateTime('2012-01-01 12:20:00'), 600) = [toDateTime('2012-01-01 12:00:00'), toDateTime('2012-01-01 12:30:00')]`.
This is necessary for searching for pageviews in the corresponding session.

## formatDateTime(Time, Format\[, Timezone\]) {#formatdatetime}

Function formats a Time according given Format string. N.B.: Format is a constant expression, e.g. you can not have multiple formats for single result column.

Supported modifiers for Format:
(“Example” column shows formatting result for time `2018-01-02 22:33:44`)

| Modifier | Description                                             | Example    |
|----------|---------------------------------------------------------|------------|
| %C       | year divided by 100 and truncated to integer (00-99)    | 20         |
| %d       | day of the month, zero-padded (01-31)                   | 02         |
| %D       | Short MM/DD/YY date, equivalent to %m/%d/%y             | 01/02/18   |
| %e       | day of the month, space-padded ( 1-31)                  | 2          |
| %F       | short YYYY-MM-DD date, equivalent to %Y-%m-%d           | 2018-01-02 |
| %H       | hour in 24h format (00-23)                              | 22         |
| %I       | hour in 12h format (01-12)                              | 10         |
| %j       | day of the year (001-366)                               | 002        |
| %m       | month as a decimal number (01-12)                       | 01         |
| %M       | minute (00-59)                                          | 33         |
| %n       | new-line character (‘’)                                 |            |
| %p       | AM or PM designation                                    | PM         |
| %R       | 24-hour HH:MM time, equivalent to %H:%M                 | 22:33      |
| %S       | second (00-59)                                          | 44         |
| %t       | horizontal-tab character (’)                            |            |
| %T       | ISO 8601 time format (HH:MM:SS), equivalent to %H:%M:%S | 22:33:44   |
| %u       | ISO 8601 weekday as number with Monday as 1 (1-7)       | 2          |
| %V       | ISO 8601 week number (01-53)                            | 01         |
| %w       | weekday as a decimal number with Sunday as 0 (0-6)      | 2          |
| %y       | Year, last two digits (00-99)                           | 18         |
| %Y       | Year                                                    | 2018       |
| %%       | a % sign                                                | %          |

[Original article](https://clickhouse.tech/docs/en/query_language/functions/date_time_functions/) <!--hide-->

## FROM_UNIXTIME

When there is only single argument of integer type, it act in the same way as `toDateTime` and return [DateTime](../../sql-reference/data-types/datetime.md).
type.

For example:

```sql
SELECT FROM_UNIXTIME(423543535)
```

```text
┌─FROM_UNIXTIME(423543535)─┐
│      1983-06-04 10:58:55 │
└──────────────────────────┘
```

When there are two arguments, first is integer or DateTime, second is constant format string, it act in the same way as `formatDateTime` and return `String` type.

For example:

```sql
SELECT FROM_UNIXTIME(1234334543, '%Y-%m-%d %R:%S') AS DateTime
```

```text
┌─DateTime────────────┐
│ 2009-02-11 14:42:23 │
└─────────────────────┘
```
