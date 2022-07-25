---
sidebar_position: 39
sidebar_label: Dates and Times
---

# Functions for Working with Dates and Times

Support for time zones.

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

## timeZone

Returns the timezone of the server.
If it is executed in the context of a distributed table, then it generates a normal column with values relevant to each shard. Otherwise it produces a constant value.

**Syntax**

``` sql
timeZone()
```

Alias: `timezone`.

**Returned value**

-   Timezone.

Type: [String](../../sql-reference/data-types/string.md).

## toTimeZone

Converts time or date and time to the specified time zone. The time zone is an attribute of the `Date` and `DateTime` data types. The internal value (number of seconds) of the table field or of the resultset's column does not change, the column's type changes and its string representation changes accordingly.

**Syntax**

``` sql
toTimezone(value, timezone)
```

Alias: `toTimezone`.

**Arguments**

-   `value` — Time or date and time. [DateTime64](../../sql-reference/data-types/datetime64.md).
-   `timezone` — Timezone for the returned value. [String](../../sql-reference/data-types/string.md). This argument is a constant, because `toTimezone` changes the timezone of a column (timezone is an attribute of `DateTime*` types).

**Returned value**

-   Date and time.

Type: [DateTime](../../sql-reference/data-types/datetime.md).

**Example**

Query:

```sql
SELECT toDateTime('2019-01-01 00:00:00', 'UTC') AS time_utc,
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

Result:

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

`toTimeZone(time_utc, 'Asia/Yekaterinburg')` changes the `DateTime('UTC')` type to `DateTime('Asia/Yekaterinburg')`. The value (Unixtimestamp) 1546300800 stays the same, but the string representation (the result of the toString() function) changes from `time_utc:   2019-01-01 00:00:00` to `time_yekat: 2019-01-01 05:00:00`.

## timeZoneOf

Returns the timezone name of [DateTime](../../sql-reference/data-types/datetime.md) or [DateTime64](../../sql-reference/data-types/datetime64.md) data types.

**Syntax**

``` sql
timeZoneOf(value)
```

Alias: `timezoneOf`.

**Arguments**

-   `value` — Date and time. [DateTime](../../sql-reference/data-types/datetime.md) or [DateTime64](../../sql-reference/data-types/datetime64.md).

**Returned value**

-   Timezone name.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

Query:
``` sql
SELECT timezoneOf(now());
```

Result:
``` text
┌─timezoneOf(now())─┐
│ Etc/UTC           │
└───────────────────┘
```

## timeZoneOffset

Returns a timezone offset in seconds from [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time). The function takes into account [daylight saving time](https://en.wikipedia.org/wiki/Daylight_saving_time) and historical timezone changes at the specified date and time.
[IANA timezone database](https://www.iana.org/time-zones) is used to calculate the offset.

**Syntax**

``` sql
timeZoneOffset(value)
```

Alias: `timezoneOffset`.

**Arguments**

-   `value` — Date and time. [DateTime](../../sql-reference/data-types/datetime.md) or [DateTime64](../../sql-reference/data-types/datetime64.md).

**Returned value**

-   Offset from UTC in seconds.

Type: [Int32](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT toDateTime('2021-04-21 10:20:30', 'America/New_York') AS Time, toTypeName(Time) AS Type,
       timeZoneOffset(Time) AS Offset_in_seconds, (Offset_in_seconds / 3600) AS Offset_in_hours;
```

Result:

``` text
┌────────────────Time─┬─Type─────────────────────────┬─Offset_in_seconds─┬─Offset_in_hours─┐
│ 2021-04-21 10:20:30 │ DateTime('America/New_York') │            -14400 │              -4 │
└─────────────────────┴──────────────────────────────┴───────────────────┴─────────────────┘
```

## toYear

Converts a date or date with time to a UInt16 number containing the year number (AD).

Alias: `YEAR`.

## toQuarter

Converts a date or date with time to a UInt8 number containing the quarter number.

Alias: `QUARTER`.

## toMonth

Converts a date or date with time to a UInt8 number containing the month number (1-12).

Alias: `MONTH`.

## toDayOfYear

Converts a date or date with time to a UInt16 number containing the number of the day of the year (1-366).

Alias: `DAYOFYEAR`.

## toDayOfMonth

Converts a date or date with time to a UInt8 number containing the number of the day of the month (1-31).

Aliases: `DAYOFMONTH`, `DAY`.

## toDayOfWeek

Converts a date or date with time to a UInt8 number containing the number of the day of the week (Monday is 1, and Sunday is 7).

Alias: `DAYOFWEEK`.

## toHour

Converts a date with time to a UInt8 number containing the number of the hour in 24-hour time (0-23).
This function assumes that if clocks are moved ahead, it is by one hour and occurs at 2 a.m., and if clocks are moved back, it is by one hour and occurs at 3 a.m. (which is not always true – even in Moscow the clocks were twice changed at a different time).

Alias: `HOUR`.

## toMinute

Converts a date with time to a UInt8 number containing the number of the minute of the hour (0-59).

Alias: `MINUTE`.

## toSecond

Converts a date with time to a UInt8 number containing the number of the second in the minute (0-59).
Leap seconds are not accounted for.

Alias: `SECOND`.

## toUnixTimestamp

For DateTime argument: converts value to the number with type UInt32 -- Unix Timestamp (https://en.wikipedia.org/wiki/Unix_time).
For String argument: converts the input string to the datetime according to the timezone (optional second argument, server timezone is used by default) and returns the corresponding unix timestamp.

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

:::note
The return type `toStartOf*` functions described below is `Date` or `DateTime`. Though these functions can take `DateTime64` as an argument, passing them a `DateTime64` that is out of the normal range (years 1900 - 2299) will give an incorrect result.
:::

## toStartOfYear

Rounds down a date or date with time to the first day of the year.
Returns the date.

## toStartOfISOYear

Rounds down a date or date with time to the first day of ISO year.
Returns the date.

## toStartOfQuarter

Rounds down a date or date with time to the first day of the quarter.
The first day of the quarter is either 1 January, 1 April, 1 July, or 1 October.
Returns the date.

## toStartOfMonth

Rounds down a date or date with time to the first day of the month.
Returns the date.

:::note
The behavior of parsing incorrect dates is implementation specific. ClickHouse may return zero date, throw an exception or do “natural” overflow.
:::

## toMonday

Rounds down a date or date with time to the nearest Monday.
Returns the date.

## toStartOfWeek(t\[,mode\])

Rounds down a date or date with time to the nearest Sunday or Monday by mode.
Returns the date.
The mode argument works exactly like the mode argument to toWeek(). For the single-argument syntax, a mode value of 0 is used.

## toStartOfDay

Rounds down a date with time to the start of the day.

## toStartOfHour

Rounds down a date with time to the start of the hour.

## toStartOfMinute

Rounds down a date with time to the start of the minute.

## toStartOfSecond

Truncates sub-seconds.

**Syntax**

``` sql
toStartOfSecond(value, [timezone])
```

**Arguments**

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
SELECT toStartOfSecond(dt64, 'Asia/Istanbul');
```

Result:

``` text
┌─toStartOfSecond(dt64, 'Asia/Istanbul')─┐
│                2020-01-01 13:20:30.000 │
└────────────────────────────────────────┘
```

**See also**

-   [Timezone](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) server configuration parameter.

## toStartOfFiveMinutes

Rounds down a date with time to the start of the five-minute interval.

## toStartOfTenMinutes

Rounds down a date with time to the start of the ten-minute interval.

## toStartOfFifteenMinutes

Rounds down the date with time to the start of the fifteen-minute interval.

## toStartOfInterval(time_or_data, INTERVAL x unit \[, time_zone\])

This is a generalization of other functions named `toStartOf*`. For example,
`toStartOfInterval(t, INTERVAL 1 year)` returns the same as `toStartOfYear(t)`,
`toStartOfInterval(t, INTERVAL 1 month)` returns the same as `toStartOfMonth(t)`,
`toStartOfInterval(t, INTERVAL 1 day)` returns the same as `toStartOfDay(t)`,
`toStartOfInterval(t, INTERVAL 15 minute)` returns the same as `toStartOfFifteenMinutes(t)` etc.

## toTime

Converts a date with time to a certain fixed date, while preserving the time.

## toRelativeYearNum

Converts a date with time or date to the number of the year, starting from a certain fixed point in the past.

## toRelativeQuarterNum

Converts a date with time or date to the number of the quarter, starting from a certain fixed point in the past.

## toRelativeMonthNum

Converts a date with time or date to the number of the month, starting from a certain fixed point in the past.

## toRelativeWeekNum

Converts a date with time or date to the number of the week, starting from a certain fixed point in the past.

## toRelativeDayNum

Converts a date with time or date to the number of the day, starting from a certain fixed point in the past.

## toRelativeHourNum

Converts a date with time or date to the number of the hour, starting from a certain fixed point in the past.

## toRelativeMinuteNum

Converts a date with time or date to the number of the minute, starting from a certain fixed point in the past.

## toRelativeSecondNum

Converts a date with time or date to the number of the second, starting from a certain fixed point in the past.

## toISOYear

Converts a date or date with time to a UInt16 number containing the ISO Year number.

## toISOWeek

Converts a date or date with time to a UInt8 number containing the ISO Week number.

## toWeek(date\[,mode\])

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

For mode values with a meaning of “contains January 1”, the week contains January 1 is week 1. It does not matter how many days in the new year the week contained, even if it contained only one day.

``` sql
toWeek(date, [, mode][, Timezone])
```

**Arguments**

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

## toYearWeek(date\[,mode\])

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

## date\_trunc

Truncates date and time data to the specified part of date.

**Syntax**

``` sql
date_trunc(unit, value[, timezone])
```

Alias: `dateTrunc`.

**Arguments**

-   `unit` — The type of interval to truncate the result. [String Literal](../syntax.md#syntax-string-literal).
    Possible values:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

-   `value` — Date and time. [DateTime](../../sql-reference/data-types/datetime.md) or [DateTime64](../../sql-reference/data-types/datetime64.md).
-   `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) for the returned value (optional). If not specified, the function uses the timezone of the `value` parameter. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   Value, truncated to the specified part of date.

Type: [Datetime](../../sql-reference/data-types/datetime.md).

**Example**

Query without timezone:

``` sql
SELECT now(), date_trunc('hour', now());
```

Result:

``` text
┌───────────────now()─┬─date_trunc('hour', now())─┐
│ 2020-09-28 10:40:45 │       2020-09-28 10:00:00 │
└─────────────────────┴───────────────────────────┘
```

Query with the specified timezone:

```sql
SELECT now(), date_trunc('hour', now(), 'Asia/Istanbul');
```

Result:

```text
┌───────────────now()─┬─date_trunc('hour', now(), 'Asia/Istanbul')─┐
│ 2020-09-28 10:46:26 │                        2020-09-28 13:00:00 │
└─────────────────────┴────────────────────────────────────────────┘
```

**See Also**

-   [toStartOfInterval](#tostartofintervaltime-or-data-interval-x-unit-time-zone)

## date\_add

Adds the time interval or date interval to the provided date or date with time.

**Syntax**

``` sql
date_add(unit, value, date)
```

Aliases: `dateAdd`, `DATE_ADD`.

**Arguments**

-   `unit` — The type of interval to add. [String](../../sql-reference/data-types/string.md).
    Possible values:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

-   `value` — Value of interval to add. [Int](../../sql-reference/data-types/int-uint.md).
-   `date` — The date or date with time to which `value` is added. [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Returned value**

Date or date with time obtained by adding `value`, expressed in `unit`, to `date`.

Type: [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Example**

Query:

```sql
SELECT date_add(YEAR, 3, toDate('2018-01-01'));
```

Result:

```text
┌─plus(toDate('2018-01-01'), toIntervalYear(3))─┐
│                                    2021-01-01 │
└───────────────────────────────────────────────┘
```

## date\_diff

Returns the difference between two dates or dates with time values.

**Syntax**

``` sql
date_diff('unit', startdate, enddate, [timezone])
```

Aliases: `dateDiff`, `DATE_DIFF`.

**Arguments**

-   `unit` — The type of interval for result. [String](../../sql-reference/data-types/string.md).
    Possible values:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

-   `startdate` — The first time value to subtract (the subtrahend). [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

-   `enddate` — The second time value to subtract from (the minuend). [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

-   `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) (optional). If specified, it is applied to both `startdate` and `enddate`. If not specified, timezones of `startdate` and `enddate` are used. If they are not the same, the result is unspecified. [String](../../sql-reference/data-types/string.md).

**Returned value**

Difference between `enddate` and `startdate` expressed in `unit`.

Type: [Int](../../sql-reference/data-types/int-uint.md).

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

## date\_sub

Subtracts the time interval or date interval from the provided date or date with time.

**Syntax**

``` sql
date_sub(unit, value, date)
```

Aliases: `dateSub`, `DATE_SUB`.

**Arguments**

-   `unit` — The type of interval to subtract. [String](../../sql-reference/data-types/string.md).
    Possible values:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

-   `value` — Value of interval to subtract. [Int](../../sql-reference/data-types/int-uint.md).
-   `date` — The date or date with time from which `value` is subtracted. [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Returned value**

Date or date with time obtained by subtracting `value`, expressed in `unit`, from `date`.

Type: [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Example**

Query:

``` sql
SELECT date_sub(YEAR, 3, toDate('2018-01-01'));
```

Result:

``` text
┌─minus(toDate('2018-01-01'), toIntervalYear(3))─┐
│                                     2015-01-01 │
└────────────────────────────────────────────────┘
```

## timestamp\_add

Adds the specified time value with the provided date or date time value.

**Syntax**

``` sql
timestamp_add(date, INTERVAL value unit)
```

Aliases: `timeStampAdd`, `TIMESTAMP_ADD`.

**Arguments**

-   `date` — Date or date with time. [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).
-   `value` — Value of interval to add. [Int](../../sql-reference/data-types/int-uint.md).
-   `unit` — The type of interval to add. [String](../../sql-reference/data-types/string.md).
    Possible values:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

**Returned value**

Date or date with time with the specified `value` expressed in `unit` added to `date`.

Type: [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Example**

Query:

```sql
select timestamp_add(toDate('2018-01-01'), INTERVAL 3 MONTH);
```

Result:

```text
┌─plus(toDate('2018-01-01'), toIntervalMonth(3))─┐
│                                     2018-04-01 │
└────────────────────────────────────────────────┘
```

## timestamp\_sub

Subtracts the time interval from the provided date or date with time.

**Syntax**

``` sql
timestamp_sub(unit, value, date)
```

Aliases: `timeStampSub`, `TIMESTAMP_SUB`.

**Arguments**

-   `unit` — The type of interval to subtract. [String](../../sql-reference/data-types/string.md).
    Possible values:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

-   `value` — Value of interval to subtract. [Int](../../sql-reference/data-types/int-uint.md).
-   `date` — Date or date with time. [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Returned value**

Date or date with time obtained by subtracting `value`, expressed in `unit`, from `date`.

Type: [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Example**

Query:

```sql
select timestamp_sub(MONTH, 5, toDateTime('2018-12-18 01:02:03'));
```

Result:

```text
┌─minus(toDateTime('2018-12-18 01:02:03'), toIntervalMonth(5))─┐
│                                          2018-07-18 01:02:03 │
└──────────────────────────────────────────────────────────────┘
```

## now

Returns the current date and time at the moment of query analysis. The function is a constant expression.

**Syntax**

``` sql
now([timezone])
```

**Arguments**

-   `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) for the returned value (optional). [String](../../sql-reference/data-types/string.md).

**Returned value**

-   Current date and time.

Type: [Datetime](../../sql-reference/data-types/datetime.md).

**Example**

Query without timezone:

``` sql
SELECT now();
```

Result:

``` text
┌───────────────now()─┐
│ 2020-10-17 07:42:09 │
└─────────────────────┘
```

Query with the specified timezone:

``` sql
SELECT now('Asia/Istanbul');
```

Result:

``` text
┌─now('Asia/Istanbul')─┐
│  2020-10-17 10:42:23 │
└──────────────────────┘
```

## nowInBlock

Returns the current date and time at the moment of processing of each block of data. In contrast to the function `now`, it is not a constant expression, and the returned value will be different in different blocks for long-running queries.

It makes sense to use this function to generate the current time in long-running INSERT SELECT queries.

## today

Accepts zero arguments and returns the current date at one of the moments of query analysis.
The same as ‘toDate(now())’.

## yesterday

Accepts zero arguments and returns yesterday’s date at one of the moments of query analysis.
The same as ‘today() - 1’.

## timeSlot

Rounds the time to the half hour.

## toYYYYMM

Converts a date or date with time to a UInt32 number containing the year and month number (YYYY \* 100 + MM).

## toYYYYMMDD

Converts a date or date with time to a UInt32 number containing the year and month number (YYYY \* 10000 + MM \* 100 + DD).

## toYYYYMMDDhhmmss

Converts a date or date with time to a UInt64 number containing the year and month number (YYYY \* 10000000000 + MM \* 100000000 + DD \* 1000000 + hh \* 10000 + mm \* 100 + ss).

## addYears, addMonths, addWeeks, addDays, addHours, addMinutes, addSeconds, addQuarters

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

## subtractYears, subtractMonths, subtractWeeks, subtractDays, subtractHours, subtractMinutes, subtractSeconds, subtractQuarters

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

## timeSlots(StartTime, Duration,\[, Size\])

For a time interval starting at ‘StartTime’ and continuing for ‘Duration’ seconds, it returns an array of moments in time, consisting of points from this interval rounded down to the ‘Size’ in seconds. ‘Size’ is an optional parameter: a constant UInt32, set to 1800 by default.
For example, `timeSlots(toDateTime('2012-01-01 12:20:00'), 600) = [toDateTime('2012-01-01 12:00:00'), toDateTime('2012-01-01 12:30:00')]`.
This is necessary for searching for pageviews in the corresponding session.

## formatDateTime

Formats a Time according to the given Format string. Format is a constant expression, so you cannot have multiple formats for a single result column.

**Syntax**

``` sql
formatDateTime(Time, Format\[, Timezone\])
```

**Returned value(s)**

Returns time and date values according to the determined format.

**Replacement fields**
Using replacement fields, you can define a pattern for the resulting string. “Example” column shows formatting result for `2018-01-02 22:33:44`.

| Placeholder | Description                                             | Example    |
|----------|---------------------------------------------------------|------------|
| %C       | year divided by 100 and truncated to integer (00-99)    | 20         |
| %d       | day of the month, zero-padded (01-31)                   | 02         |
| %D       | Short MM/DD/YY date, equivalent to %m/%d/%y             | 01/02/18   |
| %e       | day of the month, space-padded ( 1-31)                  | &nbsp; 2   |
| %F       | short YYYY-MM-DD date, equivalent to %Y-%m-%d           | 2018-01-02 |
| %G       | four-digit year format for ISO week number, calculated from the week-based year [defined by the ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Week_dates) standard, normally useful only with %V  | 2018         |
| %g       | two-digit year format, aligned to ISO 8601, abbreviated from four-digit notation                                | 18       |
| %H       | hour in 24h format (00-23)                              | 22         |
| %I       | hour in 12h format (01-12)                              | 10         |
| %j       | day of the year (001-366)                               | 002        |
| %m       | month as a decimal number (01-12)                       | 01         |
| %M       | minute (00-59)                                          | 33         |
| %n       | new-line character (‘’)                                 |            |
| %p       | AM or PM designation                                    | PM         |
| %Q       | Quarter (1-4)                                           | 1          |
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

**Example**

Query:

``` sql
SELECT formatDateTime(toDate('2010-01-04'), '%g')
```

Result:

```
┌─formatDateTime(toDate('2010-01-04'), '%g')─┐
│ 10                                         │
└────────────────────────────────────────────┘
```

## dateName

Returns specified part of date.

**Syntax**

``` sql
dateName(date_part, date)
```

**Arguments**

-   `date_part` — Date part. Possible values: 'year', 'quarter', 'month', 'week', 'dayofyear', 'day', 'weekday', 'hour', 'minute', 'second'. [String](../../sql-reference/data-types/string.md).
-   `date` — Date. [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md) or [DateTime64](../../sql-reference/data-types/datetime64.md).
-   `timezone` — Timezone. Optional. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   The specified part of date.

Type: [String](../../sql-reference/data-types/string.md#string)

**Example**

Query:

```sql
WITH toDateTime('2021-04-14 11:22:33') AS date_value
SELECT dateName('year', date_value), dateName('month', date_value), dateName('day', date_value);
```

Result:

```text
┌─dateName('year', date_value)─┬─dateName('month', date_value)─┬─dateName('day', date_value)─┐
│ 2021                         │ April                         │ 14                          │
└──────────────────────────────┴───────────────────────────────┴─────────────────────────────
```

## FROM\_UNIXTIME

Function converts Unix timestamp to a calendar date and a time of a day. When there is only a single argument of [Integer](../../sql-reference/data-types/int-uint.md) type, it acts in the same way as [toDateTime](../../sql-reference/functions/type-conversion-functions.md#todatetime) and return [DateTime](../../sql-reference/data-types/datetime.md) type.

**Example:**

Query:

```sql
SELECT FROM_UNIXTIME(423543535);
```

Result:

```text
┌─FROM_UNIXTIME(423543535)─┐
│      1983-06-04 10:58:55 │
└──────────────────────────┘
```

When there are two arguments: first is an [Integer](../../sql-reference/data-types/int-uint.md) or [DateTime](../../sql-reference/data-types/datetime.md), second is a constant format string — it acts in the same way as [formatDateTime](#formatdatetime) and return [String](../../sql-reference/data-types/string.md#string) type.

For example:

```sql
SELECT FROM_UNIXTIME(1234334543, '%Y-%m-%d %R:%S') AS DateTime;
```

```text
┌─DateTime────────────┐
│ 2009-02-11 14:42:23 │
└─────────────────────┘
```

## toModifiedJulianDay

Converts a [Proleptic Gregorian calendar](https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar) date in text form `YYYY-MM-DD` to a [Modified Julian Day](https://en.wikipedia.org/wiki/Julian_day#Variants) number in Int32. This function supports date from `0000-01-01` to `9999-12-31`. It raises an exception if the argument cannot be parsed as a date, or the date is invalid.

**Syntax**

``` sql
toModifiedJulianDay(date)
```

**Arguments**

-   `date` — Date in text form. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).

**Returned value**

-   Modified Julian Day number.

Type: [Int32](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT toModifiedJulianDay('2020-01-01');
```

Result:

``` text
┌─toModifiedJulianDay('2020-01-01')─┐
│                             58849 │
└───────────────────────────────────┘
```

## toModifiedJulianDayOrNull

Similar to [toModifiedJulianDay()](#tomodifiedjulianday), but instead of raising exceptions it returns `NULL`.

**Syntax**

``` sql
toModifiedJulianDayOrNull(date)
```

**Arguments**

-   `date` — Date in text form. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).

**Returned value**

-   Modified Julian Day number.

Type: [Nullable(Int32)](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT toModifiedJulianDayOrNull('2020-01-01');
```

Result:

``` text
┌─toModifiedJulianDayOrNull('2020-01-01')─┐
│                                   58849 │
└─────────────────────────────────────────┘
```

## fromModifiedJulianDay

Converts a [Modified Julian Day](https://en.wikipedia.org/wiki/Julian_day#Variants) number to a [Proleptic Gregorian calendar](https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar) date in text form `YYYY-MM-DD`. This function supports day number from `-678941` to `2973119` (which represent 0000-01-01 and 9999-12-31 respectively). It raises an exception if the day number is outside of the supported range.

**Syntax**

``` sql
fromModifiedJulianDay(day)
```

**Arguments**

-   `day` — Modified Julian Day number. [Any integral types](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Date in text form.

Type: [String](../../sql-reference/data-types/string.md)

**Example**

Query:

``` sql
SELECT fromModifiedJulianDay(58849);
```

Result:

``` text
┌─fromModifiedJulianDay(58849)─┐
│ 2020-01-01                   │
└──────────────────────────────┘
```

## fromModifiedJulianDayOrNull

Similar to [fromModifiedJulianDayOrNull()](#frommodifiedjuliandayornull), but instead of raising exceptions it returns `NULL`.

**Syntax**

``` sql
fromModifiedJulianDayOrNull(day)
```

**Arguments**

-   `day` — Modified Julian Day number. [Any integral types](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Date in text form.

Type: [Nullable(String)](../../sql-reference/data-types/string.md)

**Example**

Query:

``` sql
SELECT fromModifiedJulianDayOrNull(58849);
```

Result:

``` text
┌─fromModifiedJulianDayOrNull(58849)─┐
│ 2020-01-01                         │
└────────────────────────────────────┘
```
