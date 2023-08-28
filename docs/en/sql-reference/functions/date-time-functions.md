---
slug: /en/sql-reference/functions/date-time-functions
sidebar_position: 45
sidebar_label: Dates and Times
---

# Functions for Working with Dates and Times

Most functions in this section accept an optional time zone argument, e.g. `Europe/Amsterdam`. In this case, the time zone is the specified one instead of the local (default) one.

**Example**

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

## makeDate

Creates a [Date](../../sql-reference/data-types/date.md)
- from a year, month and day argument, or
- from a year and day of year argument.

**Syntax**

``` sql
makeDate(year, month, day);
makeDate(year, day_of_year);
```

Alias:
- `MAKEDATE(year, month, day);`
- `MAKEDATE(year, day_of_year);`

**Arguments**

- `year` — Year. [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).
- `month` — Month. [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).
- `day` — Day. [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).
- `day_of_year` — Day of the year. [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).

**Returned value**

- A date created from the arguments.

Type: [Date](../../sql-reference/data-types/date.md).

**Example**

Create a Date from a year, month and day:

``` sql
SELECT makeDate(2023, 2, 28) AS Date;
```

Result:

``` text
┌───────date─┐
│ 2023-02-28 │
└────────────┘
```

Create a Date from a year and day of year argument:

``` sql
SELECT makeDate(2023, 42) AS Date;
```

Result:

``` text
┌───────date─┐
│ 2023-02-11 │
└────────────┘
```
## makeDate32

Like [makeDate](#makeDate) but produces a [Date32](../../sql-reference/data-types/date32.md).

## makeDateTime

Creates a [DateTime](../../sql-reference/data-types/datetime.md) from a year, month, day, hour, minute and second argument.

**Syntax**

``` sql
makeDateTime(year, month, day, hour, minute, second[, timezone])
```

**Arguments**

- `year` — Year. [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).
- `month` — Month. [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).
- `day` — Day. [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).
- `hour` — Hour. [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).
- `minute` — Minute. [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).
- `second` — Second. [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).
- `timezone` — [Timezone](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) for the returned value (optional).

**Returned value**

- A date with time created from the arguments.

Type: [DateTime](../../sql-reference/data-types/datetime.md).

**Example**

``` sql
SELECT makeDateTime(2023, 2, 28, 17, 12, 33) AS DateTime;
```

Result:

``` text
┌────────────DateTime─┐
│ 2023-02-28 17:12:33 │
└─────────────────────┘
```

## makeDateTime64

Like [makeDateTime](#makedatetime) but produces a [DateTime64](../../sql-reference/data-types/datetime64.md).

**Syntax**

``` sql
makeDateTime32(year, month, day, hour, minute, second[, fraction[, precision[, timezone]]])
```

## timeZone

Returns the timezone of the current session, i.e. the value of setting [session_timezone](../../operations/settings/settings.md#session_timezone).
If the function is executed in the context of a distributed table, then it generates a normal column with values relevant to each shard, otherwise it produces a constant value.

**Syntax**

```sql
timeZone()
```

Alias: `timezone`.

**Returned value**

- Timezone.

Type: [String](../../sql-reference/data-types/string.md).

**See also**

- [serverTimeZone](#serverTimeZone)

## serverTimeZone

Returns the timezone of the server, i.e. the value of setting [timezone](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone).
If the function is executed in the context of a distributed table, then it generates a normal column with values relevant to each shard. Otherwise, it produces a constant value.

**Syntax**

``` sql
serverTimeZone()
```

Alias: `serverTimezone`.

**Returned value**

-   Timezone.

Type: [String](../../sql-reference/data-types/string.md).

**See also**

- [timeZone](#timeZone)

## toTimeZone

Converts a date or date with time to the specified time zone. Does not change the internal value (number of unix seconds) of the data, only the value's time zone attribute and the value's string representation changes.

**Syntax**

``` sql
toTimezone(value, timezone)
```

Alias: `toTimezone`.

**Arguments**

- `value` — Time or date and time. [DateTime64](../../sql-reference/data-types/datetime64.md).
- `timezone` — Timezone for the returned value. [String](../../sql-reference/data-types/string.md). This argument is a constant, because `toTimezone` changes the timezone of a column (timezone is an attribute of `DateTime*` types).

**Returned value**

- Date and time.

Type: [DateTime](../../sql-reference/data-types/datetime.md).

**Example**

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

## timeZoneOf

Returns the timezone name of [DateTime](../../sql-reference/data-types/datetime.md) or [DateTime64](../../sql-reference/data-types/datetime64.md) data types.

**Syntax**

``` sql
timeZoneOf(value)
```

Alias: `timezoneOf`.

**Arguments**

- `value` — Date and time. [DateTime](../../sql-reference/data-types/datetime.md) or [DateTime64](../../sql-reference/data-types/datetime64.md).

**Returned value**

- Timezone name.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

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

Returns the timezone offset in seconds from [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time).
The function [daylight saving time](https://en.wikipedia.org/wiki/Daylight_saving_time) and historical timezone changes at the specified date and time into account.
The [IANA timezone database](https://www.iana.org/time-zones) is used to calculate the offset.

**Syntax**

``` sql
timeZoneOffset(value)
```

Alias: `timezoneOffset`.

**Arguments**

- `value` — Date and time. [DateTime](../../sql-reference/data-types/datetime.md) or [DateTime64](../../sql-reference/data-types/datetime64.md).

**Returned value**

- Offset from UTC in seconds.

Type: [Int32](../../sql-reference/data-types/int-uint.md).

**Example**

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

Converts a date or date with time to the year number (AD) as UInt16 value.

Alias: `YEAR`.

## toQuarter

Converts a date or date with time to the quarter number as UInt8 value.

Alias: `QUARTER`.

## toMonth

Converts a date or date with time to the month number (1-12) as UInt8 value.

Alias: `MONTH`.

## toDayOfYear

Converts a date or date with time to the number of the day of the year (1-366) as UInt16 value.

Alias: `DAYOFYEAR`.

## toDayOfMonth

Converts a date or date with time to the number of the day in the month (1-31) as UInt8 value.

Aliases: `DAYOFMONTH`, `DAY`.

## toDayOfWeek

Converts a date or date with time to the number of the day in the week as UInt8 value.

The two-argument form of `toDayOfWeek()` enables you to specify whether the week starts on Monday or Sunday, and whether the return value should be in the range from 0 to 6 or 1 to 7. If the mode argument is omitted, the default mode is 0. The time zone of the date can be specified as the third argument.

| Mode | First day of week | Range                                          |
|------|-------------------|------------------------------------------------|
| 0    | Monday            | 1-7: Monday = 1, Tuesday = 2, ..., Sunday = 7  |
| 1    | Monday            | 0-6: Monday = 0, Tuesday = 1, ..., Sunday = 6  |
| 2    | Sunday            | 0-6: Sunday = 0, Monday = 1, ..., Saturday = 6 |
| 3    | Sunday            | 1-7: Sunday = 1, Monday = 2, ..., Saturday = 7 |

Alias: `DAYOFWEEK`.

**Syntax**

``` sql
toDayOfWeek(t[, mode[, timezone]])
```

## toHour

Converts a date with time the number of the hour in 24-hour time (0-23) as UInt8 value.

Assumes that if clocks are moved ahead, it is by one hour and occurs at 2 a.m., and if clocks are moved back, it is by one hour and occurs at 3 a.m. (which is not always true – even in Moscow the clocks were twice changed at a different time).

Alias: `HOUR`.

## toMinute

Converts a date with time to the number of the minute of the hour (0-59) as UInt8 value.

Alias: `MINUTE`.

## toSecond

Converts a date with time to the second in the minute (0-59) as UInt8 value. Leap seconds are not considered.

Alias: `SECOND`.

## toUnixTimestamp

Converts a string, a date or a date with time to the [Unix Timestamp](https://en.wikipedia.org/wiki/Unix_time) in `UInt32` representation.

If the function is called with a string, it accepts an optional timezone argument.

**Syntax**

``` sql
toUnixTimestamp(date)
toUnixTimestamp(str, [timezone])
```

**Returned value**

- Returns the unix timestamp.

Type: `UInt32`.

**Example**

``` sql
SELECT
    '2017-11-05 08:07:47' AS dt_str,
    toUnixTimestamp(dt_str) AS from_str,
    toUnixTimestamp(dt_str, 'Asia/Tokyo') AS from_str_tokyo,
    toUnixTimestamp(toDateTime(dt_str)) AS from_datetime,
    toUnixTimestamp(toDateTime64(dt_str, 0)) AS from_datetime64,
    toUnixTimestamp(toDate(dt_str)) AS from_date,
    toUnixTimestamp(toDate32(dt_str)) AS from_date32
FORMAT Vertical;
```

Result:

``` text
Row 1:
──────
dt_str:          2017-11-05 08:07:47
from_str:        1509869267
from_str_tokyo:  1509836867
from_datetime:   1509869267
from_datetime64: 1509869267
from_date:       1509840000
from_date32:     1509840000
```

:::note
The return type of `toStartOf*`, `toLastDayOf*`, `toMonday`, `timeSlot` functions described below is determined by the configuration parameter [enable_extended_results_for_datetime_functions](../../operations/settings/settings.md#enable-extended-results-for-datetime-functions) which is `0` by default.

Behavior for
* `enable_extended_results_for_datetime_functions = 0`:
  * Functions `toStartOfYear`, `toStartOfISOYear`, `toStartOfQuarter`, `toStartOfMonth`, `toStartOfWeek`, `toLastDayOfWeek`, `toLastDayOfMonth`, `toMonday` return `Date` or `DateTime`.
  * Functions `toStartOfDay`, `toStartOfHour`, `toStartOfFifteenMinutes`, `toStartOfTenMinutes`, `toStartOfFiveMinutes`, `toStartOfMinute`, `timeSlot` return `DateTime`. Though these functions can take values of the extended types `Date32` and `DateTime64` as an argument, passing them a time outside the normal range (year 1970 to 2149 for `Date` / 2106 for `DateTime`) will produce wrong results.
* `enable_extended_results_for_datetime_functions = 1`:
  * Functions `toStartOfYear`, `toStartOfISOYear`, `toStartOfQuarter`, `toStartOfMonth`, `toStartOfWeek`, `toLastDayOfWeek`, `toLastDayOfMonth`, `toMonday` return `Date` or `DateTime` if their argument is a `Date` or `DateTime`, and they return `Date32` or `DateTime64` if their argument is a `Date32` or `DateTime64`.
  * Functions `toStartOfDay`, `toStartOfHour`, `toStartOfFifteenMinutes`, `toStartOfTenMinutes`, `toStartOfFiveMinutes`, `toStartOfMinute`, `timeSlot` return `DateTime` if their argument is a `Date` or `DateTime`, and they return `DateTime64` if their argument is a `Date32` or `DateTime64`.
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

## toLastDayOfMonth

Rounds a date, or date with time, to the last day of the month.
Returns the date.

Alias: `LAST_DAY`.

If `toLastDayOfMonth` is called with an argument of type `Date` greater then 2149-05-31, the result will be calculated from the argument 2149-05-31 instead.

## toMonday

Rounds down a date, or date with time, to the nearest Monday.
Returns the date.

## toStartOfWeek

Rounds a date or date with time down to the nearest Sunday or Monday.
Returns the date.
The mode argument works exactly like the mode argument in function `toWeek()`. If no mode is specified, mode is assumed as 0.

**Syntax**

``` sql
toStartOfWeek(t[, mode[, timezone]])
```

## toLastDayOfWeek

Rounds a date or date with time up to the nearest Saturday or Sunday.
Returns the date.
The mode argument works exactly like the mode argument in function `toWeek()`. If no mode is specified, mode is assumed as 0.

**Syntax**

``` sql
toLastDayOfWeek(t[, mode[, timezone]])
```

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

- `value` — Date and time. [DateTime64](../../sql-reference/data-types/datetime64.md).
- `timezone` — [Timezone](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) for the returned value (optional). If not specified, the function uses the timezone of the `value` parameter. [String](../../sql-reference/data-types/string.md).

**Returned value**

- Input value without sub-seconds.

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

- [Timezone](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) server configuration parameter.

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

Converts a date, or date with time, to the number of the year, starting from a certain fixed point in the past.

## toRelativeQuarterNum

Converts a date, or date with time, to the number of the quarter, starting from a certain fixed point in the past.

## toRelativeMonthNum

Converts a date, or date with time, to the number of the month, starting from a certain fixed point in the past.

## toRelativeWeekNum

Converts a date, or date with time, to the number of the week, starting from a certain fixed point in the past.

## toRelativeDayNum

Converts a date, or date with time, to the number of the day, starting from a certain fixed point in the past.

## toRelativeHourNum

Converts a date, or date with time, to the number of the hour, starting from a certain fixed point in the past.

## toRelativeMinuteNum

Converts a date, or date with time, to the number of the minute, starting from a certain fixed point in the past.

## toRelativeSecondNum

Converts a date, or date with time, to the number of the second, starting from a certain fixed point in the past.

## toISOYear

Converts a date, or date with time, to a UInt16 number containing the ISO Year number.

## toISOWeek

Converts a date, or date with time, to a UInt8 number containing the ISO Week number.

## toWeek

This function returns the week number for date or datetime. The two-argument form of `toWeek()` enables you to specify whether the week starts on Sunday or Monday and whether the return value should be in the range from 0 to 53 or from 1 to 53. If the mode argument is omitted, the default mode is 0.

`toISOWeek()` is a compatibility function that is equivalent to `toWeek(date,3)`.

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

- If the week containing January 1 has 4 or more days in the new year, it is week 1.

- Otherwise, it is the last week of the previous year, and the next week is week 1.

For mode values with a meaning of “contains January 1”, the week contains January 1 is week 1. It does not matter how many days in the new year the week contained, even if it contained only one day.

**Syntax**

``` sql
toWeek(t[, mode[, time_zone]])
```

**Arguments**

- `t` – Date or DateTime.
- `mode` – Optional parameter, Range of values is \[0,9\], default is 0.
- `Timezone` – Optional parameter, it behaves like any other conversion function.

**Example**

``` sql
SELECT toDate('2016-12-27') AS date, toWeek(date) AS week0, toWeek(date,1) AS week1, toWeek(date,9) AS week9;
```

``` text
┌───────date─┬─week0─┬─week1─┬─week9─┐
│ 2016-12-27 │    52 │    52 │     1 │
└────────────┴───────┴───────┴───────┘
```

## toYearWeek

Returns year and week for a date. The year in the result may be different from the year in the date argument for the first and the last week of the year.

The mode argument works like the mode argument to `toWeek()`. For the single-argument syntax, a mode value of 0 is used.

`toISOYear()` is a compatibility function that is equivalent to `intDiv(toYearWeek(date,3),100)`.

:::warning
The week number returned by `toYearWeek()` can be different from what the `toWeek()` returns. `toWeek()` always returns week number in the context of the given year, and in case `toWeek()` returns `0`, `toYearWeek()` returns the value corresponding to the last week of previous year. See `prev_yearWeek` in example below.
:::

**Syntax**

``` sql
toYearWeek(t[, mode[, timezone]])
```

**Example**

``` sql
SELECT toDate('2016-12-27') AS date, toYearWeek(date) AS yearWeek0, toYearWeek(date,1) AS yearWeek1, toYearWeek(date,9) AS yearWeek9, toYearWeek(toDate('2022-01-01')) AS prev_yearWeek;
```

``` text
┌───────date─┬─yearWeek0─┬─yearWeek1─┬─yearWeek9─┬─prev_yearWeek─┐
│ 2016-12-27 │    201652 │    201652 │    201701 │        202152 │
└────────────┴───────────┴───────────┴───────────┴───────────────┘
```

## age

Returns the `unit` component of the difference between `startdate` and `enddate`. The difference is calculated using a precision of 1 microsecond.
E.g. the difference between `2021-12-29` and `2022-01-01` is 3 days for `day` unit, 0 months for `month` unit, 0 years for `year` unit.

For an alternative to `age`, see function `date\_diff`.

**Syntax**

``` sql
age('unit', startdate, enddate, [timezone])
```

**Arguments**

- `unit` — The type of interval for result. [String](../../sql-reference/data-types/string.md).
    Possible values:

    - `microsecond` (possible abbreviations: `us`, `u`)
    - `millisecond` (possible abbreviations: `ms`)
    - `second` (possible abbreviations: `ss`, `s`)
    - `minute` (possible abbreviations: `mi`, `n`)
    - `hour` (possible abbreviations: `hh`, `h`)
    - `day` (possible abbreviations: `dd`, `d`)
    - `week` (possible abbreviations: `wk`, `ww`)
    - `month` (possible abbreviations: `mm`, `m`)
    - `quarter` (possible abbreviations: `qq`, `q`)
    - `year` (possible abbreviations: `yyyy`, `yy`)

- `startdate` — The first time value to subtract (the subtrahend). [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md), [DateTime](../../sql-reference/data-types/datetime.md) or [DateTime64](../../sql-reference/data-types/datetime64.md).

- `enddate` — The second time value to subtract from (the minuend). [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md), [DateTime](../../sql-reference/data-types/datetime.md) or [DateTime64](../../sql-reference/data-types/datetime64.md).

- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) (optional). If specified, it is applied to both `startdate` and `enddate`. If not specified, timezones of `startdate` and `enddate` are used. If they are not the same, the result is unspecified. [String](../../sql-reference/data-types/string.md).

**Returned value**

Difference between `enddate` and `startdate` expressed in `unit`.

Type: [Int](../../sql-reference/data-types/int-uint.md).

**Example**

``` sql
SELECT age('hour', toDateTime('2018-01-01 22:30:00'), toDateTime('2018-01-02 23:00:00'));
```

Result:

``` text
┌─age('hour', toDateTime('2018-01-01 22:30:00'), toDateTime('2018-01-02 23:00:00'))─┐
│                                                                                24 │
└───────────────────────────────────────────────────────────────────────────────────┘
```

``` sql
SELECT
    toDate('2022-01-01') AS e,
    toDate('2021-12-29') AS s,
    age('day', s, e) AS day_age,
    age('month', s, e) AS month__age,
    age('year', s, e) AS year_age;
```

Result:

``` text
┌──────────e─┬──────────s─┬─day_age─┬─month__age─┬─year_age─┐
│ 2022-01-01 │ 2021-12-29 │       3 │          0 │        0 │
└────────────┴────────────┴─────────┴────────────┴──────────┘
```


## date\_diff

Returns the count of the specified `unit` boundaries crossed between the `startdate` and the `enddate`.
The difference is calculated using relative units, e.g. the difference between `2021-12-29` and `2022-01-01` is 3 days for unit `day` (see [toRelativeDayNum](#torelativedaynum)), 1 month for unit `month` (see [toRelativeMonthNum](#torelativemonthnum)) and 1 year for unit `year` (see [toRelativeYearNum](#torelativeyearnum)).

If unit `week` was specified, `date\_diff` assumes that weeks start on Monday. Note that this behavior is different from that of function `toWeek()` in which weeks start by default on Sunday.

For an alternative to `date\_diff`, see function `age`.

**Syntax**

``` sql
date_diff('unit', startdate, enddate, [timezone])
```

Aliases: `dateDiff`, `DATE_DIFF`, `timestampDiff`, `timestamp_diff`, `TIMESTAMP_DIFF`.

**Arguments**

- `unit` — The type of interval for result. [String](../../sql-reference/data-types/string.md).
    Possible values:

    - `microsecond` (possible abbreviations: `us`, `u`)
    - `millisecond` (possible abbreviations: `ms`)
    - `second` (possible abbreviations: `ss`, `s`)
    - `minute` (possible abbreviations: `mi`, `n`)
    - `hour` (possible abbreviations: `hh`, `h`)
    - `day` (possible abbreviations: `dd`, `d`)
    - `week` (possible abbreviations: `wk`, `ww`)
    - `month` (possible abbreviations: `mm`, `m`)
    - `quarter` (possible abbreviations: `qq`, `q`)
    - `year` (possible abbreviations: `yyyy`, `yy`)

- `startdate` — The first time value to subtract (the subtrahend). [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md), [DateTime](../../sql-reference/data-types/datetime.md) or [DateTime64](../../sql-reference/data-types/datetime64.md).

- `enddate` — The second time value to subtract from (the minuend). [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md), [DateTime](../../sql-reference/data-types/datetime.md) or [DateTime64](../../sql-reference/data-types/datetime64.md).

- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) (optional). If specified, it is applied to both `startdate` and `enddate`. If not specified, timezones of `startdate` and `enddate` are used. If they are not the same, the result is unspecified. [String](../../sql-reference/data-types/string.md).

**Returned value**

Difference between `enddate` and `startdate` expressed in `unit`.

Type: [Int](../../sql-reference/data-types/int-uint.md).

**Example**

``` sql
SELECT dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'));
```

Result:

``` text
┌─dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'))─┐
│                                                                                     25 │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

``` sql
SELECT
    toDate('2022-01-01') AS e,
    toDate('2021-12-29') AS s,
    dateDiff('day', s, e) AS day_diff,
    dateDiff('month', s, e) AS month__diff,
    dateDiff('year', s, e) AS year_diff;
```

Result:

``` text
┌──────────e─┬──────────s─┬─day_diff─┬─month__diff─┬─year_diff─┐
│ 2022-01-01 │ 2021-12-29 │        3 │           1 │         1 │
└────────────┴────────────┴──────────┴─────────────┴───────────┘
```

## date\_trunc

Truncates date and time data to the specified part of date.

**Syntax**

``` sql
date_trunc(unit, value[, timezone])
```

Alias: `dateTrunc`.

**Arguments**

- `unit` — The type of interval to truncate the result. [String Literal](../syntax.md#syntax-string-literal).
    Possible values:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

- `value` — Date and time. [DateTime](../../sql-reference/data-types/datetime.md) or [DateTime64](../../sql-reference/data-types/datetime64.md).
- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) for the returned value (optional). If not specified, the function uses the timezone of the `value` parameter. [String](../../sql-reference/data-types/string.md).

**Returned value**

- Value, truncated to the specified part of date.

Type: [DateTime](../../sql-reference/data-types/datetime.md).

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

- [toStartOfInterval](#tostartofintervaltime-or-data-interval-x-unit-time-zone)

## date\_add

Adds the time interval or date interval to the provided date or date with time.

**Syntax**

``` sql
date_add(unit, value, date)
```

Aliases: `dateAdd`, `DATE_ADD`.

**Arguments**

- `unit` — The type of interval to add. [String](../../sql-reference/data-types/string.md).
    Possible values:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

- `value` — Value of interval to add. [Int](../../sql-reference/data-types/int-uint.md).
- `date` — The date or date with time to which `value` is added. [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Returned value**

Date or date with time obtained by adding `value`, expressed in `unit`, to `date`.

Type: [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Example**

```sql
SELECT date_add(YEAR, 3, toDate('2018-01-01'));
```

Result:

```text
┌─plus(toDate('2018-01-01'), toIntervalYear(3))─┐
│                                    2021-01-01 │
└───────────────────────────────────────────────┘
```

## date\_sub

Subtracts the time interval or date interval from the provided date or date with time.

**Syntax**

``` sql
date_sub(unit, value, date)
```

Aliases: `dateSub`, `DATE_SUB`.

**Arguments**

- `unit` — The type of interval to subtract. Note: The unit should be unquoted.

    Possible values:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

- `value` — Value of interval to subtract. [Int](../../sql-reference/data-types/int-uint.md).
- `date` — The date or date with time from which `value` is subtracted. [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Returned value**

Date or date with time obtained by subtracting `value`, expressed in `unit`, from `date`.

Type: [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Example**

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

- `date` — Date or date with time. [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).
- `value` — Value of interval to add. [Int](../../sql-reference/data-types/int-uint.md).
- `unit` — The type of interval to add. [String](../../sql-reference/data-types/string.md).
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

- `unit` — The type of interval to subtract. [String](../../sql-reference/data-types/string.md).
    Possible values:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

- `value` — Value of interval to subtract. [Int](../../sql-reference/data-types/int-uint.md).
- `date` — Date or date with time. [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Returned value**

Date or date with time obtained by subtracting `value`, expressed in `unit`, from `date`.

Type: [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Example**

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

Alias: `current_timestamp`.

**Syntax**

``` sql
now([timezone])
```

**Arguments**

- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) for the returned value (optional). [String](../../sql-reference/data-types/string.md).

**Returned value**

- Current date and time.

Type: [DateTime](../../sql-reference/data-types/datetime.md).

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

## now64

Returns the current date and time with sub-second precision at the moment of query analysis. The function is a constant expression.

**Syntax**

``` sql
now64([scale], [timezone])
```

**Arguments**

- `scale` - Tick size (precision): 10<sup>-precision</sup> seconds. Valid range: [ 0 : 9 ]. Typically are used - 3 (default) (milliseconds), 6 (microseconds), 9 (nanoseconds).
- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) for the returned value (optional). [String](../../sql-reference/data-types/string.md).

**Returned value**

- Current date and time with sub-second precision.

Type: [DateTime64](../../sql-reference/data-types/datetime64.md).

**Example**

``` sql
SELECT now64(), now64(9, 'Asia/Istanbul');
```

Result:

``` text
┌─────────────────now64()─┬─────now64(9, 'Asia/Istanbul')─┐
│ 2022-08-21 19:34:26.196 │ 2022-08-21 22:34:26.196542766 │
└─────────────────────────┴───────────────────────────────┘
```

## nowInBlock

Returns the current date and time at the moment of processing of each block of data. In contrast to the function [now](#now), it is not a constant expression, and the returned value will be different in different blocks for long-running queries.

It makes sense to use this function to generate the current time in long-running INSERT SELECT queries.

**Syntax**

``` sql
nowInBlock([timezone])
```

**Arguments**

- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) for the returned value (optional). [String](../../sql-reference/data-types/string.md).

**Returned value**

- Current date and time at the moment of processing of each block of data.

Type: [DateTime](../../sql-reference/data-types/datetime.md).

**Example**

``` sql
SELECT
    now(),
    nowInBlock(),
    sleep(1)
FROM numbers(3)
SETTINGS max_block_size = 1
FORMAT PrettyCompactMonoBlock
```

Result:

``` text
┌───────────────now()─┬────────nowInBlock()─┬─sleep(1)─┐
│ 2022-08-21 19:41:19 │ 2022-08-21 19:41:19 │        0 │
│ 2022-08-21 19:41:19 │ 2022-08-21 19:41:20 │        0 │
│ 2022-08-21 19:41:19 │ 2022-08-21 19:41:21 │        0 │
└─────────────────────┴─────────────────────┴──────────┘
```

## today

Accepts zero arguments and returns the current date at one of the moments of query analysis.
The same as ‘toDate(now())’.

Aliases: `curdate`, `current_date`.

## yesterday

Accepts zero arguments and returns yesterday’s date at one of the moments of query analysis.
The same as ‘today() - 1’.

## timeSlot

Rounds the time to the half hour.

## toYYYYMM

Converts a date or date with time to a UInt32 number containing the year and month number (YYYY \* 100 + MM). Accepts a second optional timezone argument. If provided, the timezone must be a string constant.

**Example**

``` sql
SELECT
    toYYYYMM(now(), 'US/Eastern')
```

Result:

``` text
┌─toYYYYMM(now(), 'US/Eastern')─┐
│                        202303 │
└───────────────────────────────┘
```

## toYYYYMMDD

Converts a date or date with time to a UInt32 number containing the year and month number (YYYY \* 10000 + MM \* 100 + DD). Accepts a second optional timezone argument. If provided, the timezone must be a string constant.

**Example**

```sql
SELECT
    toYYYYMMDD(now(), 'US/Eastern')
```

Result:

```response
┌─toYYYYMMDD(now(), 'US/Eastern')─┐
│                        20230302 │
└─────────────────────────────────┘
```

## toYYYYMMDDhhmmss

Converts a date or date with time to a UInt64 number containing the year and month number (YYYY \* 10000000000 + MM \* 100000000 + DD \* 1000000 + hh \* 10000 + mm \* 100 + ss). Accepts a second optional timezone argument. If provided, the timezone must be a string constant.

**Example**

```sql
SELECT
    toYYYYMMDDhhmmss(now(), 'US/Eastern')
```

Result:

```response
┌─toYYYYMMDDhhmmss(now(), 'US/Eastern')─┐
│                        20230302112209 │
└───────────────────────────────────────┘
```

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

For a time interval starting at ‘StartTime’ and continuing for ‘Duration’ seconds, it returns an array of moments in time, consisting of points from this interval rounded down to the ‘Size’ in seconds. ‘Size’ is an optional parameter set to 1800 (30 minutes) by default.
This is necessary, for example, when searching for pageviews in the corresponding session.
Accepts DateTime and DateTime64 as ’StartTime’ argument. For DateTime, ’Duration’ and ’Size’ arguments must be `UInt32`. For ’DateTime64’ they must be `Decimal64`.
Returns an array of DateTime/DateTime64 (return type matches the type of ’StartTime’). For DateTime64, the return value's scale can differ from the scale of ’StartTime’ --- the highest scale among all given arguments is taken.

Example:
```sql
SELECT timeSlots(toDateTime('2012-01-01 12:20:00'), toUInt32(600));
SELECT timeSlots(toDateTime('1980-12-12 21:01:02', 'UTC'), toUInt32(600), 299);
SELECT timeSlots(toDateTime64('1980-12-12 21:01:02.1234', 4, 'UTC'), toDecimal64(600.1, 1), toDecimal64(299, 0));
```
``` text
┌─timeSlots(toDateTime('2012-01-01 12:20:00'), toUInt32(600))─┐
│ ['2012-01-01 12:00:00','2012-01-01 12:30:00']               │
└─────────────────────────────────────────────────────────────┘
┌─timeSlots(toDateTime('1980-12-12 21:01:02', 'UTC'), toUInt32(600), 299)─┐
│ ['1980-12-12 20:56:13','1980-12-12 21:01:12','1980-12-12 21:06:11']     │
└─────────────────────────────────────────────────────────────────────────┘
┌─timeSlots(toDateTime64('1980-12-12 21:01:02.1234', 4, 'UTC'), toDecimal64(600.1, 1), toDecimal64(299, 0))─┐
│ ['1980-12-12 20:56:13.0000','1980-12-12 21:01:12.0000','1980-12-12 21:06:11.0000']                        │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## formatDateTime {#date_time_functions-formatDateTime}

Formats a Time according to the given Format string. Format is a constant expression, so you cannot have multiple formats for a single result column.

formatDateTime uses MySQL datetime format style, refer to https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format.

The opposite operation of this function is [parseDateTime](/docs/en/sql-reference/functions/type-conversion-functions.md#type_conversion_functions-parseDateTime).

Alias: `DATE_FORMAT`.

**Syntax**

``` sql
formatDateTime(Time, Format[, Timezone])
```

**Returned value(s)**

Returns time and date values according to the determined format.

**Replacement fields**
Using replacement fields, you can define a pattern for the resulting string. “Example” column shows formatting result for `2018-01-02 22:33:44`.

| Placeholder | Description                                             | Example    |
|----------|---------------------------------------------------------|------------|
| %a       | abbreviated weekday name (Mon-Sun)                      | Mon        |
| %b       | abbreviated month name (Jan-Dec)                        | Jan        |
| %c       | month as an integer number (01-12)                      | 01         |
| %C       | year divided by 100 and truncated to integer (00-99)    | 20         |
| %d       | day of the month, zero-padded (01-31)                   | 02         |
| %D       | Short MM/DD/YY date, equivalent to %m/%d/%y             | 01/02/18   |
| %e       | day of the month, space-padded (1-31)                   | &nbsp; 2   |
| %f       | fractional second, see 'Note 1' below                   | 1234560    |
| %F       | short YYYY-MM-DD date, equivalent to %Y-%m-%d           | 2018-01-02 |
| %g       | two-digit year format, aligned to ISO 8601, abbreviated from four-digit notation                                | 18       |
| %G       | four-digit year format for ISO week number, calculated from the week-based year [defined by the ISO 8601](https://en.wikipedia.org/wiki/ISO_8601#Week_dates) standard, normally useful only with %V  | 2018         |
| %h       | hour in 12h format (01-12)                              | 09         |
| %H       | hour in 24h format (00-23)                              | 22         |
| %i       | minute (00-59)                                          | 33         |
| %I       | hour in 12h format (01-12)                              | 10         |
| %j       | day of the year (001-366)                               | 002        |
| %k       | hour in 24h format (00-23)                              | 22         |
| %l       | hour in 12h format (01-12)                              | 09         |
| %m       | month as an integer number (01-12)                      | 01         |
| %M       | full month name (January-December), see 'Note 2' below  | January    |
| %n       | new-line character (‘’)                                 |            |
| %p       | AM or PM designation                                    | PM         |
| %Q       | Quarter (1-4)                                           | 1          |
| %r       | 12-hour HH:MM AM/PM time, equivalent to %h:%i %p        | 10:30 PM   |
| %R       | 24-hour HH:MM time, equivalent to %H:%i                 | 22:33      |
| %s       | second (00-59)                                          | 44         |
| %S       | second (00-59)                                          | 44         |
| %t       | horizontal-tab character (’)                            |            |
| %T       | ISO 8601 time format (HH:MM:SS), equivalent to %H:%i:%S | 22:33:44   |
| %u       | ISO 8601 weekday as number with Monday as 1 (1-7)       | 2          |
| %V       | ISO 8601 week number (01-53)                            | 01         |
| %w       | weekday as a integer number with Sunday as 0 (0-6)      | 2          |
| %W       | full weekday name (Monday-Sunday)                       | Monday     |
| %y       | Year, last two digits (00-99)                           | 18         |
| %Y       | Year                                                    | 2018       |
| %z       | Time offset from UTC as +HHMM or -HHMM                  | -0500      |
| %%       | a % sign                                                | %          |

Note 1: In ClickHouse versions earlier than v23.4, `%f` prints a single zero (0) if the formatted value is a Date, Date32 or DateTime (which have no fractional seconds) or a DateTime64 with a precision of 0. The previous behavior can be restored using setting `formatdatetime_f_prints_single_zero = 1`.

Note 2: In ClickHouse versions earlier than v23.4, `%M` prints the minute (00-59) instead of the full month name (January-December). The previous behavior can be restored using setting `formatdatetime_parsedatetime_m_is_month_name = 0`.

**Example**

``` sql
SELECT formatDateTime(toDate('2010-01-04'), '%g')
```

Result:

```
┌─formatDateTime(toDate('2010-01-04'), '%g')─┐
│ 10                                         │
└────────────────────────────────────────────┘
```

``` sql
SELECT formatDateTime(toDateTime64('2010-01-04 12:34:56.123456', 7), '%f')
```

Result:

```
┌─formatDateTime(toDateTime64('2010-01-04 12:34:56.123456', 7), '%f')─┐
│ 1234560                                                             │
└─────────────────────────────────────────────────────────────────────┘
```

**See Also**

- [formatDateTimeInJodaSyntax](##formatDateTimeInJodaSyntax)


## formatDateTimeInJodaSyntax {#date_time_functions-formatDateTimeInJodaSyntax}

Similar to formatDateTime, except that it formats datetime in Joda style instead of MySQL style. Refer to https://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html.

The opposite operation of this function is [parseDateTimeInJodaSyntax](/docs/en/sql-reference/functions/type-conversion-functions.md#type_conversion_functions-parseDateTimeInJodaSyntax).

**Replacement fields**

Using replacement fields, you can define a pattern for the resulting string.


| Placeholder | Description                              | Presentation  | Examples                           |
| ----------- | ---------------------------------------- | ------------- | ---------------------------------- |
| G           | era                                      | text          | AD                                 |
| C           | century of era (>=0)                     | number        | 20                                 |
| Y           | year of era (>=0)                        | year          | 1996                               |
| x           | weekyear (not supported yet)             | year          | 1996                               |
| w           | week of weekyear (not supported yet)     | number        | 27                                 |
| e           | day of week                              | number        | 2                                  |
| E           | day of week                              | text          | Tuesday; Tue                       |
| y           | year                                     | year          | 1996                               |
| D           | day of year                              | number        | 189                                |
| M           | month of year                            | month         | July; Jul; 07                      |
| d           | day of month                             | number        | 10                                 |
| a           | halfday of day                           | text          | PM                                 |
| K           | hour of halfday (0~11)                   | number        | 0                                  |
| h           | clockhour of halfday (1~12)              | number        | 12                                 |
| H           | hour of day (0~23)                       | number        | 0                                  |
| k           | clockhour of day (1~24)                  | number        | 24                                 |
| m           | minute of hour                           | number        | 30                                 |
| s           | second of minute                         | number        | 55                                 |
| S           | fraction of second (not supported yet)   | number        | 978                                |
| z           | time zone (short name not supported yet) | text          | Pacific Standard Time; PST         |
| Z           | time zone offset/id (not supported yet)  | zone          | -0800; -08:00; America/Los_Angeles |
| '           | escape for text                          | delimiter     |                                    |
| ''          | single quote                             | literal       | '                                  |

**Example**

``` sql
SELECT formatDateTimeInJodaSyntax(toDateTime('2010-01-04 12:34:56'), 'yyyy-MM-dd HH:mm:ss')
```

Result:

```
┌─formatDateTimeInJodaSyntax(toDateTime('2010-01-04 12:34:56'), 'yyyy-MM-dd HH:mm:ss')─┐
│ 2010-01-04 12:34:56                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```


## dateName

Returns specified part of date.

**Syntax**

``` sql
dateName(date_part, date)
```

**Arguments**

- `date_part` — Date part. Possible values: 'year', 'quarter', 'month', 'week', 'dayofyear', 'day', 'weekday', 'hour', 'minute', 'second'. [String](../../sql-reference/data-types/string.md).
- `date` — Date. [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md), [DateTime](../../sql-reference/data-types/datetime.md) or [DateTime64](../../sql-reference/data-types/datetime64.md).
- `timezone` — Timezone. Optional. [String](../../sql-reference/data-types/string.md).

**Returned value**

- The specified part of date.

Type: [String](../../sql-reference/data-types/string.md#string)

**Example**

```sql
WITH toDateTime('2021-04-14 11:22:33') AS date_value
SELECT
    dateName('year', date_value),
    dateName('month', date_value),
    dateName('day', date_value);
```

Result:

```text
┌─dateName('year', date_value)─┬─dateName('month', date_value)─┬─dateName('day', date_value)─┐
│ 2021                         │ April                         │ 14                          │
└──────────────────────────────┴───────────────────────────────┴─────────────────────────────┘
```

## monthName

Returns name of the month.

**Syntax**

``` sql
monthName(date)
```

**Arguments**

- `date` — Date or date with time. [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Returned value**

- The name of the month.

Type: [String](../../sql-reference/data-types/string.md#string)

**Example**

```sql
WITH toDateTime('2021-04-14 11:22:33') AS date_value
SELECT monthName(date_value);
```

Result:

```text
┌─monthName(date_value)─┐
│ April                 │
└───────────────────────┘
```

## fromUnixTimestamp

Function converts Unix timestamp to a calendar date and a time of a day. When there is only a single argument of [Integer](../../sql-reference/data-types/int-uint.md) type, it acts in the same way as [toDateTime](../../sql-reference/functions/type-conversion-functions.md#todatetime) and return [DateTime](../../sql-reference/data-types/datetime.md) type.

fromUnixTimestamp uses MySQL datetime format style, refer to https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format.

Alias: `FROM_UNIXTIME`.

**Example:**

```sql
SELECT fromUnixTimestamp(423543535);
```

Result:

```text
┌─fromUnixTimestamp(423543535)─┐
│          1983-06-04 10:58:55 │
└──────────────────────────────┘
```

When there are two or three arguments, the first an [Integer](../../sql-reference/data-types/int-uint.md), [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md), [DateTime](../../sql-reference/data-types/datetime.md) or [DateTime64](../../sql-reference/data-types/datetime64.md), the second a constant format string and the third an optional constant time zone string — it acts in the same way as [formatDateTime](#formatdatetime) and return [String](../../sql-reference/data-types/string.md#string) type.

For example:

```sql
SELECT fromUnixTimestamp(1234334543, '%Y-%m-%d %R:%S') AS DateTime;
```

```text
┌─DateTime────────────┐
│ 2009-02-11 14:42:23 │
└─────────────────────┘
```

**See Also**

- [fromUnixTimestampInJodaSyntax](##fromUnixTimestampInJodaSyntax)

## fromUnixTimestampInJodaSyntax

Similar to fromUnixTimestamp, except that it formats time in Joda style instead of MySQL style. Refer to https://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html.

**Example:**

``` sql
SELECT fromUnixTimestampInJodaSyntax(1669804872, 'yyyy-MM-dd HH:mm:ss', 'UTC');
```

Result:
```
┌─fromUnixTimestampInJodaSyntax(1669804872, 'yyyy-MM-dd HH:mm:ss', 'UTC')────┐
│ 2022-11-30 10:41:12                                                        │
└────────────────────────────────────────────────────────────────────────────┘
```

## toModifiedJulianDay

Converts a [Proleptic Gregorian calendar](https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar) date in text form `YYYY-MM-DD` to a [Modified Julian Day](https://en.wikipedia.org/wiki/Julian_day#Variants) number in Int32. This function supports date from `0000-01-01` to `9999-12-31`. It raises an exception if the argument cannot be parsed as a date, or the date is invalid.

**Syntax**

``` sql
toModifiedJulianDay(date)
```

**Arguments**

- `date` — Date in text form. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).

**Returned value**

- Modified Julian Day number.

Type: [Int32](../../sql-reference/data-types/int-uint.md).

**Example**

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

- `date` — Date in text form. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).

**Returned value**

- Modified Julian Day number.

Type: [Nullable(Int32)](../../sql-reference/data-types/int-uint.md).

**Example**

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

- `day` — Modified Julian Day number. [Any integral types](../../sql-reference/data-types/int-uint.md).

**Returned value**

- Date in text form.

Type: [String](../../sql-reference/data-types/string.md)

**Example**

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

- `day` — Modified Julian Day number. [Any integral types](../../sql-reference/data-types/int-uint.md).

**Returned value**

- Date in text form.

Type: [Nullable(String)](../../sql-reference/data-types/string.md)

**Example**

``` sql
SELECT fromModifiedJulianDayOrNull(58849);
```

Result:

``` text
┌─fromModifiedJulianDayOrNull(58849)─┐
│ 2020-01-01                         │
└────────────────────────────────────┘
```

## toUTCTimestamp

Convert DateTime/DateTime64 type value from other time zone to UTC timezone timestamp

**Syntax**

``` sql
toUTCTimestamp(time_val, time_zone)
```

**Arguments**

- `time_val` — A DateTime/DateTime64 type const value or a expression . [DateTime/DateTime64 types](../../sql-reference/data-types/datetime.md)
- `time_zone` — A String type const value or a expression represent the time zone. [String types](../../sql-reference/data-types/string.md)

**Returned value**

- DateTime/DateTime64 in text form

**Example**

``` sql
SELECT toUTCTimestamp(toDateTime('2023-03-16'), 'Asia/Shanghai');
```

Result:

``` text
┌─toUTCTimestamp(toDateTime('2023-03-16'),'Asia/Shanghai')┐
│                                     2023-03-15 16:00:00 │
└─────────────────────────────────────────────────────────┘
```

## fromUTCTimestamp

Convert DateTime/DateTime64 type value from UTC timezone to other time zone timestamp

**Syntax**

``` sql
fromUTCTimestamp(time_val, time_zone)
```

**Arguments**

- `time_val` — A DateTime/DateTime64 type const value or a expression . [DateTime/DateTime64 types](../../sql-reference/data-types/datetime.md)
- `time_zone` — A String type const value or a expression represent the time zone. [String types](../../sql-reference/data-types/string.md)

**Returned value**

- DateTime/DateTime64 in text form

**Example**

``` sql
SELECT fromUTCTimestamp(toDateTime64('2023-03-16 10:00:00', 3), 'Asia/Shanghai');
```

Result:

``` text
┌─fromUTCTimestamp(toDateTime64('2023-03-16 10:00:00',3),'Asia/Shanghai')─┐
│                                                 2023-03-16 18:00:00.000 │
└─────────────────────────────────────────────────────────────────────────┘
```

## Related content

- Blog: [Working with time series data in ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)

