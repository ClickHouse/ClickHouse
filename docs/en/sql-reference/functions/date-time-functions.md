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

Creates a [Date](../data-types/date.md)
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

- `year` — Year. [Integer](../data-types/int-uint.md), [Float](../data-types/float.md) or [Decimal](../data-types/decimal.md).
- `month` — Month. [Integer](../data-types/int-uint.md), [Float](../data-types/float.md) or [Decimal](../data-types/decimal.md).
- `day` — Day. [Integer](../data-types/int-uint.md), [Float](../data-types/float.md) or [Decimal](../data-types/decimal.md).
- `day_of_year` — Day of the year. [Integer](../data-types/int-uint.md), [Float](../data-types/float.md) or [Decimal](../data-types/decimal.md).

**Returned value**

- A date created from the arguments. [Date](../data-types/date.md).

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

Creates a date of type [Date32](../../sql-reference/data-types/date32.md) from a year, month, day (or optionally a year and a day).

**Syntax**

```sql
makeDate32(year, [month,] day)
```

**Arguments**

- `year` — Year. [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).
- `month` — Month (optional). [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).
- `day` — Day. [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).

:::note
If `month` is omitted then `day` should take a value between `1` and `365`, otherwise it should take a value between `1` and `31`.
:::

**Returned values**

- A date created from the arguments. [Date32](../../sql-reference/data-types/date32.md).

**Examples**

Create a date from a year, month, and day:

Query:

```sql
SELECT makeDate32(2024, 1, 1);
```

Result:

```response
2024-01-01
```

Create a Date from a year and day of year:

Query:

``` sql
SELECT makeDate32(2024, 100);
```

Result:

```response
2024-04-09
```

## makeDateTime

Creates a [DateTime](../data-types/datetime.md) from a year, month, day, hour, minute and second argument.

**Syntax**

``` sql
makeDateTime(year, month, day, hour, minute, second[, timezone])
```

**Arguments**

- `year` — Year. [Integer](../data-types/int-uint.md), [Float](../data-types/float.md) or [Decimal](../data-types/decimal.md).
- `month` — Month. [Integer](../data-types/int-uint.md), [Float](../data-types/float.md) or [Decimal](../data-types/decimal.md).
- `day` — Day. [Integer](../data-types/int-uint.md), [Float](../data-types/float.md) or [Decimal](../data-types/decimal.md).
- `hour` — Hour. [Integer](../data-types/int-uint.md), [Float](../data-types/float.md) or [Decimal](../data-types/decimal.md).
- `minute` — Minute. [Integer](../data-types/int-uint.md), [Float](../data-types/float.md) or [Decimal](../data-types/decimal.md).
- `second` — Second. [Integer](../data-types/int-uint.md), [Float](../data-types/float.md) or [Decimal](../data-types/decimal.md).
- `timezone` — [Timezone](../../operations/server-configuration-parameters/settings.md#timezone) for the returned value (optional).

**Returned value**

- A date with time created from the arguments. [DateTime](../data-types/datetime.md).

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

Creates a [DateTime64](../../sql-reference/data-types/datetime64.md) data type value from its components: year, month, day, hour, minute, second. With optional sub-second precision.

**Syntax**

```sql
makeDateTime64(year, month, day, hour, minute, second[, precision])
```

**Arguments**

- `year` — Year (0-9999). [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).
- `month` — Month (1-12). [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).
- `day` — Day (1-31). [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).
- `hour` — Hour (0-23). [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).
- `minute` — Minute (0-59). [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).
- `second` — Second (0-59). [Integer](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md) or [Decimal](../../sql-reference/data-types/decimal.md).
- `precision` — Optional precision of the sub-second component (0-9). [Integer](../../sql-reference/data-types/int-uint.md).

**Returned value**

- A date and time created from the supplied arguments. [DateTime64](../../sql-reference/data-types/datetime64.md).  

**Example**

``` sql
SELECT makeDateTime64(2023, 5, 15, 10, 30, 45, 779, 5);
```

```response
┌─makeDateTime64(2023, 5, 15, 10, 30, 45, 779, 5)─┐
│                       2023-05-15 10:30:45.00779 │
└─────────────────────────────────────────────────┘
```

## timestamp

Converts the first argument 'expr' to type [DateTime64(6)](../data-types/datetime64.md).
If a second argument 'expr_time' is provided, it adds the specified time to the converted value.

**Syntax**

``` sql
timestamp(expr[, expr_time])
```

Alias: `TIMESTAMP`

**Arguments**

- `expr` - Date or date with time. [String](../data-types/string.md).
- `expr_time` - Optional parameter. Time to add. [String](../data-types/string.md).

**Examples**

``` sql
SELECT timestamp('2023-12-31') as ts;
```

Result:

``` text
┌─────────────────────────ts─┐
│ 2023-12-31 00:00:00.000000 │
└────────────────────────────┘
```

``` sql
SELECT timestamp('2023-12-31 12:00:00', '12:00:00.11') as ts;
```

Result:

``` text
┌─────────────────────────ts─┐
│ 2024-01-01 00:00:00.110000 │
└────────────────────────────┘
```

**Returned value**

- [DateTime64](../data-types/datetime64.md)(6)

## timeZone

Returns the timezone of the current session, i.e. the value of setting [session_timezone](../../operations/settings/settings.md#session_timezone).
If the function is executed in the context of a distributed table, then it generates a normal column with values relevant to each shard, otherwise it produces a constant value.

**Syntax**

```sql
timeZone()
```

Alias: `timezone`.

**Returned value**

- Timezone. [String](../data-types/string.md).

**Example**

```sql
SELECT timezone()
```

Result:

```response
┌─timezone()─────┐
│ America/Denver │
└────────────────┘
```

**See also**

- [serverTimeZone](#servertimezone)

## serverTimeZone

Returns the timezone of the server, i.e. the value of setting [timezone](../../operations/server-configuration-parameters/settings.md#timezone).
If the function is executed in the context of a distributed table, then it generates a normal column with values relevant to each shard. Otherwise, it produces a constant value.

**Syntax**

``` sql
serverTimeZone()
```

Alias: `serverTimezone`.

**Returned value**

-   Timezone. [String](../data-types/string.md).

**Example**

```sql
SELECT serverTimeZone()
```

Result:

```response
┌─serverTimeZone()─┐
│ UTC              │
└──────────────────┘
```

**See also**

- [timeZone](#timezone)

## toTimeZone

Converts a date or date with time to the specified time zone. Does not change the internal value (number of unix seconds) of the data, only the value's time zone attribute and the value's string representation changes.

**Syntax**

``` sql
toTimezone(value, timezone)
```

Alias: `toTimezone`.

**Arguments**

- `value` — Time or date and time. [DateTime64](../data-types/datetime64.md).
- `timezone` — Timezone for the returned value. [String](../data-types/string.md). This argument is a constant, because `toTimezone` changes the timezone of a column (timezone is an attribute of `DateTime*` types).

**Returned value**

- Date and time. [DateTime](../data-types/datetime.md).

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

**See Also**

- [formatDateTime](#formatdatetime) - supports non-constant timezone.
- [toString](type-conversion-functions.md#tostring) - supports non-constant timezone.

## timeZoneOf

Returns the timezone name of [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md) data types.

**Syntax**

``` sql
timeZoneOf(value)
```

Alias: `timezoneOf`.

**Arguments**

- `value` — Date and time. [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).

**Returned value**

- Timezone name. [String](../data-types/string.md).

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

- `value` — Date and time. [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).

**Returned value**

- Offset from UTC in seconds. [Int32](../data-types/int-uint.md).

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

Returns the year component (AD) of a date or date with time.

**Syntax**

```sql
toYear(value)
```

Alias: `YEAR`

**Arguments**

- `value` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The year of the given date/time. [UInt16](../data-types/int-uint.md).

**Example**

```sql
SELECT toYear(toDateTime('2023-04-21 10:20:30'))
```

Result:

```response
┌─toYear(toDateTime('2023-04-21 10:20:30'))─┐
│                                      2023 │
└───────────────────────────────────────────┘
```

## toQuarter

Returns the quarter (1-4) of a date or date with time.

**Syntax**

```sql
toQuarter(value)
```

Alias: `QUARTER`

**Arguments**

- `value` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The quarter of the year (1, 2, 3 or 4) of the given date/time. [UInt8](../data-types/int-uint.md).

**Example**

```sql
SELECT toQuarter(toDateTime('2023-04-21 10:20:30'))
```

Result:

```response
┌─toQuarter(toDateTime('2023-04-21 10:20:30'))─┐
│                                            2 │
└──────────────────────────────────────────────┘
```

## toMonth

Returns the month component (1-12) of a date or date with time.

**Syntax**

```sql
toMonth(value)
```

Alias: `MONTH`

**Arguments**

- `value` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The month of the year (1 - 12) of the given date/time. [UInt8](../data-types/int-uint.md).

**Example**

```sql
SELECT toMonth(toDateTime('2023-04-21 10:20:30'))
```

Result:

```response
┌─toMonth(toDateTime('2023-04-21 10:20:30'))─┐
│                                          4 │
└────────────────────────────────────────────┘
```

## toDayOfYear

Returns the number of the day within the year (1-366) of a date or date with time.

**Syntax**

```sql
toDayOfYear(value)
```

Alias: `DAYOFYEAR`

**Arguments**

- `value` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The day of the year (1 - 366) of the given date/time. [UInt16](../data-types/int-uint.md).

**Example**

```sql
SELECT toDayOfYear(toDateTime('2023-04-21 10:20:30'))
```

Result:

```response
┌─toDayOfYear(toDateTime('2023-04-21 10:20:30'))─┐
│                                            111 │
└────────────────────────────────────────────────┘
```

## toDayOfMonth

Returns the number of the day within the month (1-31) of a date or date with time.

**Syntax**

```sql
toDayOfMonth(value)
```

Aliases: `DAYOFMONTH`, `DAY`

**Arguments**

- `value` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The day of the month (1 - 31) of the given date/time. [UInt8](../data-types/int-uint.md).

**Example**

```sql
SELECT toDayOfMonth(toDateTime('2023-04-21 10:20:30'))
```

Result:

```response
┌─toDayOfMonth(toDateTime('2023-04-21 10:20:30'))─┐
│                                              21 │
└─────────────────────────────────────────────────┘
```

## toDayOfWeek

Returns the number of the day within the week of a date or date with time.

The two-argument form of `toDayOfWeek()` enables you to specify whether the week starts on Monday or Sunday, and whether the return value should be in the range from 0 to 6 or 1 to 7. If the mode argument is omitted, the default mode is 0. The time zone of the date can be specified as the third argument.

| Mode | First day of week | Range                                          |
|------|-------------------|------------------------------------------------|
| 0    | Monday            | 1-7: Monday = 1, Tuesday = 2, ..., Sunday = 7  |
| 1    | Monday            | 0-6: Monday = 0, Tuesday = 1, ..., Sunday = 6  |
| 2    | Sunday            | 0-6: Sunday = 0, Monday = 1, ..., Saturday = 6 |
| 3    | Sunday            | 1-7: Sunday = 1, Monday = 2, ..., Saturday = 7 |

**Syntax**

``` sql
toDayOfWeek(t[, mode[, timezone]])
```

Alias: `DAYOFWEEK`.

**Arguments**

- `t` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)
- `mode` - determines what the first day of the week is. Possible values are 0, 1, 2 or 3. See the table above for the differences.
- `timezone` - optional parameter, it behaves like any other conversion function

The first argument can also be specified as [String](../data-types/string.md) in a format supported by [parseDateTime64BestEffort()](type-conversion-functions.md#parsedatetime64besteffort). Support for string arguments exists only for reasons of compatibility with MySQL which is expected by certain 3rd party tools. As string argument support may in future be made dependent on new MySQL-compatibility settings and because string parsing is generally slow, it is recommended to not use it.

**Returned value**

- The day of the week (1-7), depending on the chosen mode, of the given date/time

**Example**

The following date is April 21, 2023, which was a Friday:

```sql
SELECT
    toDayOfWeek(toDateTime('2023-04-21')),
    toDayOfWeek(toDateTime('2023-04-21'), 1)
```

Result:

```response
┌─toDayOfWeek(toDateTime('2023-04-21'))─┬─toDayOfWeek(toDateTime('2023-04-21'), 1)─┐
│                                     5 │                                        4 │
└───────────────────────────────────────┴──────────────────────────────────────────┘
```

## toHour

Returns the hour component (0-24) of a date with time.

Assumes that if clocks are moved ahead, it is by one hour and occurs at 2 a.m., and if clocks are moved back, it is by one hour and occurs at 3 a.m. (which is not always exactly when it occurs - it depends on the timezone).

**Syntax**

```sql
toHour(value)
```

Alias: `HOUR`

**Arguments**

- `value` - a [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The hour of the day (0 - 23) of the given date/time. [UInt8](../data-types/int-uint.md).

**Example**

```sql
SELECT toHour(toDateTime('2023-04-21 10:20:30'))
```

Result:

```response
┌─toHour(toDateTime('2023-04-21 10:20:30'))─┐
│                                        10 │
└───────────────────────────────────────────┘
```

## toMinute

Returns the minute component (0-59) a date with time.

**Syntax**

```sql
toMinute(value)
```

Alias: `MINUTE`

**Arguments**

- `value` - a [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The minute of the hour (0 - 59) of the given date/time. [UInt8](../data-types/int-uint.md).

**Example**

```sql
SELECT toMinute(toDateTime('2023-04-21 10:20:30'))
```

Result:

```response
┌─toMinute(toDateTime('2023-04-21 10:20:30'))─┐
│                                          20 │
└─────────────────────────────────────────────┘
```

## toSecond

Returns the second component (0-59) of a date with time. Leap seconds are not considered.

**Syntax**

```sql
toSecond(value)
```

Alias: `SECOND`

**Arguments**

- `value` - a [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The second in the minute (0 - 59) of the given date/time. [UInt8](../data-types/int-uint.md).

**Example**

```sql
SELECT toSecond(toDateTime('2023-04-21 10:20:30'))
```

Result:

```response
┌─toSecond(toDateTime('2023-04-21 10:20:30'))─┐
│                                          30 │
└─────────────────────────────────────────────┘
```

## toMillisecond

Returns the millisecond component (0-999) of a date with time.

**Syntax**

```sql
toMillisecond(value)
```

*Arguments**

- `value` - [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

Alias: `MILLISECOND`

```sql
SELECT toMillisecond(toDateTime64('2023-04-21 10:20:30.456', 3))
```

Result:

```response
┌──toMillisecond(toDateTime64('2023-04-21 10:20:30.456', 3))─┐
│                                                        456 │
└────────────────────────────────────────────────────────────┘
```

**Returned value**

- The millisecond in the minute (0 - 59) of the given date/time. [UInt16](../data-types/int-uint.md).

## toUnixTimestamp

Converts a string, a date or a date with time to the [Unix Timestamp](https://en.wikipedia.org/wiki/Unix_time) in `UInt32` representation.

If the function is called with a string, it accepts an optional timezone argument.

**Syntax**

``` sql
toUnixTimestamp(date)
toUnixTimestamp(str, [timezone])
```

**Returned value**

- Returns the unix timestamp. [UInt32](../data-types/int-uint.md).

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

Rounds down a date or date with time to the first day of the year. Returns the date as a `Date` object.

**Syntax**

```sql
toStartOfYear(value)
```

**Arguments**

- `value` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The first day of the year of the input date/time. [Date](../data-types/date.md).

**Example**

```sql
SELECT toStartOfYear(toDateTime('2023-04-21 10:20:30'))
```

Result:

```response
┌─toStartOfYear(toDateTime('2023-04-21 10:20:30'))─┐
│                                       2023-01-01 │
└──────────────────────────────────────────────────┘
```

## toStartOfISOYear

Rounds down a date or date with time to the first day of the ISO year, which can be different than a "regular" year. (See [https://en.wikipedia.org/wiki/ISO_week_date](https://en.wikipedia.org/wiki/ISO_week_date).)

**Syntax**

```sql
toStartOfISOYear(value)
```

**Arguments**

- `value` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The first day of the year of the input date/time. [Date](../data-types/date.md).

**Example**

```sql
SELECT toStartOfISOYear(toDateTime('2023-04-21 10:20:30'))
```

Result:

```response
┌─toStartOfISOYear(toDateTime('2023-04-21 10:20:30'))─┐
│                                          2023-01-02 │
└─────────────────────────────────────────────────────┘
```

## toStartOfQuarter

Rounds down a date or date with time to the first day of the quarter. The first day of the quarter is either 1 January, 1 April, 1 July, or 1 October.
Returns the date.

**Syntax**

```sql
toStartOfQuarter(value)
```

**Arguments**

- `value` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The first day of the quarter of the given date/time. [Date](../data-types/date.md).

**Example**

```sql
SELECT toStartOfQuarter(toDateTime('2023-04-21 10:20:30'))
```

Result:

```response
┌─toStartOfQuarter(toDateTime('2023-04-21 10:20:30'))─┐
│                                          2023-04-01 │
└─────────────────────────────────────────────────────┘
```

## toStartOfMonth

Rounds down a date or date with time to the first day of the month. Returns the date.

**Syntax**

```sql
toStartOfMonth(value)
```

**Arguments**

- `value` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The first day of the month of the given date/time. [Date](../data-types/date.md).

**Example**

```sql
SELECT toStartOfMonth(toDateTime('2023-04-21 10:20:30'))
```

Result:

```response
┌─toStartOfMonth(toDateTime('2023-04-21 10:20:30'))─┐
│                                        2023-04-01 │
└───────────────────────────────────────────────────┘
```

:::note
The behavior of parsing incorrect dates is implementation specific. ClickHouse may return zero date, throw an exception, or do “natural” overflow.
:::

## toLastDayOfMonth

Rounds a date or date with time to the last day of the month. Returns the date.

**Syntax**

```sql
toLastDayOfMonth(value)
```

Alias: `LAST_DAY`

**Arguments**

- `value` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The last day of the month of the given date/time=. [Date](../data-types/date.md).

**Example**

```sql
SELECT toLastDayOfMonth(toDateTime('2023-04-21 10:20:30'))
```

Result:

```response
┌─toLastDayOfMonth(toDateTime('2023-04-21 10:20:30'))─┐
│                                          2023-04-30 │
└─────────────────────────────────────────────────────┘
```

## toMonday

Rounds down a date or date with time to the nearest Monday. Returns the date.

**Syntax**

```sql
toMonday(value)
```

**Arguments**

- `value` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The date of the nearest Monday on or prior to the given date. [Date](../data-types/date.md).

**Example**

```sql
SELECT
    toMonday(toDateTime('2023-04-21 10:20:30')), /* a Friday */
    toMonday(toDate('2023-04-24')), /* already a Monday */
```

Result:

```response
┌─toMonday(toDateTime('2023-04-21 10:20:30'))─┬─toMonday(toDate('2023-04-24'))─┐
│                                  2023-04-17 │                     2023-04-24 │
└─────────────────────────────────────────────┴────────────────────────────────┘
```

## toStartOfWeek

Rounds a date or date with time down to the nearest Sunday or Monday. Returns the date. The mode argument works exactly like the mode argument in function `toWeek()`. If no mode is specified, it defaults to 0.

**Syntax**

``` sql
toStartOfWeek(t[, mode[, timezone]])
```

**Arguments**

- `t` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)
- `mode` - determines the first day of the week as described in the [toWeek()](#toweek) function
- `timezone` - Optional parameter, it behaves like any other conversion function

**Returned value**

- The date of the nearest Sunday or Monday on or prior to the given date, depending on the mode. [Date](../data-types/date.md).

**Example**

```sql
SELECT
    toStartOfWeek(toDateTime('2023-04-21 10:20:30')), /* a Friday */
    toStartOfWeek(toDateTime('2023-04-21 10:20:30'), 1), /* a Friday */
    toStartOfWeek(toDate('2023-04-24')), /* a Monday */
    toStartOfWeek(toDate('2023-04-24'), 1) /* a Monday */
FORMAT Vertical
```

Result:

```response
Row 1:
──────
toStartOfWeek(toDateTime('2023-04-21 10:20:30')):    2023-04-16
toStartOfWeek(toDateTime('2023-04-21 10:20:30'), 1): 2023-04-17
toStartOfWeek(toDate('2023-04-24')):                 2023-04-23
toStartOfWeek(toDate('2023-04-24'), 1):              2023-04-24
```

## toLastDayOfWeek

Rounds a date or date with time up to the nearest Saturday or Sunday. Returns the date.
The mode argument works exactly like the mode argument in function `toWeek()`. If no mode is specified, mode is assumed as 0.

**Syntax**

``` sql
toLastDayOfWeek(t[, mode[, timezone]])
```

**Arguments**

- `t` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)
- `mode` - determines the last day of the week as described in the [toWeek](#toweek) function
- `timezone` - Optional parameter, it behaves like any other conversion function

**Returned value**

- The date of the nearest Sunday or Monday on or after the given date, depending on the mode. [Date](../data-types/date.md).

**Example**

```sql
SELECT
    toLastDayOfWeek(toDateTime('2023-04-21 10:20:30')), /* a Friday */
    toLastDayOfWeek(toDateTime('2023-04-21 10:20:30'), 1), /* a Friday */
    toLastDayOfWeek(toDate('2023-04-22')), /* a Saturday */
    toLastDayOfWeek(toDate('2023-04-22'), 1) /* a Saturday */
FORMAT Vertical
```

Result:

```response
Row 1:
──────
toLastDayOfWeek(toDateTime('2023-04-21 10:20:30')):    2023-04-22
toLastDayOfWeek(toDateTime('2023-04-21 10:20:30'), 1): 2023-04-23
toLastDayOfWeek(toDate('2023-04-22')):                 2023-04-22
toLastDayOfWeek(toDate('2023-04-22'), 1):              2023-04-23
```

## toStartOfDay

Rounds down a date with time to the start of the day.

**Syntax**

```sql
toStartOfDay(value)
```

**Arguments**

- `value` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The start of the day of the given date/time. [DateTime](../data-types/datetime.md).

**Example**

```sql
SELECT toStartOfDay(toDateTime('2023-04-21 10:20:30'))
```

Result:

```response
┌─toStartOfDay(toDateTime('2023-04-21 10:20:30'))─┐
│                             2023-04-21 00:00:00 │
└─────────────────────────────────────────────────┘
```

## toStartOfHour

Rounds down a date with time to the start of the hour.

**Syntax**

```sql
toStartOfHour(value)
```

**Arguments**

- `value` - a  [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The start of the hour of the given date/time. [DateTime](../data-types/datetime.md).

**Example**

```sql
SELECT
    toStartOfHour(toDateTime('2023-04-21 10:20:30')),
    toStartOfHour(toDateTime64('2023-04-21', 6))
```

Result:

```response
┌─toStartOfHour(toDateTime('2023-04-21 10:20:30'))─┬─toStartOfHour(toDateTime64('2023-04-21', 6))─┐
│                              2023-04-21 10:00:00 │                          2023-04-21 00:00:00 │
└──────────────────────────────────────────────────┴──────────────────────────────────────────────┘
```

## toStartOfMinute

Rounds down a date with time to the start of the minute.

**Syntax**

```sql
toStartOfMinute(value)
```

**Arguments**

- `value` - a  [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The start of the minute of the given date/time. [DateTime](../data-types/datetime.md).

**Example**

```sql
SELECT
    toStartOfMinute(toDateTime('2023-04-21 10:20:30')),
    toStartOfMinute(toDateTime64('2023-04-21 10:20:30.5300', 8))
FORMAT Vertical
```

Result:

```response
Row 1:
──────
toStartOfMinute(toDateTime('2023-04-21 10:20:30')):           2023-04-21 10:20:00
toStartOfMinute(toDateTime64('2023-04-21 10:20:30.5300', 8)): 2023-04-21 10:20:00
```

## toStartOfSecond

Truncates sub-seconds.

**Syntax**

``` sql
toStartOfSecond(value, [timezone])
```

**Arguments**

- `value` — Date and time. [DateTime64](../data-types/datetime64.md).
- `timezone` — [Timezone](../../operations/server-configuration-parameters/settings.md#timezone) for the returned value (optional). If not specified, the function uses the timezone of the `value` parameter. [String](../data-types/string.md).

**Returned value**

- Input value without sub-seconds. [DateTime64](../data-types/datetime64.md).

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

- [Timezone](../../operations/server-configuration-parameters/settings.md#timezone) server configuration parameter.

## toStartOfMillisecond

Rounds down a date with time to the start of the milliseconds.

**Syntax**

``` sql
toStartOfMillisecond(value, [timezone])
```

**Arguments**

- `value` — Date and time. [DateTime64](../../sql-reference/data-types/datetime64.md).
- `timezone` — [Timezone](../../operations/server-configuration-parameters/settings.md#timezone) for the returned value (optional). If not specified, the function uses the timezone of the `value` parameter. [String](../../sql-reference/data-types/string.md).

**Returned value**

- Input value with sub-milliseconds. [DateTime64](../../sql-reference/data-types/datetime64.md).

**Examples**

Query without timezone:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999999999', 9) AS dt64
SELECT toStartOfMillisecond(dt64);
```

Result:

``` text
┌────toStartOfMillisecond(dt64)─┐
│ 2020-01-01 10:20:30.999000000 │
└───────────────────────────────┘
```

Query with timezone:

``` sql
┌─toStartOfMillisecond(dt64, 'Asia/Istanbul')─┐
│               2020-01-01 12:20:30.999000000 │
└─────────────────────────────────────────────┘
```

Result:

``` text
┌─toStartOfMillisecond(dt64, 'Asia/Istanbul')─┐
│                     2020-01-01 12:20:30.999 │
└─────────────────────────────────────────────┘
```

## toStartOfMicrosecond

Rounds down a date with time to the start of the microseconds.

**Syntax**

``` sql
toStartOfMicrosecond(value, [timezone])
```

**Arguments**

- `value` — Date and time. [DateTime64](../../sql-reference/data-types/datetime64.md).
- `timezone` — [Timezone](../../operations/server-configuration-parameters/settings.md#timezone) for the returned value (optional). If not specified, the function uses the timezone of the `value` parameter. [String](../../sql-reference/data-types/string.md).

**Returned value**

- Input value with sub-microseconds. [DateTime64](../../sql-reference/data-types/datetime64.md).

**Examples**

Query without timezone:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999999999', 9) AS dt64
SELECT toStartOfMicrosecond(dt64);
```

Result:

``` text
┌────toStartOfMicrosecond(dt64)─┐
│ 2020-01-01 10:20:30.999999000 │
└───────────────────────────────┘
```

Query with timezone:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999999999', 9) AS dt64
SELECT toStartOfMicrosecond(dt64, 'Asia/Istanbul');
```

Result:

``` text
┌─toStartOfMicrosecond(dt64, 'Asia/Istanbul')─┐
│               2020-01-01 12:20:30.999999000 │
└─────────────────────────────────────────────┘
```

**See also**

- [Timezone](../../operations/server-configuration-parameters/settings.md#timezone) server configuration parameter.

## toStartOfNanosecond

Rounds down a date with time to the start of the nanoseconds.

**Syntax**

``` sql
toStartOfNanosecond(value, [timezone])
```

**Arguments**

- `value` — Date and time. [DateTime64](../../sql-reference/data-types/datetime64.md).
- `timezone` — [Timezone](../../operations/server-configuration-parameters/settings.md#timezone) for the returned value (optional). If not specified, the function uses the timezone of the `value` parameter. [String](../../sql-reference/data-types/string.md).

**Returned value**

- Input value with nanoseconds. [DateTime64](../../sql-reference/data-types/datetime64.md).

**Examples**

Query without timezone:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999999999', 9) AS dt64
SELECT toStartOfNanosecond(dt64);
```

Result:

``` text
┌─────toStartOfNanosecond(dt64)─┐
│ 2020-01-01 10:20:30.999999999 │
└───────────────────────────────┘
```

Query with timezone:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999999999', 9) AS dt64
SELECT toStartOfNanosecond(dt64, 'Asia/Istanbul');
```

Result:

``` text
┌─toStartOfNanosecond(dt64, 'Asia/Istanbul')─┐
│              2020-01-01 12:20:30.999999999 │
└────────────────────────────────────────────┘
```

**See also**

- [Timezone](../../operations/server-configuration-parameters/settings.md#timezone) server configuration parameter.

## toStartOfFiveMinutes

Rounds down a date with time to the start of the five-minute interval.

**Syntax**

```sql
toStartOfFiveMinutes(value)
```

**Arguments**

- `value` - a  [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The start of the five-minute interval of the given date/time. [DateTime](../data-types/datetime.md).

**Example**

```sql
SELECT
    toStartOfFiveMinutes(toDateTime('2023-04-21 10:17:00')),
    toStartOfFiveMinutes(toDateTime('2023-04-21 10:20:00')),
    toStartOfFiveMinutes(toDateTime('2023-04-21 10:23:00'))
FORMAT Vertical
```

Result:

```response
Row 1:
──────
toStartOfFiveMinutes(toDateTime('2023-04-21 10:17:00')): 2023-04-21 10:15:00
toStartOfFiveMinutes(toDateTime('2023-04-21 10:20:00')): 2023-04-21 10:20:00
toStartOfFiveMinutes(toDateTime('2023-04-21 10:23:00')): 2023-04-21 10:20:00
```

## toStartOfTenMinutes

Rounds down a date with time to the start of the ten-minute interval.

**Syntax**

```sql
toStartOfTenMinutes(value)
```

**Arguments**

- `value` - a  [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The start of the ten-minute interval of the given date/time. [DateTime](../data-types/datetime.md).

**Example**

```sql
SELECT
    toStartOfTenMinutes(toDateTime('2023-04-21 10:17:00')),
    toStartOfTenMinutes(toDateTime('2023-04-21 10:20:00')),
    toStartOfTenMinutes(toDateTime('2023-04-21 10:23:00'))
FORMAT Vertical
```

Result:

```response
Row 1:
──────
toStartOfTenMinutes(toDateTime('2023-04-21 10:17:00')): 2023-04-21 10:10:00
toStartOfTenMinutes(toDateTime('2023-04-21 10:20:00')): 2023-04-21 10:20:00
toStartOfTenMinutes(toDateTime('2023-04-21 10:23:00')): 2023-04-21 10:20:00
```

## toStartOfFifteenMinutes

Rounds down the date with time to the start of the fifteen-minute interval.

**Syntax**

```sql
toStartOfFifteenMinutes(value)
```

**Arguments**

- `value` - a  [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The start of the fifteen-minute interval of the given date/time. [DateTime](../data-types/datetime.md).

**Example**

```sql
SELECT
    toStartOfFifteenMinutes(toDateTime('2023-04-21 10:17:00')),
    toStartOfFifteenMinutes(toDateTime('2023-04-21 10:20:00')),
    toStartOfFifteenMinutes(toDateTime('2023-04-21 10:23:00'))
FORMAT Vertical
```

Result:

```response
Row 1:
──────
toStartOfFifteenMinutes(toDateTime('2023-04-21 10:17:00')): 2023-04-21 10:15:00
toStartOfFifteenMinutes(toDateTime('2023-04-21 10:20:00')): 2023-04-21 10:15:00
toStartOfFifteenMinutes(toDateTime('2023-04-21 10:23:00')): 2023-04-21 10:15:00
```

## toStartOfInterval

This function generalizes other `toStartOf*()` functions with `toStartOfInterval(date_or_date_with_time, INTERVAL x unit [, time_zone])` syntax.
For example,
- `toStartOfInterval(t, INTERVAL 1 YEAR)` returns the same as `toStartOfYear(t)`,
- `toStartOfInterval(t, INTERVAL 1 MONTH)` returns the same as `toStartOfMonth(t)`,
- `toStartOfInterval(t, INTERVAL 1 DAY)` returns the same as `toStartOfDay(t)`,
- `toStartOfInterval(t, INTERVAL 15 MINUTE)` returns the same as `toStartOfFifteenMinutes(t)`.

The calculation is performed relative to specific points in time:

| Interval    | Start                  |
|-------------|------------------------|
| YEAR        | year 0                 |
| QUARTER     | 1900 Q1                |
| MONTH       | 1900 January           |
| WEEK        | 1970, 1st week (01-05) |
| DAY         | 1970-01-01             |
| HOUR        | (*)                    |
| MINUTE      | 1970-01-01 00:00:00    |
| SECOND      | 1970-01-01 00:00:00    |
| MILLISECOND | 1970-01-01 00:00:00    |
| MICROSECOND | 1970-01-01 00:00:00    |
| NANOSECOND  | 1970-01-01 00:00:00    |

(*) hour intervals are special: the calculation is always performed relative to 00:00:00 (midnight) of the current day. As a result, only
    hour values between 1 and 23 are useful.

If unit `WEEK` was specified, `toStartOfInterval` assumes that weeks start on Monday. Note that this behavior is different from that of function `toStartOfWeek` in which weeks start by default on Sunday.

**Syntax**

```sql
toStartOfInterval(value, INTERVAL x unit[, time_zone])
toStartOfInterval(value, INTERVAL x unit[, origin[, time_zone]])
```
Aliases: `time_bucket`, `date_bin`.

The second overload emulates TimescaleDB's `time_bucket()` function, respectively PostgreSQL's `date_bin()` function, e.g.

``` SQL
SELECT toStartOfInterval(toDateTime('2023-01-01 14:45:00'), INTERVAL 1 MINUTE, toDateTime('2023-01-01 14:35:30'));
```

Result:

``` reference
┌───toStartOfInterval(...)─┐
│      2023-01-01 14:44:30 │
└──────────────────────────┘
```

**See Also**
- [date_trunc](#date_trunc)

## toTime

Converts a date with time to a certain fixed date, while preserving the time.

**Syntax**

```sql
toTime(date[,timezone])
```

**Arguments**

- `date` — Date to convert to a time. [Date](../data-types/date.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).
- `timezone` (optional) — Timezone for the returned value. [String](../data-types/string.md).

**Returned value**

- DateTime with date equated to `1970-01-02` while preserving the time. [DateTime](../data-types/datetime.md).

:::note
If the `date` input argument contained sub-second components,
they will be dropped in the returned `DateTime` value with second-accuracy.
:::

**Example**

Query:

```sql
SELECT toTime(toDateTime64('1970-12-10 01:20:30.3000',3)) AS result, toTypeName(result);
```

Result:

```response
┌──────────────result─┬─toTypeName(result)─┐
│ 1970-01-02 01:20:30 │ DateTime           │
└─────────────────────┴────────────────────┘
```

## toRelativeYearNum

Converts a date, or date with time, to the number of years elapsed since a certain fixed point in the past.

**Syntax**

```sql
toRelativeYearNum(date)
```

**Arguments**

- `date` — Date or date with time. [Date](../data-types/date.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Returned value**

- The number of years from a fixed reference point in the past. [UInt16](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT
    toRelativeYearNum(toDate('2002-12-08')) AS y1,
    toRelativeYearNum(toDate('2010-10-26')) AS y2
```

Result:

```response
┌───y1─┬───y2─┐
│ 2002 │ 2010 │
└──────┴──────┘
```

## toRelativeQuarterNum

Converts a date, or date with time, to the number of quarters elapsed since a certain fixed point in the past.

**Syntax**

```sql
toRelativeQuarterNum(date)
```

**Arguments**

- `date` — Date or date with time. [Date](../data-types/date.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Returned value**

- The number of quarters from a fixed reference point in the past. [UInt32](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT
  toRelativeQuarterNum(toDate('1993-11-25')) AS q1,
  toRelativeQuarterNum(toDate('2005-01-05')) AS q2
```

Result:

```response
┌───q1─┬───q2─┐
│ 7975 │ 8020 │
└──────┴──────┘
```

## toRelativeMonthNum

Converts a date, or date with time, to the number of months elapsed since a certain fixed point in the past.

**Syntax**

```sql
toRelativeMonthNum(date)
```

**Arguments**

- `date` — Date or date with time. [Date](../data-types/date.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Returned value**

- The number of months from a fixed reference point in the past. [UInt32](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT
  toRelativeMonthNum(toDate('2001-04-25')) AS m1,
  toRelativeMonthNum(toDate('2009-07-08')) AS m2
```

Result:

```response
┌────m1─┬────m2─┐
│ 24016 │ 24115 │
└───────┴───────┘
```

## toRelativeWeekNum

Converts a date, or date with time, to the number of weeks elapsed since a certain fixed point in the past.

**Syntax**

```sql
toRelativeWeekNum(date)
```

**Arguments**

- `date` — Date or date with time. [Date](../data-types/date.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Returned value**

- The number of weeks from a fixed reference point in the past. [UInt32](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT
  toRelativeWeekNum(toDate('2000-02-29')) AS w1,
  toRelativeWeekNum(toDate('2001-01-12')) AS w2
```

Result:

```response
┌───w1─┬───w2─┐
│ 1574 │ 1619 │
└──────┴──────┘
```

## toRelativeDayNum

Converts a date, or date with time, to the number of days elapsed since a certain fixed point in the past.

**Syntax**

```sql
toRelativeDayNum(date)
```

**Arguments**

- `date` — Date or date with time. [Date](../data-types/date.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Returned value**

- The number of days from a fixed reference point in the past. [UInt32](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT
  toRelativeDayNum(toDate('1993-10-05')) AS d1,
  toRelativeDayNum(toDate('2000-09-20')) AS d2
```

Result:

```response
┌───d1─┬────d2─┐
│ 8678 │ 11220 │
└──────┴───────┘
```

## toRelativeHourNum

Converts a date, or date with time, to the number of hours elapsed since a certain fixed point in the past.

**Syntax**

```sql
toRelativeHourNum(date)
```

**Arguments**

- `date` — Date or date with time. [Date](../data-types/date.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Returned value**

- The number of hours from a fixed reference point in the past. [UInt32](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT
  toRelativeHourNum(toDateTime('1993-10-05 05:20:36')) AS h1,
  toRelativeHourNum(toDateTime('2000-09-20 14:11:29')) AS h2
```

Result:

```response
┌─────h1─┬─────h2─┐
│ 208276 │ 269292 │
└────────┴────────┘
```

## toRelativeMinuteNum

Converts a date, or date with time, to the number of minutes elapsed since a certain fixed point in the past.

**Syntax**

```sql
toRelativeMinuteNum(date)
```

**Arguments**

- `date` — Date or date with time. [Date](../data-types/date.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Returned value**

- The number of minutes from a fixed reference point in the past. [UInt32](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT
  toRelativeMinuteNum(toDateTime('1993-10-05 05:20:36')) AS m1,
  toRelativeMinuteNum(toDateTime('2000-09-20 14:11:29')) AS m2
```

Result:

```response
┌───────m1─┬───────m2─┐
│ 12496580 │ 16157531 │
└──────────┴──────────┘
```

## toRelativeSecondNum

Converts a date, or date with time, to the number of the seconds elapsed since a certain fixed point in the past.

**Syntax**

```sql
toRelativeSecondNum(date)
```

**Arguments**

- `date` — Date or date with time. [Date](../data-types/date.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Returned value**

- The number of seconds from a fixed reference point in the past. [UInt32](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT
  toRelativeSecondNum(toDateTime('1993-10-05 05:20:36')) AS s1,
  toRelativeSecondNum(toDateTime('2000-09-20 14:11:29')) AS s2
```

Result:

```response
┌────────s1─┬────────s2─┐
│ 749794836 │ 969451889 │
└───────────┴───────────┘
```

## toISOYear

Converts a date, or date with time, to the ISO year as a UInt16 number.

**Syntax**

```sql
toISOYear(value)
```

**Arguments**

- `value` — The value with date or date with time. [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)

**Returned value**

- The input value converted to a ISO year number. [UInt16](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT
  toISOYear(toDate('2024/10/02')) as year1,
  toISOYear(toDateTime('2024-10-02 01:30:00')) as year2
```

Result:

```response
┌─year1─┬─year2─┐
│  2024 │  2024 │
└───────┴───────┘
```

## toISOWeek

Converts a date, or date with time, to a UInt8 number containing the ISO Week number.

**Syntax**

```sql
toISOWeek(value)
```

**Arguments**

- `value` — The value with date or date with time.

**Returned value**

- `value` converted to the current ISO week number. [UInt8](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT
  toISOWeek(toDate('2024/10/02')) AS week1,
  toISOWeek(toDateTime('2024/10/02 01:30:00')) AS week2
```

Response:

```response
┌─week1─┬─week2─┐
│    40 │    40 │
└───────┴───────┘
```

## toWeek

This function returns the week number for date or datetime. The two-argument form of `toWeek()` enables you to specify whether the week starts on Sunday or Monday and whether the return value should be in the range from 0 to 53 or from 1 to 53. If the mode argument is omitted, the default mode is 0.

`toISOWeek()` is a compatibility function that is equivalent to `toWeek(date,3)`.

The following table describes how the mode argument works.

| Mode | First day of week | Range | Week 1 is the first week ...    |
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

For mode values with a meaning of “contains January 1”, the week contains January 1 is week 1.
It does not matter how many days in the new year the week contained, even if it contained only one day.
I.e. if the last week of December contains January 1 of the next year, it will be week 1 of the next year.

**Syntax**

``` sql
toWeek(t[, mode[, time_zone]])
```

Alias: `WEEK`

**Arguments**

- `t` – Date or DateTime.
- `mode` – Optional parameter, Range of values is \[0,9\], default is 0.
- `Timezone` – Optional parameter, it behaves like any other conversion function.

The first argument can also be specified as [String](../data-types/string.md) in a format supported by [parseDateTime64BestEffort()](type-conversion-functions.md#parsedatetime64besteffort). Support for string arguments exists only for reasons of compatibility with MySQL which is expected by certain 3rd party tools. As string argument support may in future be made dependent on new MySQL-compatibility settings and because string parsing is generally slow, it is recommended to not use it.

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

Alias: `YEARWEEK`

The first argument can also be specified as [String](../data-types/string.md) in a format supported by [parseDateTime64BestEffort()](type-conversion-functions.md#parsedatetime64besteffort). Support for string arguments exists only for reasons of compatibility with MySQL which is expected by certain 3rd party tools. As string argument support may in future be made dependent on new MySQL-compatibility settings and because string parsing is generally slow, it is recommended to not use it.

**Example**

``` sql
SELECT toDate('2016-12-27') AS date, toYearWeek(date) AS yearWeek0, toYearWeek(date,1) AS yearWeek1, toYearWeek(date,9) AS yearWeek9, toYearWeek(toDate('2022-01-01')) AS prev_yearWeek;
```

``` text
┌───────date─┬─yearWeek0─┬─yearWeek1─┬─yearWeek9─┬─prev_yearWeek─┐
│ 2016-12-27 │    201652 │    201652 │    201701 │        202152 │
└────────────┴───────────┴───────────┴───────────┴───────────────┘
```

## toDaysSinceYearZero

Returns for a given date, the number of days passed since [1 January 0000](https://en.wikipedia.org/wiki/Year_zero) in the [proleptic Gregorian calendar defined by ISO 8601](https://en.wikipedia.org/wiki/Gregorian_calendar#Proleptic_Gregorian_calendar). The calculation is the same as in MySQL's [`TO_DAYS()`](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_to-days) function.

**Syntax**

``` sql
toDaysSinceYearZero(date[, time_zone])
```

Alias: `TO_DAYS`

**Arguments**

- `date` — The date to calculate the number of days passed since year zero from. [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).
- `time_zone` — A String type const value or an expression represent the time zone. [String types](../data-types/string.md)

**Returned value**

The number of days passed since date 0000-01-01. [UInt32](../data-types/int-uint.md).

**Example**

``` sql
SELECT toDaysSinceYearZero(toDate('2023-09-08'));
```

Result:

``` text
┌─toDaysSinceYearZero(toDate('2023-09-08')))─┐
│                                     713569 │
└────────────────────────────────────────────┘
```

**See Also**

- [fromDaysSinceYearZero](#fromdayssinceyearzero)

## fromDaysSinceYearZero

Returns for a given number of days passed since [1 January 0000](https://en.wikipedia.org/wiki/Year_zero) the corresponding date in the [proleptic Gregorian calendar defined by ISO 8601](https://en.wikipedia.org/wiki/Gregorian_calendar#Proleptic_Gregorian_calendar). The calculation is the same as in MySQL's [`FROM_DAYS()`](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_from-days) function.

The result is undefined if it cannot be represented within the bounds of the [Date](../data-types/date.md) type.

**Syntax**

``` sql
fromDaysSinceYearZero(days)
```

Alias: `FROM_DAYS`

**Arguments**

- `days` — The number of days passed since year zero.

**Returned value**

The date corresponding to the number of days passed since year zero. [Date](../data-types/date.md).

**Example**

``` sql
SELECT fromDaysSinceYearZero(739136), fromDaysSinceYearZero(toDaysSinceYearZero(toDate('2023-09-08')));
```

Result:

``` text
┌─fromDaysSinceYearZero(739136)─┬─fromDaysSinceYearZero(toDaysSinceYearZero(toDate('2023-09-08')))─┐
│                    2023-09-08 │                                                       2023-09-08 │
└───────────────────────────────┴──────────────────────────────────────────────────────────────────┘
```

**See Also**

- [toDaysSinceYearZero](#todayssinceyearzero)

## fromDaysSinceYearZero32

Like [fromDaysSinceYearZero](#fromdayssinceyearzero) but returns a [Date32](../data-types/date32.md).

## age

Returns the `unit` component of the difference between `startdate` and `enddate`. The difference is calculated using a precision of 1 nanosecond.
E.g. the difference between `2021-12-29` and `2022-01-01` is 3 days for `day` unit, 0 months for `month` unit, 0 years for `year` unit.

For an alternative to `age`, see function `date_diff`.

**Syntax**

``` sql
age('unit', startdate, enddate, [timezone])
```

**Arguments**

- `unit` — The type of interval for result. [String](../data-types/string.md).
    Possible values:

    - `nanosecond`, `nanoseconds`, `ns`
    - `microsecond`, `microseconds`, `us`, `u`
    - `millisecond`, `milliseconds`, `ms`
    - `second`, `seconds`, `ss`, `s`
    - `minute`, `minutes`, `mi`, `n`
    - `hour`, `hours`, `hh`, `h`
    - `day`, `days`, `dd`, `d`
    - `week`, `weeks`, `wk`, `ww`
    - `month`, `months`, `mm`, `m`
    - `quarter`, `quarters`, `qq`, `q`
    - `year`, `years`, `yyyy`, `yy`

- `startdate` — The first time value to subtract (the subtrahend). [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).

- `enddate` — The second time value to subtract from (the minuend). [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).

- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#timezone) (optional). If specified, it is applied to both `startdate` and `enddate`. If not specified, timezones of `startdate` and `enddate` are used. If they are not the same, the result is unspecified. [String](../data-types/string.md).

**Returned value**

Difference between `enddate` and `startdate` expressed in `unit`. [Int](../data-types/int-uint.md).

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


## date_diff

Returns the count of the specified `unit` boundaries crossed between the `startdate` and the `enddate`.
The difference is calculated using relative units, e.g. the difference between `2021-12-29` and `2022-01-01` is 3 days for unit `day` (see [toRelativeDayNum](#torelativedaynum)), 1 month for unit `month` (see [toRelativeMonthNum](#torelativemonthnum)) and 1 year for unit `year` (see [toRelativeYearNum](#torelativeyearnum)).

If unit `week` was specified, `date_diff` assumes that weeks start on Monday. Note that this behavior is different from that of function `toWeek()` in which weeks start by default on Sunday.

For an alternative to `date_diff`, see function `age`.

**Syntax**

``` sql
date_diff('unit', startdate, enddate, [timezone])
```

Aliases: `dateDiff`, `DATE_DIFF`, `timestampDiff`, `timestamp_diff`, `TIMESTAMP_DIFF`.

**Arguments**

- `unit` — The type of interval for result. [String](../data-types/string.md).
    Possible values:

    - `nanosecond`, `nanoseconds`, `ns`
    - `microsecond`, `microseconds`, `us`, `u`
    - `millisecond`, `milliseconds`, `ms`
    - `second`, `seconds`, `ss`, `s`
    - `minute`, `minutes`, `mi`, `n`
    - `hour`, `hours`, `hh`, `h`
    - `day`, `days`, `dd`, `d`
    - `week`, `weeks`, `wk`, `ww`
    - `month`, `months`, `mm`, `m`
    - `quarter`, `quarters`, `qq`, `q`
    - `year`, `years`, `yyyy`, `yy`

- `startdate` — The first time value to subtract (the subtrahend). [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).

- `enddate` — The second time value to subtract from (the minuend). [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).

- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#timezone) (optional). If specified, it is applied to both `startdate` and `enddate`. If not specified, timezones of `startdate` and `enddate` are used. If they are not the same, the result is unspecified. [String](../data-types/string.md).

**Returned value**

Difference between `enddate` and `startdate` expressed in `unit`. [Int](../data-types/int-uint.md).

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

    - `nanosecond` - Compatible only with DateTime64
    - `microsecond` - Compatible only with DateTime64
    - `milisecond` - Compatible only with DateTime64
    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

    `unit` argument is case-insensitive.

- `value` — Date and time. [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).
- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#timezone) for the returned value (optional). If not specified, the function uses the timezone of the `value` parameter. [String](../data-types/string.md).

**Returned value**

- Value, truncated to the specified part of date. [DateTime](../data-types/datetime.md).

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

- [toStartOfInterval](#tostartofinterval)

## date\_add

Adds the time interval or date interval to the provided date or date with time.

If the addition results in a value outside the bounds of the data type, the result is undefined.

**Syntax**

``` sql
date_add(unit, value, date)
```

Alternative syntax:

``` sql
date_add(date, INTERVAL value unit)
```

Aliases: `dateAdd`, `DATE_ADD`.

**Arguments**

- `unit` — The type of interval to add. Note: This is not a [String](../data-types/string.md) and must therefore not be quoted.
    Possible values:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

- `value` — Value of interval to add. [Int](../data-types/int-uint.md).
- `date` — The date or date with time to which `value` is added. [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).

**Returned value**

Date or date with time obtained by adding `value`, expressed in `unit`, to `date`. [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).

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

```sql
SELECT date_add(toDate('2018-01-01'), INTERVAL 3 YEAR);
```

Result:

```text
┌─plus(toDate('2018-01-01'), toIntervalYear(3))─┐
│                                    2021-01-01 │
└───────────────────────────────────────────────┘
```



**See Also**

- [addDate](#adddate)

## date\_sub

Subtracts the time interval or date interval from the provided date or date with time.

If the subtraction results in a value outside the bounds of the data type, the result is undefined.

**Syntax**

``` sql
date_sub(unit, value, date)
```

Alternative syntax:

``` sql
date_sub(date, INTERVAL value unit)
```


Aliases: `dateSub`, `DATE_SUB`.

**Arguments**

- `unit` — The type of interval to subtract. Note: This is not a [String](../data-types/string.md) and must therefore not be quoted.

    Possible values:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

- `value` — Value of interval to subtract. [Int](../data-types/int-uint.md).
- `date` — The date or date with time from which `value` is subtracted. [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).

**Returned value**

Date or date with time obtained by subtracting `value`, expressed in `unit`, from `date`. [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).

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

``` sql
SELECT date_sub(toDate('2018-01-01'), INTERVAL 3 YEAR);
```

Result:

``` text
┌─minus(toDate('2018-01-01'), toIntervalYear(3))─┐
│                                     2015-01-01 │
└────────────────────────────────────────────────┘
```


**See Also**

- [subDate](#subdate)

## timestamp\_add

Adds the specified time value with the provided date or date time value.

If the addition results in a value outside the bounds of the data type, the result is undefined.

**Syntax**

``` sql
timestamp_add(date, INTERVAL value unit)
```

Aliases: `timeStampAdd`, `TIMESTAMP_ADD`.

**Arguments**

- `date` — Date or date with time. [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).
- `value` — Value of interval to add. [Int](../data-types/int-uint.md).
- `unit` — The type of interval to add. [String](../data-types/string.md).
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

Date or date with time with the specified `value` expressed in `unit` added to `date`. [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).

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

If the subtraction results in a value outside the bounds of the data type, the result is undefined.

**Syntax**

``` sql
timestamp_sub(unit, value, date)
```

Aliases: `timeStampSub`, `TIMESTAMP_SUB`.

**Arguments**

- `unit` — The type of interval to subtract. [String](../data-types/string.md).
    Possible values:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

- `value` — Value of interval to subtract. [Int](../data-types/int-uint.md).
- `date` — Date or date with time. [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).

**Returned value**

Date or date with time obtained by subtracting `value`, expressed in `unit`, from `date`. [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).

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

## addDate

Adds the time interval to the provided date, date with time or String-encoded date / date with time.

If the addition results in a value outside the bounds of the data type, the result is undefined.

**Syntax**

``` sql
addDate(date, interval)
```

**Arguments**

- `date` — The date or date with time to which `interval` is added. [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md), [DateTime64](../data-types/datetime64.md), or [String](../data-types/string.md)
- `interval` — Interval to add. [Interval](../data-types/special-data-types/interval.md).

**Returned value**

Date or date with time obtained by adding `interval` to `date`. [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).

**Example**

```sql
SELECT addDate(toDate('2018-01-01'), INTERVAL 3 YEAR);
```

Result:

```text
┌─addDate(toDate('2018-01-01'), toIntervalYear(3))─┐
│                                       2021-01-01 │
└──────────────────────────────────────────────────┘
```

Alias: `ADDDATE`

**See Also**

- [date_add](#date_add)

## subDate

Subtracts the time interval from the provided date, date with time or String-encoded date / date with time.

If the subtraction results in a value outside the bounds of the data type, the result is undefined.

**Syntax**

``` sql
subDate(date, interval)
```

**Arguments**

- `date` — The date or date with time from which `interval` is subtracted. [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md), [DateTime64](../data-types/datetime64.md), or [String](../data-types/string.md)
- `interval` — Interval to subtract. [Interval](../data-types/special-data-types/interval.md).

**Returned value**

Date or date with time obtained by subtracting `interval` from `date`. [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).

**Example**

```sql
SELECT subDate(toDate('2018-01-01'), INTERVAL 3 YEAR);
```

Result:

```text
┌─subDate(toDate('2018-01-01'), toIntervalYear(3))─┐
│                                       2015-01-01 │
└──────────────────────────────────────────────────┘
```

Alias: `SUBDATE`

**See Also**

- [date_sub](#date_sub)

## now

Returns the current date and time at the moment of query analysis. The function is a constant expression.

Alias: `current_timestamp`.

**Syntax**

``` sql
now([timezone])
```

**Arguments**

- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#timezone) for the returned value (optional). [String](../data-types/string.md).

**Returned value**

- Current date and time. [DateTime](../data-types/datetime.md).

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

- `scale` - Tick size (precision): 10<sup>-precision</sup> seconds. Valid range: [ 0 : 9 ]. Typically, are used - 3 (default) (milliseconds), 6 (microseconds), 9 (nanoseconds).
- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#timezone) for the returned value (optional). [String](../data-types/string.md).

**Returned value**

- Current date and time with sub-second precision. [DateTime64](../data-types/datetime64.md).

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

## nowInBlock {#nowInBlock}

Returns the current date and time at the moment of processing of each block of data. In contrast to the function [now](#now), it is not a constant expression, and the returned value will be different in different blocks for long-running queries.

It makes sense to use this function to generate the current time in long-running INSERT SELECT queries.

**Syntax**

``` sql
nowInBlock([timezone])
```

**Arguments**

- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#timezone) for the returned value (optional). [String](../data-types/string.md).

**Returned value**

- Current date and time at the moment of processing of each block of data. [DateTime](../data-types/datetime.md).

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

## today {#today}

Returns the current date at moment of query analysis. It is the same as ‘toDate(now())’ and has aliases: `curdate`, `current_date`.

**Syntax**

```sql
today()
```

**Arguments**

- None

**Returned value**

- Current date. [DateTime](../data-types/datetime.md).

**Example**

Query:

```sql
SELECT today() AS today, curdate() AS curdate, current_date() AS current_date FORMAT Pretty
```

**Result**:

Running the query above on the 3rd of March 2024 would have returned the following response:

```response
┏━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓
┃      today ┃    curdate ┃ current_date ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━┩
│ 2024-03-03 │ 2024-03-03 │   2024-03-03 │
└────────────┴────────────┴──────────────┘
```

## yesterday {#yesterday}

Accepts zero arguments and returns yesterday’s date at one of the moments of query analysis.
The same as ‘today() - 1’.

## timeSlot

Round the time to the start of a half-an-hour length interval.

**Syntax**

```sql
timeSlot(time[, time_zone])
```

**Arguments**

- `time` — Time to round to the start of a half-an-hour length interval. [DateTime](../data-types/datetime.md)/[Date32](../data-types/date32.md)/[DateTime64](../data-types/datetime64.md).
- `time_zone` — A String type const value or an expression representing the time zone. [String](../data-types/string.md).

:::note
Though this function can take values of the extended types `Date32` and `DateTime64` as an argument, passing it a time outside the normal range (year 1970 to 2149 for `Date` / 2106 for `DateTime`) will produce wrong results.
:::

**Return type**

- Returns the time rounded to the start of a half-an-hour length interval. [DateTime](../data-types/datetime.md).

**Example**

Query:

```sql
SELECT timeSlot(toDateTime('2000-01-02 03:04:05', 'UTC'));
```

Result:

```response
┌─timeSlot(toDateTime('2000-01-02 03:04:05', 'UTC'))─┐
│                                2000-01-02 03:00:00 │
└────────────────────────────────────────────────────┘
```

## toYYYYMM

Converts a date or date with time to a UInt32 number containing the year and month number (YYYY \* 100 + MM). Accepts a second optional timezone argument. If provided, the timezone must be a string constant.

This function is the opposite of function `YYYYMMDDToDate()`.

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
SELECT toYYYYMMDD(now(), 'US/Eastern')
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
SELECT toYYYYMMDDhhmmss(now(), 'US/Eastern')
```

Result:

```response
┌─toYYYYMMDDhhmmss(now(), 'US/Eastern')─┐
│                        20230302112209 │
└───────────────────────────────────────┘
```

## YYYYMMDDToDate

Converts a number containing the year, month and day number to a [Date](../data-types/date.md).

This function is the opposite of function `toYYYYMMDD()`.

The output is undefined if the input does not encode a valid Date value.

**Syntax**

```sql
YYYYMMDDToDate(yyyymmdd);
```

**Arguments**

- `yyyymmdd` - A number representing the year, month and day. [Integer](../data-types/int-uint.md), [Float](../data-types/float.md) or [Decimal](../data-types/decimal.md).

**Returned value**

- a date created from the arguments. [Date](../data-types/date.md).

**Example**

```sql
SELECT YYYYMMDDToDate(20230911);
```

Result:

```response
┌─toYYYYMMDD(20230911)─┐
│           2023-09-11 │
└──────────────────────┘
```

## YYYYMMDDToDate32

Like function `YYYYMMDDToDate()` but produces a [Date32](../data-types/date32.md).

## YYYYMMDDhhmmssToDateTime

Converts a number containing the year, month, day, hours, minute and second number to a [DateTime](../data-types/datetime.md).

The output is undefined if the input does not encode a valid DateTime value.

This function is the opposite of function `toYYYYMMDDhhmmss()`.

**Syntax**

```sql
YYYYMMDDhhmmssToDateTime(yyyymmddhhmmss[, timezone]);
```

**Arguments**

- `yyyymmddhhmmss` - A number representing the year, month and day. [Integer](../data-types/int-uint.md), [Float](../data-types/float.md) or [Decimal](../data-types/decimal.md).
- `timezone` - [Timezone](../../operations/server-configuration-parameters/settings.md#timezone) for the returned value (optional).

**Returned value**

- a date with time created from the arguments. [DateTime](../data-types/datetime.md).

**Example**

```sql
SELECT YYYYMMDDToDateTime(20230911131415);
```

Result:

```response
┌──────YYYYMMDDhhmmssToDateTime(20230911131415)─┐
│                           2023-09-11 13:14:15 │
└───────────────────────────────────────────────┘
```

## YYYYMMDDhhmmssToDateTime64

Like function `YYYYMMDDhhmmssToDate()` but produces a [DateTime64](../data-types/datetime64.md).

Accepts an additional, optional `precision` parameter after the `timezone` parameter.

## changeYear

Changes the year component of a date or date time.

**Syntax**
``` sql

changeYear(date_or_datetime, value)
```

**Arguments**

- `date_or_datetime` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)
- `value` - a new value of the year. [Integer](../../sql-reference/data-types/int-uint.md).

**Returned value**

- The same type as `date_or_datetime`.

**Example**

``` sql
SELECT changeYear(toDate('1999-01-01'), 2000), changeYear(toDateTime64('1999-01-01 00:00:00.000', 3), 2000);
```

Result:

```
┌─changeYear(toDate('1999-01-01'), 2000)─┬─changeYear(toDateTime64('1999-01-01 00:00:00.000', 3), 2000)─┐
│                             2000-01-01 │                                      2000-01-01 00:00:00.000 │
└────────────────────────────────────────┴──────────────────────────────────────────────────────────────┘
```

## changeMonth

Changes the month component of a date or date time.

**Syntax**

``` sql
changeMonth(date_or_datetime, value)
```

**Arguments**

- `date_or_datetime` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)
- `value` - a new value of the month. [Integer](../../sql-reference/data-types/int-uint.md).

**Returned value**

- Returns a value of same type as `date_or_datetime`.

**Example**

``` sql
SELECT changeMonth(toDate('1999-01-01'), 2), changeMonth(toDateTime64('1999-01-01 00:00:00.000', 3), 2);
```

Result:

```
┌─changeMonth(toDate('1999-01-01'), 2)─┬─changeMonth(toDateTime64('1999-01-01 00:00:00.000', 3), 2)─┐
│                           1999-02-01 │                                    1999-02-01 00:00:00.000 │
└──────────────────────────────────────┴────────────────────────────────────────────────────────────┘
```

## changeDay

Changes the day component of a date or date time.

**Syntax**

``` sql
changeDay(date_or_datetime, value)
```

**Arguments**

- `date_or_datetime` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)
- `value` - a new value of the day. [Integer](../../sql-reference/data-types/int-uint.md).

**Returned value**

- Returns a value of same type as `date_or_datetime`.

**Example**

``` sql
SELECT changeDay(toDate('1999-01-01'), 5), changeDay(toDateTime64('1999-01-01 00:00:00.000', 3), 5);
```

Result:

```
┌─changeDay(toDate('1999-01-01'), 5)─┬─changeDay(toDateTime64('1999-01-01 00:00:00.000', 3), 5)─┐
│                         1999-01-05 │                                  1999-01-05 00:00:00.000 │
└────────────────────────────────────┴──────────────────────────────────────────────────────────┘
```

## changeHour

Changes the hour component of a date or date time.

**Syntax**

``` sql
changeHour(date_or_datetime, value)
```

**Arguments**

- `date_or_datetime` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)
- `value` - a new value of the hour. [Integer](../../sql-reference/data-types/int-uint.md).

**Returned value**

- Returns a value of same type as `date_or_datetime`. If the input is a [Date](../data-types/date.md), return [DateTime](../data-types/datetime.md). If the input is a [Date32](../data-types/date32.md), return [DateTime64](../data-types/datetime64.md).

**Example**

``` sql
SELECT changeHour(toDate('1999-01-01'), 14), changeHour(toDateTime64('1999-01-01 00:00:00.000', 3), 14);
```

Result:

```
┌─changeHour(toDate('1999-01-01'), 14)─┬─changeHour(toDateTime64('1999-01-01 00:00:00.000', 3), 14)─┐
│                  1999-01-01 14:00:00 │                                    1999-01-01 14:00:00.000 │
└──────────────────────────────────────┴────────────────────────────────────────────────────────────┘
```

## changeMinute

Changes the minute component of a date or date time.

**Syntax**

``` sql
changeMinute(date_or_datetime, value)
```

**Arguments**

- `date_or_datetime` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)
- `value` - a new value of the minute. [Integer](../../sql-reference/data-types/int-uint.md).

**Returned value**

- Returns a value of same type as `date_or_datetime`. If the input is a [Date](../data-types/date.md), return [DateTime](../data-types/datetime.md). If the input is a [Date32](../data-types/date32.md), return [DateTime64](../data-types/datetime64.md).

**Example**

``` sql
    SELECT changeMinute(toDate('1999-01-01'), 15), changeMinute(toDateTime64('1999-01-01 00:00:00.000', 3), 15);
```

Result:

```
┌─changeMinute(toDate('1999-01-01'), 15)─┬─changeMinute(toDateTime64('1999-01-01 00:00:00.000', 3), 15)─┐
│                    1999-01-01 00:15:00 │                                      1999-01-01 00:15:00.000 │
└────────────────────────────────────────┴──────────────────────────────────────────────────────────────┘
```

## changeSecond

Changes the second component of a date or date time.

**Syntax**

``` sql
changeSecond(date_or_datetime, value)
```

**Arguments**

- `date_or_datetime` - a [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)
- `value` - a new value of the second. [Integer](../../sql-reference/data-types/int-uint.md).

**Returned value**

- Returns a value of same type as `date_or_datetime`. If the input is a [Date](../data-types/date.md), return [DateTime](../data-types/datetime.md). If the input is a [Date32](../data-types/date32.md), return [DateTime64](../data-types/datetime64.md).

**Example**

``` sql
SELECT changeSecond(toDate('1999-01-01'), 15), changeSecond(toDateTime64('1999-01-01 00:00:00.000', 3), 15);
```

Result:

```
┌─changeSecond(toDate('1999-01-01'), 15)─┬─changeSecond(toDateTime64('1999-01-01 00:00:00.000', 3), 15)─┐
│                    1999-01-01 00:00:15 │                                      1999-01-01 00:00:15.000 │
└────────────────────────────────────────┴──────────────────────────────────────────────────────────────┘
```

## addYears

Adds a specified number of years to a date, a date with time or a string-encoded date / date with time.

**Syntax**

```sql
addYears(date, num)
```

**Parameters**

- `date`: Date / date with time to add specified number of years to. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of years to add. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date` plus `num` years. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addYears(date, 1) AS add_years_with_date,
    addYears(date_time, 1) AS add_years_with_date_time,
    addYears(date_time_string, 1) AS add_years_with_date_time_string
```

```response
┌─add_years_with_date─┬─add_years_with_date_time─┬─add_years_with_date_time_string─┐
│          2025-01-01 │      2025-01-01 00:00:00 │         2025-01-01 00:00:00.000 │
└─────────────────────┴──────────────────────────┴─────────────────────────────────┘
```

## addQuarters

Adds a specified number of quarters to a date, a date with time or a string-encoded date / date with time.

**Syntax**

```sql
addQuarters(date, num)
```

**Parameters**

- `date`: Date / date with time to add specified number of quarters to. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of quarters to add. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date` plus `num` quarters. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addQuarters(date, 1) AS add_quarters_with_date,
    addQuarters(date_time, 1) AS add_quarters_with_date_time,
    addQuarters(date_time_string, 1) AS add_quarters_with_date_time_string
```

```response
┌─add_quarters_with_date─┬─add_quarters_with_date_time─┬─add_quarters_with_date_time_string─┐
│             2024-04-01 │         2024-04-01 00:00:00 │            2024-04-01 00:00:00.000 │
└────────────────────────┴─────────────────────────────┴────────────────────────────────────┘
```

## addMonths

Adds a specified number of months to a date, a date with time or a string-encoded date / date with time.

**Syntax**

```sql
addMonths(date, num)
```

**Parameters**

- `date`: Date / date with time to add specified number of months to. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of months to add. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date` plus `num` months. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addMonths(date, 6) AS add_months_with_date,
    addMonths(date_time, 6) AS add_months_with_date_time,
    addMonths(date_time_string, 6) AS add_months_with_date_time_string
```

```response
┌─add_months_with_date─┬─add_months_with_date_time─┬─add_months_with_date_time_string─┐
│           2024-07-01 │       2024-07-01 00:00:00 │          2024-07-01 00:00:00.000 │
└──────────────────────┴───────────────────────────┴──────────────────────────────────┘
```

## addWeeks

Adds a specified number of weeks to a date, a date with time or a string-encoded date / date with time.

**Syntax**

```sql
addWeeks(date, num)
```

**Parameters**

- `date`: Date / date with time to add specified number of weeks to. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of weeks to add. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date` plus `num` weeks. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addWeeks(date, 5) AS add_weeks_with_date,
    addWeeks(date_time, 5) AS add_weeks_with_date_time,
    addWeeks(date_time_string, 5) AS add_weeks_with_date_time_string
```

```response
┌─add_weeks_with_date─┬─add_weeks_with_date_time─┬─add_weeks_with_date_time_string─┐
│          2024-02-05 │      2024-02-05 00:00:00 │         2024-02-05 00:00:00.000 │
└─────────────────────┴──────────────────────────┴─────────────────────────────────┘
```

## addDays

Adds a specified number of days to a date, a date with time or a string-encoded date / date with time.

**Syntax**

```sql
addDays(date, num)
```

**Parameters**

- `date`: Date / date with time to add specified number of days to. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of days to add. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date` plus `num` days. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addDays(date, 5) AS add_days_with_date,
    addDays(date_time, 5) AS add_days_with_date_time,
    addDays(date_time_string, 5) AS add_days_with_date_time_string
```

```response
┌─add_days_with_date─┬─add_days_with_date_time─┬─add_days_with_date_time_string─┐
│         2024-01-06 │     2024-01-06 00:00:00 │        2024-01-06 00:00:00.000 │
└────────────────────┴─────────────────────────┴────────────────────────────────┘
```

## addHours

Adds a specified number of days to a date, a date with time or a string-encoded date / date with time.

**Syntax**

```sql
addHours(date, num)
```

**Parameters**

- `date`: Date / date with time to add specified number of hours to. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of hours to add. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**
o
- Returns `date` plus `num` hours. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addHours(date, 12) AS add_hours_with_date,
    addHours(date_time, 12) AS add_hours_with_date_time,
    addHours(date_time_string, 12) AS add_hours_with_date_time_string
```

```response
┌─add_hours_with_date─┬─add_hours_with_date_time─┬─add_hours_with_date_time_string─┐
│ 2024-01-01 12:00:00 │      2024-01-01 12:00:00 │         2024-01-01 12:00:00.000 │
└─────────────────────┴──────────────────────────┴─────────────────────────────────┘
```

## addMinutes

Adds a specified number of minutes to a date, a date with time or a string-encoded date / date with time.

**Syntax**

```sql
addMinutes(date, num)
```

**Parameters**

- `date`: Date / date with time to add specified number of minutes to. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of minutes to add. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date` plus `num` minutes. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addMinutes(date, 20) AS add_minutes_with_date,
    addMinutes(date_time, 20) AS add_minutes_with_date_time,
    addMinutes(date_time_string, 20) AS add_minutes_with_date_time_string
```

```response
┌─add_minutes_with_date─┬─add_minutes_with_date_time─┬─add_minutes_with_date_time_string─┐
│   2024-01-01 00:20:00 │        2024-01-01 00:20:00 │           2024-01-01 00:20:00.000 │
└───────────────────────┴────────────────────────────┴───────────────────────────────────┘
```

## addSeconds

Adds a specified number of seconds to a date, a date with time or a string-encoded date / date with time.

**Syntax**

```sql
addSeconds(date, num)
```

**Parameters**

- `date`: Date / date with time to add specified number of seconds to. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of seconds to add. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date` plus `num` seconds. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addSeconds(date, 30) AS add_seconds_with_date,
    addSeconds(date_time, 30) AS add_seconds_with_date_time,
    addSeconds(date_time_string, 30) AS add_seconds_with_date_time_string
```

```response
┌─add_seconds_with_date─┬─add_seconds_with_date_time─┬─add_seconds_with_date_time_string─┐
│   2024-01-01 00:00:30 │        2024-01-01 00:00:30 │           2024-01-01 00:00:30.000 │
└───────────────────────┴────────────────────────────┴───────────────────────────────────┘
```

## addMilliseconds

Adds a specified number of milliseconds to a date with time or a string-encoded date with time.

**Syntax**

```sql
addMilliseconds(date_time, num)
```

**Parameters**

- `date_time`: Date with time to add specified number of milliseconds to. [DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of milliseconds to add. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date_time` plus `num` milliseconds. [DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addMilliseconds(date_time, 1000) AS add_milliseconds_with_date_time,
    addMilliseconds(date_time_string, 1000) AS add_milliseconds_with_date_time_string
```

```response
┌─add_milliseconds_with_date_time─┬─add_milliseconds_with_date_time_string─┐
│         2024-01-01 00:00:01.000 │                2024-01-01 00:00:01.000 │
└─────────────────────────────────┴────────────────────────────────────────┘
```

## addMicroseconds

Adds a specified number of microseconds to a date with time or a string-encoded date with time.

**Syntax**

```sql
addMicroseconds(date_time, num)
```

**Parameters**

- `date_time`: Date with time to add specified number of microseconds to. [DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of microseconds to add. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date_time` plus `num` microseconds. [DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addMicroseconds(date_time, 1000000) AS add_microseconds_with_date_time,
    addMicroseconds(date_time_string, 1000000) AS add_microseconds_with_date_time_string
```

```response
┌─add_microseconds_with_date_time─┬─add_microseconds_with_date_time_string─┐
│      2024-01-01 00:00:01.000000 │             2024-01-01 00:00:01.000000 │
└─────────────────────────────────┴────────────────────────────────────────┘
```

## addNanoseconds

Adds a specified number of microseconds to a date with time or a string-encoded date with time.

**Syntax**

```sql
addNanoseconds(date_time, num)
```

**Parameters**

- `date_time`: Date with time to add specified number of nanoseconds to. [DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of nanoseconds to add. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date_time` plus `num` nanoseconds. [DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addNanoseconds(date_time, 1000) AS add_nanoseconds_with_date_time,
    addNanoseconds(date_time_string, 1000) AS add_nanoseconds_with_date_time_string
```

```response
┌─add_nanoseconds_with_date_time─┬─add_nanoseconds_with_date_time_string─┐
│  2024-01-01 00:00:00.000001000 │         2024-01-01 00:00:00.000001000 │
└────────────────────────────────┴───────────────────────────────────────┘
```

## addInterval

Adds an interval to another interval or tuple of intervals.

**Syntax**

```sql
addInterval(interval_1, interval_2)
```

**Parameters**

- `interval_1`: First interval or tuple of intervals. [interval](../data-types/special-data-types/interval.md), [tuple](../data-types/tuple.md)([interval](../data-types/special-data-types/interval.md)).
- `interval_2`: Second interval to be added. [interval](../data-types/special-data-types/interval.md).

**Returned value**

- Returns a tuple of intervals. [tuple](../data-types/tuple.md)([interval](../data-types/special-data-types/interval.md)).

:::note
Intervals of the same type will be combined into a single interval. For instance if `toIntervalDay(1)` and `toIntervalDay(2)` are passed then the result will be `(3)` rather than `(1,1)`.
:::

**Example**

Query:

```sql
SELECT addInterval(INTERVAL 1 DAY, INTERVAL 1 MONTH);
SELECT addInterval((INTERVAL 1 DAY, INTERVAL 1 YEAR), INTERVAL 1 MONTH);
SELECT addInterval(INTERVAL 2 DAY, INTERVAL 1 DAY);
```

Result:

```response
┌─addInterval(toIntervalDay(1), toIntervalMonth(1))─┐
│ (1,1)                                             │
└───────────────────────────────────────────────────┘
┌─addInterval((toIntervalDay(1), toIntervalYear(1)), toIntervalMonth(1))─┐
│ (1,1,1)                                                                │
└────────────────────────────────────────────────────────────────────────┘
┌─addInterval(toIntervalDay(2), toIntervalDay(1))─┐
│ (3)                                             │
└─────────────────────────────────────────────────┘
```

## addTupleOfIntervals

Consecutively adds a tuple of intervals to a Date or a DateTime.

**Syntax**

```sql
addTupleOfIntervals(interval_1, interval_2)
```

**Parameters**

- `date`: First interval or interval of tuples. [date](../data-types/date.md)/[date32](../data-types/date32.md)/[datetime](../data-types/datetime.md)/[datetime64](../data-types/datetime64.md).
- `intervals`: Tuple of intervals to add to `date`. [tuple](../data-types/tuple.md)([interval](../data-types/special-data-types/interval.md)).

**Returned value**

- Returns `date` with added `intervals`. [date](../data-types/date.md)/[date32](../data-types/date32.md)/[datetime](../data-types/datetime.md)/[datetime64](../data-types/datetime64.md).

**Example**

Query:

```sql
WITH toDate('2018-01-01') AS date
SELECT addTupleOfIntervals(date, (INTERVAL 1 DAY, INTERVAL 1 MONTH, INTERVAL 1 YEAR))
```

Result:

```response
┌─addTupleOfIntervals(date, (toIntervalDay(1), toIntervalMonth(1), toIntervalYear(1)))─┐
│                                                                           2019-02-02 │
└──────────────────────────────────────────────────────────────────────────────────────┘
```
## subtractYears

Subtracts a specified number of years from a date, a date with time or a string-encoded date / date with time.

**Syntax**

```sql
subtractYears(date, num)
```

**Parameters**

- `date`: Date / date with time to subtract specified number of years from. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of years to subtract. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date` minus `num` years. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractYears(date, 1) AS subtract_years_with_date,
    subtractYears(date_time, 1) AS subtract_years_with_date_time,
    subtractYears(date_time_string, 1) AS subtract_years_with_date_time_string
```

```response
┌─subtract_years_with_date─┬─subtract_years_with_date_time─┬─subtract_years_with_date_time_string─┐
│               2023-01-01 │           2023-01-01 00:00:00 │              2023-01-01 00:00:00.000 │
└──────────────────────────┴───────────────────────────────┴──────────────────────────────────────┘
```

## subtractQuarters

Subtracts a specified number of quarters from a date, a date with time or a string-encoded date / date with time.

**Syntax**

```sql
subtractQuarters(date, num)
```

**Parameters**

- `date`: Date / date with time to subtract specified number of quarters from. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of quarters to subtract. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date` minus `num` quarters. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractQuarters(date, 1) AS subtract_quarters_with_date,
    subtractQuarters(date_time, 1) AS subtract_quarters_with_date_time,
    subtractQuarters(date_time_string, 1) AS subtract_quarters_with_date_time_string
```

```response
┌─subtract_quarters_with_date─┬─subtract_quarters_with_date_time─┬─subtract_quarters_with_date_time_string─┐
│                  2023-10-01 │              2023-10-01 00:00:00 │                 2023-10-01 00:00:00.000 │
└─────────────────────────────┴──────────────────────────────────┴─────────────────────────────────────────┘
```

## subtractMonths

Subtracts a specified number of months from a date, a date with time or a string-encoded date / date with time.

**Syntax**

```sql
subtractMonths(date, num)
```

**Parameters**

- `date`: Date / date with time to subtract specified number of months from. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of months to subtract. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date` minus `num` months. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractMonths(date, 1) AS subtract_months_with_date,
    subtractMonths(date_time, 1) AS subtract_months_with_date_time,
    subtractMonths(date_time_string, 1) AS subtract_months_with_date_time_string
```

```response
┌─subtract_months_with_date─┬─subtract_months_with_date_time─┬─subtract_months_with_date_time_string─┐
│                2023-12-01 │            2023-12-01 00:00:00 │               2023-12-01 00:00:00.000 │
└───────────────────────────┴────────────────────────────────┴───────────────────────────────────────┘
```

## subtractWeeks

Subtracts a specified number of weeks from a date, a date with time or a string-encoded date / date with time.

**Syntax**

```sql
subtractWeeks(date, num)
```

**Parameters**

- `date`: Date / date with time to subtract specified number of weeks from. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of weeks to subtract. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date` minus `num` weeks. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractWeeks(date, 1) AS subtract_weeks_with_date,
    subtractWeeks(date_time, 1) AS subtract_weeks_with_date_time,
    subtractWeeks(date_time_string, 1) AS subtract_weeks_with_date_time_string
```

```response
 ┌─subtract_weeks_with_date─┬─subtract_weeks_with_date_time─┬─subtract_weeks_with_date_time_string─┐
 │               2023-12-25 │           2023-12-25 00:00:00 │              2023-12-25 00:00:00.000 │
 └──────────────────────────┴───────────────────────────────┴──────────────────────────────────────┘
```

## subtractDays

Subtracts a specified number of days from a date, a date with time or a string-encoded date / date with time.

**Syntax**

```sql
subtractDays(date, num)
```

**Parameters**

- `date`: Date / date with time to subtract specified number of days from. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of days to subtract. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date` minus `num` days. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractDays(date, 31) AS subtract_days_with_date,
    subtractDays(date_time, 31) AS subtract_days_with_date_time,
    subtractDays(date_time_string, 31) AS subtract_days_with_date_time_string
```

```response
┌─subtract_days_with_date─┬─subtract_days_with_date_time─┬─subtract_days_with_date_time_string─┐
│              2023-12-01 │          2023-12-01 00:00:00 │             2023-12-01 00:00:00.000 │
└─────────────────────────┴──────────────────────────────┴─────────────────────────────────────┘
```

## subtractHours

Subtracts a specified number of hours from a date, a date with time or a string-encoded date / date with time.

**Syntax**

```sql
subtractHours(date, num)
```

**Parameters**

- `date`: Date / date with time to subtract specified number of hours from. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[Datetime](../data-types/datetime.md)/[Datetime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of hours to subtract. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date` minus `num` hours. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[Datetime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractHours(date, 12) AS subtract_hours_with_date,
    subtractHours(date_time, 12) AS subtract_hours_with_date_time,
    subtractHours(date_time_string, 12) AS subtract_hours_with_date_time_string
```

```response
┌─subtract_hours_with_date─┬─subtract_hours_with_date_time─┬─subtract_hours_with_date_time_string─┐
│      2023-12-31 12:00:00 │           2023-12-31 12:00:00 │              2023-12-31 12:00:00.000 │
└──────────────────────────┴───────────────────────────────┴──────────────────────────────────────┘
```

## subtractMinutes

Subtracts a specified number of minutes from a date, a date with time or a string-encoded date / date with time.

**Syntax**

```sql
subtractMinutes(date, num)
```

**Parameters**

- `date`: Date / date with time to subtract specified number of minutes from. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of minutes to subtract. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date` minus `num` minutes. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractMinutes(date, 30) AS subtract_minutes_with_date,
    subtractMinutes(date_time, 30) AS subtract_minutes_with_date_time,
    subtractMinutes(date_time_string, 30) AS subtract_minutes_with_date_time_string
```

```response
┌─subtract_minutes_with_date─┬─subtract_minutes_with_date_time─┬─subtract_minutes_with_date_time_string─┐
│        2023-12-31 23:30:00 │             2023-12-31 23:30:00 │                2023-12-31 23:30:00.000 │
└────────────────────────────┴─────────────────────────────────┴────────────────────────────────────────┘
```

## subtractSeconds

Subtracts a specified number of seconds from a date, a date with time or a string-encoded date / date with time.

**Syntax**

```sql
subtractSeconds(date, num)
```

**Parameters**

- `date`: Date / date with time to subtract specified number of seconds from. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of seconds to subtract. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date` minus `num` seconds. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractSeconds(date, 60) AS subtract_seconds_with_date,
    subtractSeconds(date_time, 60) AS subtract_seconds_with_date_time,
    subtractSeconds(date_time_string, 60) AS subtract_seconds_with_date_time_string
```

```response
┌─subtract_seconds_with_date─┬─subtract_seconds_with_date_time─┬─subtract_seconds_with_date_time_string─┐
│        2023-12-31 23:59:00 │             2023-12-31 23:59:00 │                2023-12-31 23:59:00.000 │
└────────────────────────────┴─────────────────────────────────┴────────────────────────────────────────┘
```

## subtractMilliseconds

Subtracts a specified number of milliseconds from a date with time or a string-encoded date with time.

**Syntax**

```sql
subtractMilliseconds(date_time, num)
```

**Parameters**

- `date_time`: Date with time to subtract specified number of milliseconds from. [DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of milliseconds to subtract. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date_time` minus `num` milliseconds. [DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractMilliseconds(date_time, 1000) AS subtract_milliseconds_with_date_time,
    subtractMilliseconds(date_time_string, 1000) AS subtract_milliseconds_with_date_time_string
```

```response
┌─subtract_milliseconds_with_date_time─┬─subtract_milliseconds_with_date_time_string─┐
│              2023-12-31 23:59:59.000 │                     2023-12-31 23:59:59.000 │
└──────────────────────────────────────┴─────────────────────────────────────────────┘
```

## subtractMicroseconds

Subtracts a specified number of microseconds from a date with time or a string-encoded date with time.

**Syntax**

```sql
subtractMicroseconds(date_time, num)
```

**Parameters**

- `date_time`: Date with time to subtract specified number of microseconds from. [DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of microseconds to subtract. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date_time` minus `num` microseconds. [DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractMicroseconds(date_time, 1000000) AS subtract_microseconds_with_date_time,
    subtractMicroseconds(date_time_string, 1000000) AS subtract_microseconds_with_date_time_string
```

```response
┌─subtract_microseconds_with_date_time─┬─subtract_microseconds_with_date_time_string─┐
│           2023-12-31 23:59:59.000000 │                  2023-12-31 23:59:59.000000 │
└──────────────────────────────────────┴─────────────────────────────────────────────┘
```

## subtractNanoseconds

Subtracts a specified number of nanoseconds from a date with time or a string-encoded date with time.

**Syntax**

```sql
subtractNanoseconds(date_time, num)
```

**Parameters**

- `date_time`: Date with time to subtract specified number of nanoseconds from. [DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md), [String](../data-types/string.md).
- `num`: Number of nanoseconds to subtract. [(U)Int*](../data-types/int-uint.md), [Float*](../data-types/float.md).

**Returned value**

- Returns `date_time` minus `num` nanoseconds. [DateTime64](../data-types/datetime64.md).

**Example**

```sql
WITH
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractNanoseconds(date_time, 1000) AS subtract_nanoseconds_with_date_time,
    subtractNanoseconds(date_time_string, 1000) AS subtract_nanoseconds_with_date_time_string
```

```response
┌─subtract_nanoseconds_with_date_time─┬─subtract_nanoseconds_with_date_time_string─┐
│       2023-12-31 23:59:59.999999000 │              2023-12-31 23:59:59.999999000 │
└─────────────────────────────────────┴────────────────────────────────────────────┘
```

## subtractInterval

Adds a negated interval to another interval or tuple of intervals.

**Syntax**

```sql
subtractInterval(interval_1, interval_2)
```

**Parameters**

- `interval_1`: First interval or interval of tuples. [interval](../data-types/special-data-types/interval.md), [tuple](../data-types/tuple.md)([interval](../data-types/special-data-types/interval.md)).
- `interval_2`: Second interval to be negated. [interval](../data-types/special-data-types/interval.md).

**Returned value**

- Returns a tuple of intervals. [tuple](../data-types/tuple.md)([interval](../data-types/special-data-types/interval.md)).

:::note
Intervals of the same type will be combined into a single interval. For instance if `toIntervalDay(2)` and `toIntervalDay(1)` are passed then the result will be `(1)` rather than `(2,1)`
:::

**Example**

Query:

```sql
SELECT subtractInterval(INTERVAL 1 DAY, INTERVAL 1 MONTH);
SELECT subtractInterval((INTERVAL 1 DAY, INTERVAL 1 YEAR), INTERVAL 1 MONTH);
SELECT subtractInterval(INTERVAL 2 DAY, INTERVAL 1 DAY);
```

Result:

```response
┌─subtractInterval(toIntervalDay(1), toIntervalMonth(1))─┐
│ (1,-1)                                                 │
└────────────────────────────────────────────────────────┘
┌─subtractInterval((toIntervalDay(1), toIntervalYear(1)), toIntervalMonth(1))─┐
│ (1,1,-1)                                                                    │
└─────────────────────────────────────────────────────────────────────────────┘
┌─subtractInterval(toIntervalDay(2), toIntervalDay(1))─┐
│ (1)                                                  │
└──────────────────────────────────────────────────────┘
```

## subtractTupleOfIntervals

Consecutively subtracts a tuple of intervals from a Date or a DateTime.

**Syntax**

```sql
subtractTupleOfIntervals(interval_1, interval_2)
```

**Parameters**

- `date`: First interval or interval of tuples. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).
- `intervals`: Tuple of intervals to subtract from `date`. [tuple](../data-types/tuple.md)([interval](../data-types/special-data-types/interval.md)).

**Returned value**

- Returns `date` with subtracted `intervals`. [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md).

**Example**

Query:

```sql
WITH toDate('2018-01-01') AS date SELECT subtractTupleOfIntervals(date, (INTERVAL 1 DAY, INTERVAL 1 YEAR))
```

Result:

```response
┌─subtractTupleOfIntervals(date, (toIntervalDay(1), toIntervalYear(1)))─┐
│                                                            2016-12-31 │
└───────────────────────────────────────────────────────────────────────┘
```

## timeSlots

For a time interval starting at ‘StartTime’ and continuing for ‘Duration’ seconds, it returns an array of moments in time, consisting of points from this interval rounded down to the ‘Size’ in seconds. ‘Size’ is an optional parameter set to 1800 (30 minutes) by default.
This is necessary, for example, when searching for pageviews in the corresponding session.
Accepts DateTime and DateTime64 as ’StartTime’ argument. For DateTime, ’Duration’ and ’Size’ arguments must be `UInt32`. For ’DateTime64’ they must be `Decimal64`.
Returns an array of DateTime/DateTime64 (return type matches the type of ’StartTime’). For DateTime64, the return value's scale can differ from the scale of ’StartTime’ --- the highest scale among all given arguments is taken.

**Syntax**

```sql
timeSlots(StartTime, Duration,\[, Size\])
```

**Example**

```sql
SELECT timeSlots(toDateTime('2012-01-01 12:20:00'), toUInt32(600));
SELECT timeSlots(toDateTime('1980-12-12 21:01:02', 'UTC'), toUInt32(600), 299);
SELECT timeSlots(toDateTime64('1980-12-12 21:01:02.1234', 4, 'UTC'), toDecimal64(600.1, 1), toDecimal64(299, 0));
```

Result:

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

## formatDateTime

Formats a Time according to the given Format string. Format is a constant expression, so you cannot have multiple formats for a single result column.

formatDateTime uses MySQL datetime format style, refer to https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format.

The opposite operation of this function is [parseDateTime](../functions/type-conversion-functions.md#type_conversion_functions-parseDateTime).

Alias: `DATE_FORMAT`.

**Syntax**

``` sql
formatDateTime(Time, Format[, Timezone])
```

**Returned value(s)**

Returns time and date values according to the determined format.

**Replacement fields**

Using replacement fields, you can define a pattern for the resulting string. “Example” column shows formatting result for `2018-01-02 22:33:44`.

| Placeholder | Description                                          | Example    |
|----------|---------------------------------------------------------|------------|
| %a       | abbreviated weekday name (Mon-Sun)                      | Mon        |
| %b       | abbreviated month name (Jan-Dec)                        | Jan        |
| %c       | month as an integer number (01-12), see 'Note 3' below  | 01         |
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
| %k       | hour in 24h format (00-23), see 'Note 3' below          | 14         |
| %l       | hour in 12h format (01-12), see 'Note 3' below          | 09         |
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

Note 3: In ClickHouse versions earlier than v23.11, function `parseDateTime()` required leading zeros for formatters `%c` (month) and `%l`/`%k` (hour), e.g. `07`. In later versions, the leading zero may be omitted, e.g. `7`. The previous behavior can be restored using setting `parsedatetime_parse_without_leading_zeros = 0`. Note that function `formatDateTime()` by default still prints leading zeros for `%c` and `%l`/`%k` to not break existing use cases. This behavior can be changed by setting `formatdatetime_format_without_leading_zeros = 1`.

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

Additionally, the `formatDateTime` function can take a third String argument containing the name of the time zone. Example: `Asia/Istanbul`. In this case, the time is formatted according to the specified time zone.

**Example**

```sql
SELECT
    now() AS ts,
    time_zone,
    formatDateTime(ts, '%T', time_zone) AS str_tz_time
FROM system.time_zones
WHERE time_zone LIKE 'Europe%'
LIMIT 10

┌──────────────────ts─┬─time_zone─────────┬─str_tz_time─┐
│ 2023-09-08 19:13:40 │ Europe/Amsterdam  │ 21:13:40    │
│ 2023-09-08 19:13:40 │ Europe/Andorra    │ 21:13:40    │
│ 2023-09-08 19:13:40 │ Europe/Astrakhan  │ 23:13:40    │
│ 2023-09-08 19:13:40 │ Europe/Athens     │ 22:13:40    │
│ 2023-09-08 19:13:40 │ Europe/Belfast    │ 20:13:40    │
│ 2023-09-08 19:13:40 │ Europe/Belgrade   │ 21:13:40    │
│ 2023-09-08 19:13:40 │ Europe/Berlin     │ 21:13:40    │
│ 2023-09-08 19:13:40 │ Europe/Bratislava │ 21:13:40    │
│ 2023-09-08 19:13:40 │ Europe/Brussels   │ 21:13:40    │
│ 2023-09-08 19:13:40 │ Europe/Bucharest  │ 22:13:40    │
└─────────────────────┴───────────────────┴─────────────┘
```

**See Also**

- [formatDateTimeInJodaSyntax](#formatdatetimeinjodasyntax)

## formatDateTimeInJodaSyntax

Similar to formatDateTime, except that it formats datetime in Joda style instead of MySQL style. Refer to https://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html.

The opposite operation of this function is [parseDateTimeInJodaSyntax](../functions/type-conversion-functions.md#type_conversion_functions-parseDateTimeInJodaSyntax).

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

- `date_part` — Date part. Possible values: 'year', 'quarter', 'month', 'week', 'dayofyear', 'day', 'weekday', 'hour', 'minute', 'second'. [String](../data-types/string.md).
- `date` — Date. [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).
- `timezone` — Timezone. Optional. [String](../data-types/string.md).

**Returned value**

- The specified part of date. [String](../data-types/string.md#string)

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

- `date` — Date or date with time. [Date](../data-types/date.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).

**Returned value**

- The name of the month. [String](../data-types/string.md#string)

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

This function converts a Unix timestamp to a calendar date and a time of a day.

It can be called in two ways:

When given a single argument of type [Integer](../data-types/int-uint.md), it returns a value of type [DateTime](../data-types/datetime.md), i.e. behaves like [toDateTime](../../sql-reference/functions/type-conversion-functions.md#todatetime).

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

When given two or three arguments where the first argument is a value of type [Integer](../data-types/int-uint.md), [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md), the second argument is a constant format string and the third argument is an optional constant time zone string, the function returns a value of type [String](../data-types/string.md#string), i.e. it behaves like [formatDateTime](#formatdatetime). In this case, [MySQL's datetime format style](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format) is used.

**Example:**

```sql
SELECT fromUnixTimestamp(1234334543, '%Y-%m-%d %R:%S') AS DateTime;
```

Result:

```text
┌─DateTime────────────┐
│ 2009-02-11 14:42:23 │
└─────────────────────┘
```

**See Also**

- [fromUnixTimestampInJodaSyntax](#fromunixtimestampinjodasyntax)

## fromUnixTimestampInJodaSyntax

Same as [fromUnixTimestamp](#fromunixtimestamp) but when called in the second way (two or three arguments), the formatting is performed using [Joda style](https://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html) instead of MySQL style.

**Example:**

``` sql
SELECT fromUnixTimestampInJodaSyntax(1234334543, 'yyyy-MM-dd HH:mm:ss', 'UTC') AS DateTime;
```

Result:

```
┌─DateTime────────────┐
│ 2009-02-11 06:42:23 │
└─────────────────────┘
```

## toModifiedJulianDay

Converts a [Proleptic Gregorian calendar](https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar) date in text form `YYYY-MM-DD` to a [Modified Julian Day](https://en.wikipedia.org/wiki/Julian_day#Variants) number in Int32. This function supports date from `0000-01-01` to `9999-12-31`. It raises an exception if the argument cannot be parsed as a date, or the date is invalid.

**Syntax**

``` sql
toModifiedJulianDay(date)
```

**Arguments**

- `date` — Date in text form. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).

**Returned value**

- Modified Julian Day number. [Int32](../data-types/int-uint.md).

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

- `date` — Date in text form. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).

**Returned value**

- Modified Julian Day number. [Nullable(Int32)](../data-types/int-uint.md).

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

Converts a [Modified Julian Day](https://en.wikipedia.org/wiki/Julian_day#Variants) number to a [Proleptic Gregorian calendar](https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar) date in text form `YYYY-MM-DD`. This function supports day number from `-678941` to `2973483` (which represent 0000-01-01 and 9999-12-31 respectively). It raises an exception if the day number is outside of the supported range.

**Syntax**

``` sql
fromModifiedJulianDay(day)
```

**Arguments**

- `day` — Modified Julian Day number. [Any integral types](../data-types/int-uint.md).

**Returned value**

- Date in text form. [String](../data-types/string.md)

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

- `day` — Modified Julian Day number. [Any integral types](../data-types/int-uint.md).

**Returned value**

- Date in text form. [Nullable(String)](../data-types/string.md)

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

- `time_val` — A DateTime/DateTime64 type const value or an expression . [DateTime/DateTime64 types](../data-types/datetime.md)
- `time_zone` — A String type const value or an expression represent the time zone. [String types](../data-types/string.md)

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

- `time_val` — A DateTime/DateTime64 type const value or an expression . [DateTime/DateTime64 types](../data-types/datetime.md)
- `time_zone` — A String type const value or an expression represent the time zone. [String types](../data-types/string.md)

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

## UTCTimestamp

Returns the current date and time at the moment of query analysis. The function is a constant expression.

:::note
This function gives the same result that `now('UTC')` would. It was added only for MySQL support and [`now`](#now) is the preferred usage.
:::

**Syntax**

```sql
UTCTimestamp()
```

Alias: `UTC_timestamp`.

**Returned value**

- Returns the current date and time at the moment of query analysis. [DateTime](../data-types/datetime.md).

**Example**

Query:

```sql
SELECT UTCTimestamp();
```

Result:

```response
┌──────UTCTimestamp()─┐
│ 2024-05-28 08:32:09 │
└─────────────────────┘
```

## timeDiff

Returns the difference between two dates or dates with time values. The difference is calculated in units of seconds. It is same as `dateDiff` and was added only for MySQL support. `dateDiff` is preferred.

**Syntax**

```sql
timeDiff(first_datetime, second_datetime)
```

*Arguments**

- `first_datetime` — A DateTime/DateTime64 type const value or an expression . [DateTime/DateTime64 types](../data-types/datetime.md)
- `second_datetime` — A DateTime/DateTime64 type const value or an expression . [DateTime/DateTime64 types](../data-types/datetime.md)

**Returned value**

The difference between two dates or dates with time values in seconds.

**Example**

Query:

```sql
timeDiff(toDateTime64('1927-01-01 00:00:00', 3), toDate32('1927-01-02'));
```

**Result**:

```response
┌─timeDiff(toDateTime64('1927-01-01 00:00:00', 3), toDate32('1927-01-02'))─┐
│                                                                    86400 │
└──────────────────────────────────────────────────────────────────────────┘
```

## Related content

- Blog: [Working with time series data in ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
