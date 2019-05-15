# Functions for working with dates and times

Support for time zones

All functions for working with the date and time that have a logical use for the time zone can accept a second optional time zone argument. Example: Asia/Yekaterinburg. In this case, they use the specified time zone instead of the local (default) one.

``` sql
SELECT
    toDateTime('2016-06-15 23:00:00') AS time,
    toDate(time) AS date_local,
    toDate(time, 'Asia/Yekaterinburg') AS date_yekat,
    toString(time, 'US/Samoa') AS time_samoa
```

```
┌────────────────time─┬─date_local─┬─date_yekat─┬─time_samoa──────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-16 │ 2016-06-15 09:00:00 │
└─────────────────────┴────────────┴────────────┴─────────────────────┘
```

Only time zones that differ from UTC by a whole number of hours are supported.

## toTimeZone

Convert time or date and time to the specified time zone.

## toYear

Converts a date or date with time to a UInt16 number containing the year number (AD).

## toQuarter

Converts a date or date with time to a UInt8 number containing the quarter number.

## toMonth

Converts a date or date with time to a UInt8 number containing the month number (1-12).

## toDayOfYear

Converts a date or date with time to a UInt8 number containing the number of the day of the year (1-366).

## toDayOfMonth

Converts a date or date with time to a UInt8 number containing the number of the day of the month (1-31).

## toDayOfWeek

Converts a date or date with time to a UInt8 number containing the number of the day of the week (Monday is 1, and Sunday is 7).

## toHour

Converts a date with time to a UInt8 number containing the number of the hour in 24-hour time (0-23).
This function assumes that if clocks are moved ahead, it is by one hour and occurs at 2 a.m., and if clocks are moved back, it is by one hour and occurs at 3 a.m. (which is not always true – even in Moscow the clocks were twice changed at a different time).

## toMinute

Converts a date with time to a UInt8 number containing the number of the minute of the hour (0-59).

## toSecond

Converts a date with time to a UInt8 number containing the number of the second in the minute (0-59).
Leap seconds are not accounted for.

## toUnixTimestamp

Converts a date with time to a unix timestamp.

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

!!! attention
    The behavior of parsing incorrect dates is implementation specific. ClickHouse may return zero date, throw an exception or do "natural" overflow.

## toMonday

Rounds down a date or date with time to the nearest Monday.
Returns the date.

## toStartOfDay

Rounds down a date with time to the start of the day.

## toStartOfHour

Rounds down a date with time to the start of the hour.

## toStartOfMinute

Rounds down a date with time to the start of the minute.

## toStartOfFiveMinute

Rounds down a date with time to the start of the five-minute interval.

## toStartOfTenMinutes
Rounds down a date with time to the start of the ten-minute interval.

## toStartOfFifteenMinutes

Rounds down the date with time to the start of the fifteen-minute interval.

## toStartOfInterval(time_or_data, INTERVAL x unit [, time_zone])
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

## now

Accepts zero arguments and returns the current time at one of the moments of request execution.
This function returns a constant, even if the request took a long time to complete.

## today

Accepts zero arguments and returns the current date at one of the moments of request execution.
The same as 'toDate(now())'.

## yesterday

Accepts zero arguments and returns yesterday's date at one of the moments of request execution.
The same as 'today() - 1'.

## timeSlot

Rounds the time to the half hour.
This function is specific to Yandex.Metrica, since half an hour is the minimum amount of time for breaking a session into two sessions if a tracking tag shows a single user's consecutive pageviews that differ in time by strictly more than this amount. This means that tuples (the tag ID, user ID, and time slot) can be used to search for pageviews that are included in the corresponding session.

## toYYYYMM

Converts a date or date with time to a UInt32 number containing the year and month number (YYYY * 100 + MM).

## toYYYYMMDD

Converts a date or date with time to a UInt32 number containing the year and month number (YYYY * 10000 + MM * 100 + DD).

## toYYYYMMDDhhmmss

Converts a date or date with time to a UInt64 number containing the year and month number (YYYY * 10000000000 + MM * 100000000 + DD * 1000000 + hh * 10000 + mm * 100 + ss).

## addYears, addMonths, addWeeks, addDays, addHours, addMinutes, addSeconds, addQuarters

Function adds a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime. For example:

```sql
WITH
    toDate('2018-01-01') AS date,
    toDateTime('2018-01-01 00:00:00') AS date_time
SELECT
    addYears(date, 1) AS add_years_with_date,
    addYears(date_time, 1) AS add_years_with_date_time
```

```
┌─add_years_with_date─┬─add_years_with_date_time─┐
│          2019-01-01 │      2019-01-01 00:00:00 │
└─────────────────────┴──────────────────────────┘
```

## subtractYears, subtractMonths, subtractWeeks, subtractDays, subtractHours, subtractMinutes, subtractSeconds, subtractQuarters

Function subtract a Date/DateTime interval to a Date/DateTime and then return the Date/DateTime. For example:

```sql
WITH
    toDate('2019-01-01') AS date,
    toDateTime('2019-01-01 00:00:00') AS date_time
SELECT
    subtractYears(date, 1) AS subtract_years_with_date,
    subtractYears(date_time, 1) AS subtract_years_with_date_time
```

```
┌─subtract_years_with_date─┬─subtract_years_with_date_time─┐
│               2018-01-01 │           2018-01-01 00:00:00 │
└──────────────────────────┴───────────────────────────────┘
```

## dateDiff('unit', t1, t2, \[timezone\])

Return the difference between two times expressed in 'unit' e.g. `'hours'`. 't1' and 't2' can be Date or DateTime, If 'timezone' is specified, it applied to both arguments. If not, timezones from datatypes 't1' and 't2' are used. If that timezones are not the same, the result is unspecified.

Supported unit values:

| unit   | 
| ------ |
|second  |
|minute  |
|hour    |
|day     |
|week    |
|month   |
|quarter |
|year    |

## timeSlots(StartTime, Duration,\[, Size\])

For a time interval starting at 'StartTime' and continuing for 'Duration' seconds, it returns an array of moments in time, consisting of points from this interval rounded down to the 'Size' in seconds. 'Size' is an optional parameter: a constant UInt32, set to 1800 by default.
For example, `timeSlots(toDateTime('2012-01-01 12:20:00'), 600) = [toDateTime('2012-01-01 12:00:00'), toDateTime('2012-01-01 12:30:00')]`.
This is necessary for searching for pageviews in the corresponding session.

## formatDateTime(Time, Format\[, Timezone\])

Function formats a Time according given Format string. N.B.: Format is a constant expression, e.g. you can not have multiple formats for single result column.

Supported modifiers for Format:
("Example" column shows formatting result for time `2018-01-02 22:33:44`)

| Modifier | Description | Example |
| ----------- | -------- | --------------- |
|%C|year divided by 100 and truncated to integer (00-99)|20
|%d|day of the month, zero-padded (01-31)|02
|%D|Short MM/DD/YY date, equivalent to %m/%d/%y|01/02/2018|
|%e|day of the month, space-padded ( 1-31)|  2|
|%F|short YYYY-MM-DD date, equivalent to %Y-%m-%d|2018-01-02
|%H|hour in 24h format (00-23)|22|
|%I|hour in 12h format (01-12)|10|
|%j|day of the year (001-366)|002|
|%m|month as a decimal number (01-12)|01|
|%M|minute (00-59)|33|
|%n|new-line character ('\n')||
|%p|AM or PM designation|PM|
|%R|24-hour HH:MM time, equivalent to %H:%M|22:33|
|%S|second (00-59)|44|
|%t|horizontal-tab character ('\t')||
|%T|ISO 8601 time format (HH:MM:SS), equivalent to %H:%M:%S|22:33:44|
|%u|ISO 8601 weekday as number with Monday as 1 (1-7)|2|
|%V|ISO 8601 week number (01-53)|01|
|%w|weekday as a decimal number with Sunday as 0 (0-6)|2|
|%y|Year, last two digits (00-99)|18|
|%Y|Year|2018|
|%%|a % sign|%|

[Original article](https://clickhouse.yandex/docs/en/query_language/functions/date_time_functions/) <!--hide-->
