# Functions for working with dates and times

Support for time zones

All functions for working with the date and time that have a logical use for the time zone can accept a second optional time zone argument. Example: Asia/Yekaterinburg. In this case, they use the specified time zone instead of the local (default) one.

```sql
SELECT
    toDateTime('2016-06-15 23:00:00') AS time,
    toDate(time) AS date_local,
    toDate(time, 'Asia/Yekaterinburg') AS date_yekat,
    toString(time, 'US/Samoa') AS time_samoa
```

```text
┌────────────────time─┬─date_local─┬─date_yekat─┬─time_samoa──────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-16 │ 2016-06-15 09:00:00 │
└─────────────────────┴────────────┴────────────┴─────────────────────┘
```

Only time zones that differ from UTC by a whole number of hours are supported.

## toYear

Converts a date or date with time to a UInt16 number containing the year number (AD).

## toMonth

Converts a date or date with time to a UInt8 number containing the month number (1-12).

## toDayOfMonth

-Converts a date or date with time to a UInt8 number containing the number of the day of the month (1-31).

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

## toMonday

Rounds down a date or date with time to the nearest Monday.
Returns the date.

## toStartOfMonth

Rounds down a date or date with time to the first day of the month.
Returns the date.

## toStartOfQuarter

Rounds down a date or date with time to the first day of the quarter.
The first day of the quarter is either 1 January, 1 April, 1 July, or 1 October.
Returns the date.

## toStartOfYear

Rounds down a date or date with time to the first day of the year.
Returns the date.

## toStartOfMinute

Rounds down a date with time to the start of the minute.

## toStartOfFiveMinute

Rounds down a date with time to the start of the hour.

Note: If you need to round a date with time to any other number of seconds, minutes, or hours, you can convert it into a number by using the toUInt32 function, then round the number using intDiv and multiplication, and convert it back using the toDateTime function.

## toStartOfHour

Rounds down a date with time to the start of the hour.

## toStartOfDay

Rounds down a date with time to the start of the day.

## toTime

Converts a date with time to a certain fixed date, while preserving the time.

## toRelativeYearNum

Converts a date with time or date to the number of the year, starting from a certain fixed point in the past.

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
This function is specific to Yandex.Metrica, since half an hour is the minimum amount of time for breaking a session into two sessions if a counter shows a single user's consecutive pageviews that differ in time by strictly more than this amount. This means that tuples (the counter number, user ID, and time slot) can be used to search for pageviews that are included in the corresponding session.

## timeSlots(StartTime, Duration)

For a time interval starting at 'StartTime' and continuing for 'Duration' seconds, it returns an array of moments in time, consisting of points from this interval rounded down to the half hour.
For example, `timeSlots(toDateTime('2012-01-01 12:20:00'), 600) = [toDateTime('2012-01-01 12:00:00'), toDateTime('2012-01-01 12:30:00')]`.
This is necessary for searching for pageviews in the corresponding session.

