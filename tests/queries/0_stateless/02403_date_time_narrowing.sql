-- check conversion of numbers to date/time --
SELECT toDate(toInt32(toDate32('1930-01-01', 'UTC')), 'UTC'),
       toDate(toInt32(toDate32('2151-01-01', 'UTC')), 'UTC'),
       toDate(toInt64(toDateTime64('1930-01-01 12:12:12.123', 3, 'UTC')), 'UTC'),
       toDate(toInt64(toDateTime64('2151-01-01 12:12:12.123', 3, 'UTC')), 'UTC'),
       toDate32(toInt32(toDate32('1900-01-01', 'UTC')) - 1, 'UTC'),
       toDate32(toInt32(toDate32('2299-12-31', 'UTC')) + 1, 'UTC'),
       toDateTime(toInt64(toDateTime64('1930-01-01 12:12:12.123', 3, 'UTC')), 'UTC'),
       toDateTime(toInt64(toDateTime64('2151-01-01 12:12:12.123', 3, 'UTC')), 'UTC');

-- check conversion of extended range type to normal range type --
SELECT toDate(toDate32('1930-01-01', 'UTC'), 'UTC'),
       toDate(toDate32('2151-01-01', 'UTC'), 'UTC');

SELECT toDate(toDateTime64('1930-01-01 12:12:12.12', 3, 'UTC'), 'UTC'),
       toDate(toDateTime64('2151-01-01 12:12:12.12', 3, 'UTC'), 'UTC');

SELECT toDateTime(toDateTime64('1930-01-01 12:12:12.12', 3, 'UTC'), 'UTC'),
       toDateTime(toDateTime64('2151-01-01 12:12:12.12', 3, 'UTC'), 'UTC');

SELECT toDateTime(toDate32('1930-01-01', 'UTC'), 'UTC'),
       toDateTime(toDate32('2151-01-01', 'UTC'), 'UTC');

SELECT toDateTime(toDate('2141-01-01', 'UTC'), 'UTC');

-- test DateTimeTransforms --
SELECT 'toStartOfDay';
SELECT toStartOfDay(toDate('2141-01-01', 'UTC'), 'UTC'),
       toStartOfDay(toDate32('1930-01-01', 'UTC'), 'UTC'),
       toStartOfDay(toDate32('2141-01-01', 'UTC'), 'UTC'),
       toStartOfDay(toDateTime64('1930-01-01 12:12:12.123', 3, 'UTC'), 'UTC'),
       toStartOfDay(toDateTime64('2141-01-01 12:12:12.123', 3, 'UTC'), 'UTC');

SELECT 'toStartOfWeek';
SELECT toStartOfWeek(toDate('1970-01-01', 'UTC')),
       toStartOfWeek(toDate32('1970-01-01', 'UTC')),
       toStartOfWeek(toDateTime('1970-01-01 10:10:10', 'UTC'), 0, 'UTC'),
       toStartOfWeek(toDateTime64('1970-01-01 10:10:10.123', 3, 'UTC'), 1, 'UTC'),
       toStartOfWeek(toDate32('1930-01-01', 'UTC')),
       toStartOfWeek(toDate32('2151-01-01', 'UTC')),
       toStartOfWeek(toDateTime64('1930-01-01 12:12:12.123', 3, 'UTC'), 2, 'UTC'),
       toStartOfWeek(toDateTime64('2151-01-01 12:12:12.123', 3, 'UTC'), 3, 'UTC');

SELECT 'toMonday';
SELECT toMonday(toDate('1970-01-02', 'UTC')),
       toMonday(toDate32('1930-01-01', 'UTC')),
       toMonday(toDate32('2151-01-01', 'UTC')),
       toMonday(toDateTime64('1930-01-01 12:12:12.123', 3, 'UTC'), 'UTC'),
       toMonday(toDateTime64('2151-01-01 12:12:12.123', 3, 'UTC'), 'UTC');

SELECT 'toStartOfMonth';
SELECT toStartOfMonth(toDate32('1930-01-01', 'UTC')),
       toStartOfMonth(toDate32('2151-01-01', 'UTC')),
       toStartOfMonth(toDateTime64('1930-01-01 12:12:12.123', 3, 'UTC'), 'UTC'),
       toStartOfMonth(toDateTime64('2151-01-01 12:12:12.123', 3, 'UTC'), 'UTC');

SELECT 'toLastDayOfMonth';
SELECT toLastDayOfMonth(toDate('2149-06-03', 'UTC')),
       toLastDayOfMonth(toDate32('1930-01-01', 'UTC')),
       toLastDayOfMonth(toDate32('2151-01-01', 'UTC')),
       toLastDayOfMonth(toDateTime64('1930-01-01 12:12:12.123', 3, 'UTC'), 'UTC'),
       toLastDayOfMonth(toDateTime64('2151-01-01 12:12:12.123', 3, 'UTC'), 'UTC');

SELECT 'toStartOfQuarter';
SELECT toStartOfQuarter(toDate32('1930-01-01', 'UTC')),
       toStartOfQuarter(toDate32('2151-01-01', 'UTC')),
       toStartOfQuarter(toDateTime64('1930-01-01 12:12:12.123', 3, 'UTC'), 'UTC'),
       toStartOfQuarter(toDateTime64('2151-01-01 12:12:12.123', 3, 'UTC'), 'UTC');

SELECT 'toStartOfYear';
SELECT toStartOfYear(toDate32('1930-01-01', 'UTC')),
       toStartOfYear(toDate32('2151-01-01', 'UTC')),
       toStartOfYear(toDateTime64('1930-01-01 12:12:12.123', 3, 'UTC'), 'UTC'),
       toStartOfYear(toDateTime64('2151-01-01 12:12:12.123', 3, 'UTC'), 'UTC');
