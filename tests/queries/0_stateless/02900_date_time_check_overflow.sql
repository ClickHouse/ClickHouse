-- I hope we will get rid of "ignore" option one day :)
-- It gives unpredictable result, but it is faster than others.
SELECT 'ignore';
SET date_time_overflow_mode = 'ignore';
SELECT toDateTime(toDateTime64('1900-01-01 00:00:00.123', 3, 'UTC'), 'UTC');
SELECT toDateTime(toDateTime64('2299-12-31 23:59:59.999', 3, 'UTC'), 'UTC');

SELECT toDateTime(toDate32('1900-01-01', 'UTC'), 'UTC');
SELECT toDateTime(toDate32('2299-12-31', 'UTC'), 'UTC');

SELECT toDateTime(toDate('2149-06-06', 'UTC'), 'UTC');

SELECT toDate(toDateTime64('1900-01-01 00:00:00.123', 3, 'UTC'), 'UTC');
SELECT toDate(toDateTime64('2149-06-07 00:00:00.123', 3, 'UTC'), 'UTC');
SELECT toDate(toDateTime64('2299-12-31 23:59:59.999', 3, 'UTC'), 'UTC');

SELECT toDate(toDate32('1900-01-01', 'UTC'), 'UTC');
SELECT toDate(toDate32('2299-12-31', 'UTC'), 'UTC');


SELECT 'saturate';
SET date_time_overflow_mode = 'saturate';

SELECT toDateTime(toDateTime64('1900-01-01 00:00:00.123', 3, 'UTC'), 'UTC');
SELECT toDateTime(toDateTime64('2299-12-31 23:59:59.999', 3, 'UTC'), 'UTC');

SELECT toDateTime(toDate32('1900-01-01', 'UTC'), 'UTC');
SELECT toDateTime(toDate32('2299-12-31', 'UTC'), 'UTC');

SELECT toDateTime(toDate('2149-06-06', 'UTC'), 'UTC');

SELECT toDate(toDateTime64('1900-01-01 00:00:00.123', 3, 'UTC'), 'UTC');
SELECT toDate(toDateTime64('2149-06-07 00:00:00.123', 3, 'UTC'), 'UTC');
SELECT toDate(toDateTime64('2299-12-31 23:59:59.999', 3, 'UTC'), 'UTC');

SELECT toDate(toDate32('1900-01-01', 'UTC'), 'UTC');
SELECT toDate(toDate32('2299-12-31', 'UTC'), 'UTC');


SELECT 'No output on `throw`';
SET date_time_overflow_mode = 'throw';
SELECT toDateTime(toDateTime64('1900-01-01 00:00:00.123', 3, 'UTC'), 'UTC'); -- { serverError 707 }
SELECT toDateTime(toDateTime64('2299-12-31 23:59:59.999', 3, 'UTC'), 'UTC'); -- { serverError 707 }
SELECT toDateTime(toDate32('1900-01-01', 'UTC'), 'UTC'); -- { serverError 707 }
SELECT toDateTime(toDate32('2299-12-31', 'UTC'), 'UTC'); -- { serverError 707 }
SELECT toDateTime(toDate('2149-06-06', 'UTC'), 'UTC'); -- { serverError 707 }
SELECT toDate(toDateTime64('1900-01-01 00:00:00.123', 3, 'UTC'), 'UTC'); -- { serverError 707 }
SELECT toDate(toDateTime64('2299-12-31 23:59:59.999', 3, 'UTC'), 'UTC'); -- { serverError 707 }
SELECT toDate(toDate32('1900-01-01', 'UTC'), 'UTC'); -- { serverError 707 }
SELECT toDate(toDate32('2299-12-31', 'UTC'), 'UTC'); -- { serverError 707 }
