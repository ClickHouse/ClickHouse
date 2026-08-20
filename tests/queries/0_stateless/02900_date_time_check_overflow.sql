SET session_timezone = 'UTC';

SELECT 'ignore';
SET date_time_overflow_behavior = 'ignore';
SELECT toDateTime(toDateTime64('1900-01-01 00:00:00.123', 3));
SELECT toDateTime(toDateTime64('2299-12-31 23:59:59.999', 3));

SELECT toDateTime(toDate32('1900-01-01'));
SELECT toDateTime(toDate32('2299-12-31'));

SELECT toDateTime(toDate('2149-06-06'));

SELECT toDate(toDateTime64('1900-01-01 00:00:00.123', 3));
SELECT toDate(toDateTime64('2149-06-07 00:00:00.123', 3));
SELECT toDate(toDateTime64('2299-12-31 23:59:59.999', 3));

SELECT toDate(toDate32('1900-01-01'));
SELECT toDate(toDate32('2299-12-31'));


SELECT 'No output on `throw`';
SET date_time_overflow_behavior = 'throw';
SELECT toDateTime(toDateTime64('1900-01-01 00:00:00.123', 3)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime(toDateTime64('2299-12-31 23:59:59.999', 3)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime(toDate32('1900-01-01')); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime(toDate32('2299-12-31')); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime(toDate('2149-06-06')); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDate(toDateTime64('1900-01-01 00:00:00.123', 3)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDate(toDateTime64('2299-12-31 23:59:59.999', 3)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDate(toDate32('1900-01-01')); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDate(toDate32('2299-12-31')); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }


SELECT 'saturate';
SET date_time_overflow_behavior = 'saturate';

SELECT toDateTime(toDateTime64('1900-01-01 00:00:00.123', 3));
SELECT toDateTime(toDateTime64('2299-12-31 23:59:59.999', 3));

SELECT toDateTime(toDate32('1900-01-01'));
SELECT toDateTime(toDate32('2299-12-31'));

SELECT toDateTime(toDate('2149-06-06'));

SELECT toDate(toDateTime64('1900-01-01 00:00:00.123', 3));
SELECT toDate(toDateTime64('2149-06-07 00:00:00.123', 3));
SELECT toDate(toDateTime64('2299-12-31 23:59:59.999', 3));

SELECT toDate(toDate32('1900-01-01'));
SELECT toDate(toDate32('2299-12-31'));


-- Test DateTime64 to Date conversion with different timezones
SELECT 'DateTime64 to Date with timezones';
SET date_time_overflow_behavior = 'saturate';

-- Test with UTC
SELECT toDate(toDateTime64('2245-12-31 23:59:59', 0, 'UTC'));
SELECT toDate(toDateTime64('1900-01-01 00:00:00', 0, 'UTC'));

-- Test with Europe/Berlin (UTC+1/+2)
SELECT toDate(toDateTime64('2245-12-31 23:59:59', 0, 'Europe/Berlin'));
SELECT toDate(toDateTime64('1900-01-01 00:00:00', 0, 'Europe/Berlin'));

-- Test with America/New_York (UTC-5/-4)
SELECT toDate(toDateTime64('2245-12-31 23:59:59', 0, 'America/New_York'));
SELECT toDate(toDateTime64('1900-01-01 00:00:00', 0, 'America/New_York'));

-- Test edge cases around max date with timezone
SELECT toDate(toDateTime64('2149-06-06 23:59:59', 0, 'UTC'));
SELECT toDate(toDateTime64('2149-06-07 00:00:00', 0, 'UTC'));
SELECT toDate(toDateTime64('2149-06-06 23:59:59', 0, 'Europe/Berlin'));
SELECT toDate(toDateTime64('2149-06-07 00:00:00', 0, 'Europe/Berlin'));
