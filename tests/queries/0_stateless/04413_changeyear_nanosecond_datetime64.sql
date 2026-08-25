-- Tags: no-fasttest
-- Test for bug #72019: changeYear + nanosecond DateTime64 (scale=9) causes DECIMAL_OVERFLOW
-- The overflow occurred when computing max_date boundary (year 2299 * 10^9 > Int64 max).

SET session_timezone = 'UTC';

SELECT '-- changeYear with DateTime64 scale=9 (the original bug)';
SELECT changeYear(toDateTime64('2024-01-01 00:00:00.000000000', 9), 2025);
SELECT changeYear(toDateTime64('2024-06-15 12:30:45.123456789', 9), 2000);
SELECT changeYear(toDateTime64('2024-06-15 12:30:45.123456789', 9), 1970);

SELECT '-- changeYear with DateTime64 scale=9, out-of-bounds year (should clamp to max)';
SELECT changeYear(toDateTime64('2024-01-01 00:00:00.000000000', 9), 2500);
SELECT changeYear(toDateTime64('2024-01-01 00:00:00.000000000', 9), -5000);

SELECT '-- changeMonth with DateTime64 scale=9';
SELECT changeMonth(toDateTime64('2024-01-15 10:20:30.123456789', 9), 6);
SELECT changeMonth(toDateTime64('2024-01-15 10:20:30.123456789', 9), 12);
SELECT changeMonth(toDateTime64('2024-01-15 10:20:30.123456789', 9), 0);

SELECT '-- changeDay with DateTime64 scale=9';
SELECT changeDay(toDateTime64('2024-01-01 10:20:30.123456789', 9), 15);
SELECT changeDay(toDateTime64('2024-01-01 10:20:30.123456789', 9), 31);
SELECT changeDay(toDateTime64('2024-01-01 10:20:30.123456789', 9), 0);

SELECT '-- changeHour with DateTime64 scale=9';
SELECT changeHour(toDateTime64('2024-01-01 10:20:30.123456789', 9), 23);
SELECT changeHour(toDateTime64('2024-01-01 10:20:30.123456789', 9), -1);

SELECT '-- changeMinute with DateTime64 scale=9';
SELECT changeMinute(toDateTime64('2024-01-01 10:20:30.123456789', 9), 59);
SELECT changeMinute(toDateTime64('2024-01-01 10:20:30.123456789', 9), -1);

SELECT '-- changeSecond with DateTime64 scale=9';
SELECT changeSecond(toDateTime64('2024-01-01 10:20:30.123456789', 9), 59);
SELECT changeSecond(toDateTime64('2024-01-01 10:20:30.123456789', 9), -1);

SELECT '-- Regression test: scale=3 and scale=6 still work';
SELECT changeYear(toDateTime64('2024-01-01 00:00:00.000', 3), 2025);
SELECT changeYear(toDateTime64('2024-01-01 00:00:00.000000', 6), 2025);

SELECT '-- Regression test: scale=3 and scale=6 out-of-bounds still work';
SELECT changeYear(toDateTime64('2024-01-01 00:00:00.000', 3), 2500);
SELECT changeYear(toDateTime64('2024-01-01 00:00:00.000000', 6), 2500);

SELECT '-- Verify fractional seconds are preserved';
SELECT changeYear(toDateTime64('2024-03-15 08:30:45.999999999', 9), 2023);
SELECT changeMonth(toDateTime64('2024-03-15 08:30:45.999999999', 9), 7);

SELECT '-- In-range year past the representable scale=9 boundary (2262-04-11 23:47:16.854775807) must clamp, not overflow';
SELECT changeYear(toDateTime64('2024-01-01 00:00:00.000000000', 9), 2299);
SELECT changeYear(toDateTime64('2024-01-01 00:00:00.000000000', 9), 2263);
SELECT changeYear(toDateTime64('2024-01-01 00:00:00.000000000', 9), 2262);

SELECT '-- Component change moving a representable scale=9 value past the boundary must clamp, not overflow';
SELECT changeMonth(toDateTime64('2262-01-01 00:00:00.000000000', 9), 12);
SELECT changeDay(toDateTime64('2262-04-01 00:00:00.000000000', 9), 30);
SELECT changeDay(toDateTime64('2262-04-01 00:00:00.000000000', 9), 10);

SELECT '-- Pre-epoch sub-second values: the whole-second part must round toward negative infinity, not truncate toward zero';
SELECT changeYear(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'), 1969);
SELECT changeYear(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'), 1965);
SELECT changeMonth(toDateTime64('1969-06-30 23:59:59.500', 3, 'UTC'), 3);
SELECT changeDay(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'), 15);
SELECT changeHour(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'), 10);
SELECT changeMinute(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'), 45);
SELECT changeSecond(toDateTime64('1969-06-15 12:30:45.500', 3, 'UTC'), 30);

SELECT '-- Pre-epoch sub-second values at scale=9';
SELECT changeYear(toDateTime64('1969-12-31 23:59:59.999999999', 9, 'UTC'), 1969);
SELECT changeSecond(toDateTime64('1969-12-31 23:59:59.999999999', 9, 'UTC'), 0);

SELECT '-- Pre-epoch value exactly on a whole second is unaffected';
SELECT changeYear(toDateTime64('1969-12-31 23:59:59.000', 3, 'UTC'), 1968);
