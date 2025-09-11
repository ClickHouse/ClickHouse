-- Test for pre-epoch dates with fractional seconds
-- This tests the fix for the DateTime64 bug where pre-epoch dates with fractional seconds
-- were parsed incorrectly due to DecimalUtils::decimalFromComponents implementation

-- Disable timezone randomization
SET session_timezone='UTC';

SELECT 'changeYear with pre-epoch DateTime64';
SELECT changeYear(toDateTime64('1969-01-01 00:00:00.468', 3), 1970);
SELECT changeYear(toDateTime64('1969-01-01 00:00:00.468', 3), 1968);
SELECT changeYear(toDateTime64('1969-12-31 23:59:59.999', 3), 1970);
SELECT changeYear(toDateTime64('1969-12-31 23:59:59.001', 3), 1968);

SELECT 'changeMonth with pre-epoch DateTime64';
SELECT changeMonth(toDateTime64('1969-01-01 00:00:00.468', 3), 12);
SELECT changeMonth(toDateTime64('1969-12-31 23:59:59.999', 3), 1);
SELECT changeMonth(toDateTime64('1969-06-15 12:30:45.123', 3), 2);

SELECT 'changeDay with pre-epoch DateTime64';
SELECT changeDay(toDateTime64('1969-01-01 00:00:00.468', 3), 31);
SELECT changeDay(toDateTime64('1969-12-31 23:59:59.999', 3), 1);
SELECT changeDay(toDateTime64('1969-06-15 12:30:45.123', 3), 28);

SELECT 'changeHour with pre-epoch DateTime64';
SELECT changeHour(toDateTime64('1969-01-01 00:00:00.468', 3), 23);
SELECT changeHour(toDateTime64('1969-12-31 23:59:59.999', 3), 0);
SELECT changeHour(toDateTime64('1969-06-15 12:30:45.123', 3), 6);

SELECT 'changeMinute with pre-epoch DateTime64';
SELECT changeMinute(toDateTime64('1969-01-01 00:00:00.468', 3), 59);
SELECT changeMinute(toDateTime64('1969-12-31 23:59:59.999', 3), 0);
SELECT changeMinute(toDateTime64('1969-06-15 12:30:45.123', 3), 15);

SELECT 'changeSecond with pre-epoch DateTime64';
SELECT changeSecond(toDateTime64('1969-01-01 00:00:00.468', 3), 59);
SELECT changeSecond(toDateTime64('1969-12-31 23:59:59.999', 3), 0);
SELECT changeSecond(toDateTime64('1969-06-15 12:30:45.123', 3), 30);

SELECT 'Edge cases with different scales';
SELECT changeYear(toDateTime64('1969-01-01 00:00:00.1', 1), 1970);
SELECT changeYear(toDateTime64('1969-01-01 00:00:00.123456', 6), 1970);
SELECT changeYear(toDateTime64('1969-01-01 00:00:00.123456789', 9), 1970);

SELECT 'Verify fractional seconds are preserved correctly';
SELECT changeYear(toDateTime64('1969-01-01 00:00:00.468', 3), 2024);
SELECT changeMonth(toDateTime64('1969-06-15 12:30:45.123', 3), 12);
SELECT changeDay(toDateTime64('1969-12-01 23:59:59.999', 3), 15);