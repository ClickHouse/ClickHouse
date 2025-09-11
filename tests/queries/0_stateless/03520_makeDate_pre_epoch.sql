-- Test for makeDate with pre-epoch dates
-- This tests the fix for the DateTime64 bug where pre-epoch dates with fractional seconds
-- were constructed incorrectly due to DecimalUtils::decimalFromComponents implementation

-- Disable timezone randomization
SET session_timezone='UTC';

SELECT 'makeDate with pre-epoch dates';
SELECT makeDate(1969, 1, 1);
SELECT makeDate(1969, 12, 31);
SELECT makeDate(1900, 1, 1);
SELECT makeDate(1950, 6, 15);

SELECT 'makeDateTime with pre-epoch dates';
SELECT makeDateTime(1969, 1, 1, 0, 0, 0);
SELECT makeDateTime(1969, 12, 31, 23, 59, 59);
SELECT makeDateTime(1900, 1, 1, 12, 30, 45);

SELECT 'makeDateTime64 with pre-epoch dates and fractional seconds';
SELECT makeDateTime64(1969, 1, 1, 0, 0, 0, 468, 3);
SELECT makeDateTime64(1969, 12, 31, 23, 59, 59, 999, 3);
SELECT makeDateTime64(1900, 1, 1, 12, 30, 45, 123, 3);
SELECT makeDateTime64(1950, 6, 15, 6, 30, 0, 500, 3);

SELECT 'makeDateTime64 with different scales';
SELECT makeDateTime64(1969, 1, 1, 0, 0, 0, 5, 1);
SELECT makeDateTime64(1969, 1, 1, 0, 0, 0, 123456, 6);
SELECT makeDateTime64(1969, 1, 1, 0, 0, 0, 123456789, 9);

SELECT 'parseDateTime64BestEffort with pre-epoch dates and fractional seconds';
SELECT parseDateTime64BestEffort('1969-01-01 00:00:00.468', 3, 'UTC');
SELECT parseDateTime64BestEffort('1969-12-31 23:59:59.001', 3, 'UTC');
SELECT parseDateTime64BestEffort('1969-12-31 23:59:59.999', 3, 'UTC');
SELECT parseDateTime64BestEffort('1900-01-01 12:34:56.789', 3, 'UTC');

SELECT 'parseDateTime64BestEffort with different scales';
SELECT parseDateTime64BestEffort('1969-12-31 23:59:58.12', 2, 'UTC');
SELECT parseDateTime64BestEffort('1969-12-31 23:59:58.123456', 6, 'UTC');
SELECT parseDateTime64BestEffort('1969-12-31 23:59:58.123456789', 9, 'UTC');