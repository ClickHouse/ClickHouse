-- Test parseDateTime64BestEffort with pre-epoch dates containing fractional seconds
-- This tests the fix for DateTime64 parsing of pre-epoch dates with fractional parts

-- Original issue case: '1969-01-01 00:00:00.468'
SELECT parseDateTime64BestEffort('1969-01-01 00:00:00.468', 3, 'UTC') AS result1;

-- Additional fractional seconds test cases
SELECT parseDateTime64BestEffort('1969-12-31 23:59:59.001', 3, 'UTC') AS result2;
SELECT parseDateTime64BestEffort('1969-12-31 23:59:59.999', 3, 'UTC') AS result3;
SELECT parseDateTime64BestEffort('1969-12-31 23:59:59.500', 3, 'UTC') AS result4;

-- Test different scales
SELECT parseDateTime64BestEffort('1969-12-31 23:59:58.12', 2, 'UTC') AS result5;
SELECT parseDateTime64BestEffort('1969-12-31 23:59:58.123456', 6, 'UTC') AS result6;
SELECT parseDateTime64BestEffort('1969-12-31 23:59:58.123456789', 9, 'UTC') AS result7;

-- Test dates far from epoch
SELECT parseDateTime64BestEffort('1900-01-01 12:34:56.789', 3, 'UTC') AS result8;

-- Test zero fractional seconds
SELECT parseDateTime64BestEffort('1969-12-31 23:59:59.000', 3, 'UTC') AS result9;

-- Test minimal and maximal fractional parts for scale 3
SELECT parseDateTime64BestEffort('1969-12-31 23:59:59.001', 3, 'UTC') AS result10;
SELECT parseDateTime64BestEffort('1969-12-31 23:59:59.999', 3, 'UTC') AS result11;

-- Verify the results are negative timestamps (pre-epoch)
SELECT parseDateTime64BestEffort('1969-01-01 00:00:00.468', 3, 'UTC') < 0 AS is_negative;
SELECT parseDateTime64BestEffort('1969-12-31 23:59:59.999', 3, 'UTC') < 0 AS is_negative2;