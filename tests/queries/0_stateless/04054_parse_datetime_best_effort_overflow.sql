-- Test that parseDateTimeBestEffort does not overflow on very long fractional parts.
-- The fractional digit count must be clamped to avoid signed integer overflow in readDecimalNumber.
-- https://github.com/ClickHouse/ClickHouse/pull/100368

SET session_timezone = 'UTC';

-- 10-digit unix timestamp with 19-digit fractional part that exceeds Int64 range
SELECT parseDateTime64BestEffort('1234567890.9999999999999999999', 3, 'UTC');

-- 9-digit unix timestamp with 19-digit fractional part that exceeds Int64 range
SELECT parseDateTime64BestEffort('123456789.9999999999999999999', 3, 'UTC');

-- Exactly 18 digits (at the limit, should also work)
SELECT parseDateTime64BestEffort('1234567890.999999999999999999', 3, 'UTC');
SELECT parseDateTime64BestEffort('123456789.999999999999999999', 3, 'UTC');
