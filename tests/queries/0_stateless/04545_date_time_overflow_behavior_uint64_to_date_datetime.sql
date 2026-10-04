-- Test that huge UInt64 values (above Int64::max) saturate to the maximum representable value for
-- Date/Date32/DateTime instead of wrapping through a signed intermediate, and that 'throw' still throws.
-- https://github.com/ClickHouse/ClickHouse/issues/100471

SET session_timezone = 'UTC';

SELECT '--- saturate: huge UInt64 (above Int64::max) -> Date/Date32/DateTime ---';
SELECT CAST(9223372036854775808::UInt64, 'Date') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT CAST(9223372036854775808::UInt64, 'Date32') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT CAST(9223372036854775808::UInt64, 'DateTime') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT toDate(9223372036854775808::UInt64) SETTINGS date_time_overflow_behavior = 'saturate';
SELECT toDate32(9223372036854775808::UInt64) SETTINGS date_time_overflow_behavior = 'saturate';
SELECT toDateTime(9223372036854775808::UInt64) SETTINGS date_time_overflow_behavior = 'saturate';

SELECT '--- ignore: huge UInt64 (above Int64::max) -> Date/Date32/DateTime ---';
SELECT CAST(9223372036854775808::UInt64, 'Date') SETTINGS date_time_overflow_behavior = 'ignore';
SELECT CAST(9223372036854775808::UInt64, 'Date32') SETTINGS date_time_overflow_behavior = 'ignore';
SELECT CAST(9223372036854775808::UInt64, 'DateTime') SETTINGS date_time_overflow_behavior = 'ignore';

SELECT '--- throw: huge UInt64 (above Int64::max) -> Date/Date32/DateTime ---';
SELECT CAST(9223372036854775808::UInt64, 'Date') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(9223372036854775808::UInt64, 'Date32') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(9223372036854775808::UInt64, 'DateTime') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '--- UInt32 (narrower than the Date32 bound) must not clamp to a truncated bound ---';
-- Regression: clamping in the unsigned domain must not truncate MAX_DATE32_TIMESTAMP (10413791999)
-- to the width of the source type: static_cast<UInt32>(10413791999) == 1823857407 (2027-10-18),
-- which turned 4294967295 seconds (2106-02-07) into 2027-10-18.
SELECT toDate32(4294967295::UInt32);
SELECT toDate32OrDefault(4294967295::UInt32);
SELECT CAST(4294967295::UInt32, 'Date32') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT CAST(4294967295::UInt32, 'Date32') SETTINGS date_time_overflow_behavior = 'throw';

SELECT '--- saturate: non-constant path via table ---';
DROP TABLE IF EXISTS overflow_date_test;
CREATE TABLE overflow_date_test (u64 UInt64) ENGINE = Memory;
INSERT INTO overflow_date_test VALUES (9223372036854775808), (18446744073709551615);

SELECT CAST(u64, 'Date') FROM overflow_date_test ORDER BY u64 SETTINGS date_time_overflow_behavior = 'saturate';
SELECT CAST(u64, 'Date32') FROM overflow_date_test ORDER BY u64 SETTINGS date_time_overflow_behavior = 'saturate';
SELECT CAST(u64, 'DateTime') FROM overflow_date_test ORDER BY u64 SETTINGS date_time_overflow_behavior = 'saturate';

DROP TABLE overflow_date_test;
