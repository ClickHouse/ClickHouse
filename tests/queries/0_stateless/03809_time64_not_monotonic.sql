-- Regression test for non-monotonic Time64 conversion in MergeTreeSetIndex.
-- Converting from DateTime to Time64 extracts time-of-day component, which is not monotonic.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS mt_test;
DROP TABLE IF EXISTS merge_test;

CREATE TABLE mt_test (d Date DEFAULT toDate('2015-05-01'), x UInt64) ENGINE = MergeTree PARTITION BY d ORDER BY x SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
SET max_block_size = 1000000;
INSERT INTO mt_test (x) SELECT number AS x FROM system.numbers LIMIT 100000;

-- Create a Merge table with Time64 column type
CREATE TABLE merge_test (d Date, x Time64(3)) ENGINE = Merge(currentDatabase(), '^mt_test$');

-- This query previously caused "Invalid binary search result in MergeTreeSetIndex" exception
-- because the conversion from UInt64 to Time64 was incorrectly marked as monotonic.
SELECT count() > 0 FROM merge_test WHERE x NOT IN (12345, 67890);

DROP TABLE merge_test;
DROP TABLE mt_test;

DROP TABLE IF EXISTS mt_datetime_test;

CREATE TABLE mt_datetime_test (ts DateTime, value UInt32) ENGINE = MergeTree ORDER BY ts;

INSERT INTO mt_datetime_test SELECT toDateTime('2024-01-15 00:00:00') + number * 60, number FROM numbers(2880);

-- Same day query (08:00-20:00, filtering 10:00-18:00) = 480 rows
SELECT count() FROM mt_datetime_test
WHERE ts >= '2024-01-15 08:00:00' AND ts < '2024-01-15 20:00:00'
  AND toTime64(ts, 3) >= '10:00:00'::Time64(3) AND toTime64(ts, 3) < '18:00:00'::Time64(3);

-- Cross-day query = 360 rows (correctly computed without monotonicity)
SELECT count() FROM mt_datetime_test
WHERE ts >= '2024-01-15 20:00:00' AND ts < '2024-01-16 08:00:00'
  AND toTime64(ts, 3) >= '06:00:00'::Time64(3);

DROP TABLE mt_datetime_test;

DROP TABLE IF EXISTS mt_datetime64_test;

CREATE TABLE mt_datetime64_test (ts DateTime64(3), value UInt32) ENGINE = MergeTree ORDER BY ts;

INSERT INTO mt_datetime64_test SELECT toDateTime64('2024-01-15 00:00:00', 3) + number * 60, number FROM numbers(2880);

-- Same day query
SELECT count() FROM mt_datetime64_test
WHERE ts >= '2024-01-15 08:00:00' AND ts < '2024-01-15 20:00:00'
  AND toTime64(ts, 3) >= '10:00:00'::Time64(3) AND toTime64(ts, 3) < '18:00:00'::Time64(3);

-- Cross-day query
SELECT count() FROM mt_datetime64_test
WHERE ts >= '2024-01-15 20:00:00' AND ts < '2024-01-16 08:00:00'
  AND toTime64(ts, 3) >= '06:00:00'::Time64(3);

DROP TABLE mt_datetime64_test;

DROP TABLE IF EXISTS mt_uint32_test;

CREATE TABLE mt_uint32_test (ts UInt32, value UInt32) ENGINE = MergeTree ORDER BY ts;

-- Insert Unix timestamps for 2024-01-15 00:00:00 UTC + minutes
INSERT INTO mt_uint32_test SELECT 1705276800 + number * 60, number FROM numbers(2880);

-- Same day query
SELECT count() FROM mt_uint32_test
WHERE ts >= 1705276800 + 8*3600 AND ts < 1705276800 + 20*3600
  AND toTime64(ts, 3) >= '10:00:00'::Time64(3) AND toTime64(ts, 3) < '18:00:00'::Time64(3);

-- Cross-day query
SELECT count() FROM mt_uint32_test
WHERE ts >= 1705276800 + 20*3600 AND ts < 1705276800 + 32*3600
  AND toTime64(ts, 3) >= '06:00:00'::Time64(3);

DROP TABLE mt_uint32_test;

DROP TABLE IF EXISTS mt_int64_test;

DROP TABLE IF EXISTS mt_int64_test;

CREATE TABLE mt_int64_test (ts Int64, value UInt32) ENGINE = MergeTree ORDER BY ts;

INSERT INTO mt_int64_test SELECT 1705276800 + number * 60, number FROM numbers(2880);

-- Same day query
SELECT count() FROM mt_int64_test
WHERE ts >= 1705276800 + 8*3600 AND ts < 1705276800 + 20*3600
  AND toTime64(ts, 3) >= '10:00:00'::Time64(3) AND toTime64(ts, 3) < '18:00:00'::Time64(3);

-- Cross-day query
SELECT count() FROM mt_int64_test
WHERE ts >= 1705276800 + 20*3600 AND ts < 1705276800 + 32*3600
  AND toTime64(ts, 3) >= '06:00:00'::Time64(3);

DROP TABLE mt_int64_test;

DROP TABLE IF EXISTS mt_time_test;

CREATE TABLE mt_time_test (t Time64(3), value UInt32) ENGINE = MergeTree ORDER BY t;

INSERT INTO mt_time_test SELECT toTime64(number * 1000, 3), number FROM numbers(86400);

-- Time64 to Time64 is always monotonic (identity-like)
SELECT count() > 0 FROM mt_time_test WHERE toTime64(t, 6) >= '10:00:00'::Time64(6);

DROP TABLE mt_time_test;
