-- Test: ABR engine fallback when max_rows_to_read limit is exceeded
-- Verifies that ABR falls back to a table with fewer rows when the initial
-- table would exceed the max_rows_to_read setting.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS row_limits_1;
DROP TABLE IF EXISTS row_limits_10;

-- Table 1: Higher resolution, more rows (1000 rows)
CREATE TABLE row_limits_1 (
    _sample_interval UInt64 DEFAULT 1,
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- Table 2: Lower resolution, fewer rows (500 rows)
CREATE TABLE row_limits_10 (
    _sample_interval UInt64 DEFAULT 10,
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- Insert 1000 rows into row_limits_1
INSERT INTO row_limits_1
SELECT
    1,
    toDateTime('2025-11-11 12:00:00') - INTERVAL number HOUR,
    1
FROM system.numbers
LIMIT 1000;

-- Insert 500 rows into row_limits_10
INSERT INTO row_limits_10
SELECT
    10,
    toDateTime('2025-11-11 12:00:00') - INTERVAL number * 10 HOUR,
    10
FROM system.numbers
LIMIT 500;

-- Create the ABR table
CREATE TABLE abr
ENGINE = ABR('row_limits', [1, 10], ('ts'));

-- The read limits are the budget of the whole query, split equally between the candidate tables, so with
-- two candidate tables each attempt gets half of max_rows_to_read.

-- Test 1: Query with max_rows_to_read = 4000 (2000 per attempt)
-- row_limits_1 has 1000 rows (OK, < 2000)
-- Expected: Chooses row_limits_1
SELECT /* test 05299, query 1 */ count(), max(value) AS source_table FROM abr
WHERE ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 950 HOUR
SETTINGS max_rows_to_read = 4000;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05299, query 1 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 2: Query with max_rows_to_read = 1800 (900 per attempt)
-- row_limits_1 has 1000 rows (> 900, EXCEEDS LIMIT - skip)
-- row_limits_10 has 500 rows, fits
-- Falls back to row_limits_10
-- Expected: Chooses row_limits_10
SELECT /* test 05299, query 3 */ count(), max(value) AS source_table FROM abr
WHERE ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 950 HOUR
SETTINGS max_rows_to_read = 1800, send_logs_level = 'error';

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval, mapApply((k, v) -> (k, errorCodeToName(v)), abr_exception_codes) AS exception_codes FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05299, query 3 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS row_limits_1;
DROP TABLE IF EXISTS row_limits_10;
