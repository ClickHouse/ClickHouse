-- Test for ABR engine with complex WHERE conditions
-- This test verifies that the engine correctly extracts the minimum query date
-- from complex filter expressions across different time ranges.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS complex_queries_1;
DROP TABLE IF EXISTS complex_queries_10;
DROP TABLE IF EXISTS complex_queries_100;

-- Table 1: 1 week retention
CREATE TABLE complex_queries_1 (
    _sample_interval UInt64 DEFAULT 1,
    ts DateTime,
    category String,
    value UInt64
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/complex_queries_1', '1')
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- Table 2: 1 month retention
CREATE TABLE complex_queries_10 (
    _sample_interval UInt64 DEFAULT 10,
    ts DateTime,
    category String,
    value UInt64
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/complex_queries_10', '1')
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- Table 3: 1 year retention
CREATE TABLE complex_queries_100 (
    _sample_interval UInt64 DEFAULT 100,
    ts DateTime,
    category String,
    value UInt64
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/complex_queries_100', '1')
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- Insert 1 week of hourly data into complex_queries_1
INSERT INTO complex_queries_1
SELECT
    1,
    toDateTime('2025-11-11 12:00:00') - INTERVAL number HOUR,
    if(number % 2 = 0, 'A', 'B'),
    1
FROM system.numbers
LIMIT (24 * 7) + 1;

-- Insert 1 month of hourly data into complex_queries_10
INSERT INTO complex_queries_10
SELECT
    10,
    toDateTime('2025-11-11 12:00:00') - INTERVAL number HOUR,
    if(number % 2 = 0, 'A', 'B'),
    10
FROM system.numbers
LIMIT (24 * 31) + 1;

-- Insert 1 year of hourly data into complex_queries_100
INSERT INTO complex_queries_100
SELECT
    100,
    toDateTime('2025-11-11 12:00:00') - INTERVAL number HOUR,
    if(number % 2 = 0, 'A', 'B'),
    100
FROM system.numbers
LIMIT (24 * 365) + 1;

-- Create the ABR table
CREATE TABLE abr
ENGINE = ABR('complex_queries', [1, 10, 100], ('ts'));

-- Test 1: OR condition with two recent time ranges
-- Query: (ts >= now - 2 days) OR (ts >= now - 3 days)
-- Minimum is: now - 3 days
-- Expected: Chooses complex_queries_1 (3 days is within 1 week)
SELECT /* test 05298, query 1 */ count(), max(value) AS source_table FROM abr
WHERE (ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 2 DAY)
   OR (ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 3 DAY);

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05298, query 1 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 2: OR condition with mixed time ranges
-- Query: (ts >= now - 5 days) OR (ts >= now - 20 days)
-- Minimum is: now - 20 days (only complex_queries_10 and complex_queries_100 have this data)
-- Expected: Chooses complex_queries_10 (20 days is within 1 month)
SELECT /* test 05298, query 2 */ count(), max(value) AS source_table FROM abr
WHERE (ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 5 DAY)
   OR (ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 20 DAY);

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05298, query 2 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 3: OR condition with very old time range
-- Query: (ts >= now - 2 days) OR (ts >= now - 200 days)
-- Minimum is: now - 200 days (only complex_queries_100 has this data)
-- Expected: Chooses complex_queries_100
SELECT /* test 05298, query 3 */ count(), max(value) AS source_table FROM abr
WHERE (ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 2 DAY)
   OR (ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 200 DAY);

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05298, query 3 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 4: Three-way OR with different time ranges
-- Query: (ts >= now - 1 day) OR (ts >= now - 10 days) OR (ts >= now - 100 days)
-- Minimum is: now - 100 days (only complex_queries_100 has this data)
-- Expected: Chooses complex_queries_100
SELECT /* test 05298, query 4 */ count(), max(value) AS source_table FROM abr
WHERE (ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 1 DAY)
   OR (ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 10 DAY)
   OR (ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 100 DAY);

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05298, query 4 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 5: OR condition with non-time filters that don't involve timestamp
-- Query: (category = 'A') OR (ts >= now - 3 days)
-- The (category = 'A') branch has no time constraint, so the OR is unbounded on ts.
-- Expected: QUERY_NOT_ALLOWED (no finite lower bound on ts)
SELECT /* test 05298, query 5 */ count(), max(value) AS source_table FROM abr
WHERE (category = 'A')
   OR (ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 3 DAY); -- { serverError QUERY_NOT_ALLOWED }

-- Test 6: OR with multiple unknown (non-time) columns
-- Both (category = 'A') and (value > 50) have no condition on ts,
-- so the OR is unbounded on ts.
-- Expected: QUERY_NOT_ALLOWED (no finite lower bound on ts)
SELECT /* test 05298, query 6 */ count(), max(value) AS source_table FROM abr
WHERE (category = 'A')
   OR (ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 3 DAY)
   OR (value > 50); -- { serverError QUERY_NOT_ALLOWED }

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS complex_queries_1;
DROP TABLE IF EXISTS complex_queries_10;
DROP TABLE IF EXISTS complex_queries_100;