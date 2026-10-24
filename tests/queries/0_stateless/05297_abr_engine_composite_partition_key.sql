-- Test for ABR engine with composite partition key (multiple columns)
-- This test verifies that the engine correctly identifies the time column
-- when the PARTITION BY clause contains multiple expressions, where at least
-- one is time-related.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS complex_partition_expression_1;
DROP TABLE IF EXISTS complex_partition_expression_10;
DROP TABLE IF EXISTS complex_partition_expression_100;

-- Table 1: 1 week retention with composite partition (region, time)
CREATE TABLE complex_partition_expression_1 (
    _sample_interval UInt64 DEFAULT 1,
    region String,
    ts DateTime,
    value UInt64
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/complex_partition_expression_1', '1')
PARTITION BY (region, toYYYYMM(ts))
ORDER BY ts;

-- Table 2: 1 month retention with composite partition (region, time)
CREATE TABLE complex_partition_expression_10 (
    _sample_interval UInt64 DEFAULT 10,
    region String,
    ts DateTime,
    value UInt64
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/complex_partition_expression_10', '1')
PARTITION BY (region, toYYYYMM(ts))
ORDER BY ts;

-- Table 3: 1 year retention with composite partition (region, time)
CREATE TABLE complex_partition_expression_100 (
    _sample_interval UInt64 DEFAULT 100,
    region String,
    ts DateTime,
    value UInt64
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/complex_partition_expression_100', '1')
PARTITION BY (region, toYYYYMM(ts))
ORDER BY ts;

-- Insert 1 week of data into complex_partition_expression_1 for region 'us-east'
INSERT INTO complex_partition_expression_1
SELECT
    1,
    'us-east',
    toDateTime('2025-11-11 12:00:00') - INTERVAL number HOUR,
    1
FROM system.numbers
LIMIT (24 * 7) + 1;

-- Insert 1 month of data into complex_partition_expression_10 for region 'us-east'
INSERT INTO complex_partition_expression_10
SELECT
    10,
    'us-east',
    toDateTime('2025-11-11 12:00:00') - INTERVAL number HOUR,
    10
FROM system.numbers
LIMIT (24 * 31) + 1;

-- Insert 1 year of data into complex_partition_expression_100 for region 'us-east'
INSERT INTO complex_partition_expression_100
SELECT
    100,
    'us-east',
    toDateTime('2025-11-11 12:00:00') - INTERVAL number HOUR,
    100
FROM system.numbers
LIMIT (24 * 365) + 1;

-- Create the ABR table with composite partition key
CREATE TABLE abr
ENGINE = ABR('complex_partition_expression', [1, 10, 100], ('ts'));

-- Test 1: Query for recent data (3 days)
-- Expected: Chooses complex_partition_expression_1 (highest granularity that has data)
SELECT /* test 05297, query 1 */ count(), max(value) AS source_table FROM abr
WHERE region = 'us-east' AND ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 3 DAY;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05297, query 1 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 2: Query for older data (2 weeks)
-- Expected: Chooses complex_partition_expression_10
SELECT /* test 05297, query 2 */ count(), max(value) AS source_table FROM abr
WHERE region = 'us-east' AND ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 2 WEEK;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05297, query 2 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 3: Query for very old data (2 months)
-- Expected: Chooses complex_partition_expression_100
SELECT /* test 05297, query 3 */ count(), max(value) AS source_table FROM abr
WHERE region = 'us-east' AND ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 2 MONTH;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05297, query 3 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 4: OR between partition column and time condition
-- region = 'us-east' is unknown to the time KeyCondition; with relax_unknown
-- it widens to (-∞, +∞), so the OR is unbounded on ts.
-- Expected: QUERY_NOT_ALLOWED (no finite lower bound on ts)
SELECT /* test 05297, query 4 */ count(), max(value) AS source_table FROM abr
WHERE region = 'us-east' OR ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 3 DAY; -- { serverError QUERY_NOT_ALLOWED }

-- Test 5: AND+OR mix — one branch has a time bound, the other is unknown
-- Branch 1: (region = 'us-east' AND ts >= ...) has a finite lower bound.
-- Branch 2: (value > 50) is unknown → (-∞, +∞).
-- OR of the two branches → unbounded on ts.
-- Expected: QUERY_NOT_ALLOWED (no finite lower bound on ts)
SELECT /* test 05297, query 5 */ count(), max(value) AS source_table FROM abr
WHERE (region = 'us-east' AND ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 3 DAY)
   OR (value > 50); -- { serverError QUERY_NOT_ALLOWED }

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS complex_partition_expression_1;
DROP TABLE IF EXISTS complex_partition_expression_10;
DROP TABLE IF EXISTS complex_partition_expression_100;