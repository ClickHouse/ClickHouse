-- Test: ABR engine table selection based on query time range
-- Verifies that ABR correctly chooses the underlying table with the smallest
-- sample interval that contains data for the requested time range.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr_table;
DROP TABLE IF EXISTS underlying_table_1;
DROP TABLE IF EXISTS underlying_table_10;
DROP TABLE IF EXISTS underlying_table_100;

-- Table 1: 1 week retention (sample_interval = 1)
CREATE TABLE underlying_table_1 (
    _sample_interval UInt64 DEFAULT 1,
    ts DateTime,
    value UInt64
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/underlying_table_1', '1')
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- Table 2: 1 month retention (sample_interval = 10)
CREATE TABLE underlying_table_10 (
    _sample_interval UInt64 DEFAULT 10,
    ts DateTime,
    value UInt64
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/underlying_table_10', '1')
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- Table 3: 1 year retention (sample_interval = 100)
CREATE TABLE underlying_table_100 (
    _sample_interval UInt64 DEFAULT 100,
    ts DateTime,
    value UInt64
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/underlying_table_100', '1')
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- Insert 1 week of hourly data into underlying_table_1
INSERT INTO underlying_table_1
SELECT
    1,
    toDateTime('2025-11-11 12:00:00') - INTERVAL number HOUR,
    1
FROM system.numbers
LIMIT (24 * 7) + 1;

-- Insert 1 month of hourly data into underlying_table_10
INSERT INTO underlying_table_10
SELECT
    10,
    toDateTime('2025-11-11 12:00:00') - INTERVAL number HOUR,
    10
FROM system.numbers
LIMIT (24 * 31) + 1;

-- Insert 1 year of hourly data into underlying_table_100
INSERT INTO underlying_table_100
SELECT
    100,
    toDateTime('2025-11-11 12:00:00') - INTERVAL number HOUR,
    100
FROM system.numbers
LIMIT (24 * 365) + 1;

-- Create the ABR table
CREATE TABLE abr_table
ENGINE = ABR('underlying_table', [1, 10, 100], ('ts'));

-- Test 1: Query for 3 days ago.
-- Expected: Chooses underlying_table_1.
SELECT /* test 05296, query 1 */ count(), max(value) AS source_table FROM abr_table
WHERE ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 3 DAY;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05296, query 1 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT ProfileEvents['ABRChooseBestTableDurationMicroseconds'] > 0 as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05296, query 1 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 2: Query for 2 weeks ago.
-- Expected: Chooses underlying_table_10.
SELECT /* test 05296, query 2 */ count(), max(value) AS source_table FROM abr_table
WHERE ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 2 WEEK;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05296, query 2 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT ProfileEvents['ABRChooseBestTableDurationMicroseconds'] > 0 as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05296, query 2 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;


-- Test 3: Query for 2 months ago.
-- Expected: Chooses underlying_table_100.
SELECT /* test 05296, query 3 */ count(), max(value) AS source_table FROM abr_table
WHERE ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 2 MONTH;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05296, query 3 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT ProfileEvents['ABRChooseBestTableDurationMicroseconds'] > 0 as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05296, query 3 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 4: Query for 2 years ago.
-- Expected: Chooses underlying_table_100 as well
SELECT /* test 05296, query 4 */ count(), max(value) AS source_table FROM abr_table
WHERE ts >= toDateTime('2025-11-11 12:00:00') - INTERVAL 2 YEAR;

SYSTEM FLUSH LOGS query_log;
SELECT abr_interval FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05296, query 4 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;
SELECT ProfileEvents['ABRChooseBestTableDurationMicroseconds'] > 0 as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%SELECT /* test 05296, query 4 */%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

-- Test 5: Query with no lower bound on ts → QUERY_NOT_ALLOWED
SELECT /* test 05296, query 5 */ count(), max(value) AS source_table FROM abr_table
WHERE ts <= toDateTime('2025-11-11 12:00:00') - INTERVAL 2 YEAR; -- {serverError QUERY_NOT_ALLOWED}

-- Test 6: Query with no lower bound on ts → QUERY_NOT_ALLOWED
SELECT /* test 05296, query 6 */ count(), max(value) AS source_table FROM abr_table
WHERE ts < toDateTime('2025-11-03 12:00:00'); -- {serverError QUERY_NOT_ALLOWED}

DROP TABLE IF EXISTS abr_table;
DROP TABLE IF EXISTS underlying_table_1;
DROP TABLE IF EXISTS underlying_table_10;
DROP TABLE IF EXISTS underlying_table_100;