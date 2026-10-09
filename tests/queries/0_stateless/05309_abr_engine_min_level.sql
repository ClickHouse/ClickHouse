-- Test: abr_first_table_suffix setting controls which tables are considered by chooseBestTable.
-- The ABR table has intervals [1, 10, 100].
-- Setting abr_first_table_suffix = N skips tables with sample_interval < N.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr_table;
DROP TABLE IF EXISTS underlying_table_1;
DROP TABLE IF EXISTS underlying_table_10;
DROP TABLE IF EXISTS underlying_table_100;

-- Table at level 0 (sample_interval = 1): 1 week of data
CREATE TABLE underlying_table_1 (
    _sample_interval UInt64 DEFAULT 1,
    ts DateTime,
    value UInt64
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/underlying_table_1_minlvl', '1')
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- Table at level 1 (sample_interval = 10): 1 month of data
CREATE TABLE underlying_table_10 (
    _sample_interval UInt64 DEFAULT 10,
    ts DateTime,
    value UInt64
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/underlying_table_10_minlvl', '1')
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- Table at level 2 (sample_interval = 100): 1 year of data
CREATE TABLE underlying_table_100 (
    _sample_interval UInt64 DEFAULT 100,
    ts DateTime,
    value UInt64
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/underlying_table_100_minlvl', '1')
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

INSERT INTO underlying_table_1
SELECT 1, toDateTime('2026-01-01 12:00:00') - INTERVAL number HOUR, 1
FROM system.numbers LIMIT (24 * 7) + 1;

INSERT INTO underlying_table_10
SELECT 10, toDateTime('2026-01-01 12:00:00') - INTERVAL number HOUR, 10
FROM system.numbers LIMIT (24 * 31) + 1;

INSERT INTO underlying_table_100
SELECT 100, toDateTime('2026-01-01 12:00:00') - INTERVAL number HOUR, 100
FROM system.numbers LIMIT (24 * 365) + 1;

CREATE TABLE abr_table
ENGINE = ABR('underlying_table', [1, 10, 100], ('ts'));

-- Test 1: Default abr_first_table_suffix = 0, query 3 days → should pick table_1 (value=1)
SELECT 'test 1';
SELECT count(), max(value) AS source_table FROM abr_table
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 3 DAY;

-- Test 2: abr_first_table_suffix = 10, query 3 days → skips table_1, picks table_10 (value=10)
SELECT 'test 2';
SELECT count(), max(value) AS source_table FROM abr_table
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 3 DAY
SETTINGS abr_first_table_suffix = 10;

-- Test 3: abr_first_table_suffix = 100, query 3 days → skips table_1 and table_10, picks table_100 (value=100)
SELECT 'test 3';
SELECT count(), max(value) AS source_table FROM abr_table
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 3 DAY
SETTINGS abr_first_table_suffix = 100;

-- Test 4: abr_first_table_suffix = 10, query 2 weeks → table_10 would be chosen anyway (value=10)
SELECT 'test 4';
SELECT count(), max(value) AS source_table FROM abr_table
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 2 WEEK
SETTINGS abr_first_table_suffix = 10;

-- Test 5: SET abr_first_table_suffix = 100 for session, query 3 days → picks table_100 (value=100)
SET abr_first_table_suffix = 100;
SELECT 'test 5';
SELECT count(), max(value) AS source_table FROM abr_table
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 3 DAY;

-- Test 6: Still in session with abr_first_table_suffix = 100, query 2 weeks → picks table_100 (value=100)
SELECT 'test 6';
SELECT count(), max(value) AS source_table FROM abr_table
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 2 WEEK;

-- Test 7: Override session SET with SETTINGS clause: abr_first_table_suffix = 0, query 3 days → picks table_1 (value=1)
SELECT 'test 7';
SELECT count(), max(value) AS source_table FROM abr_table
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 3 DAY
SETTINGS abr_first_table_suffix = 0;

-- Reset
SET abr_first_table_suffix = 0;

-- Test 8: abr_first_table_suffix exceeds all intervals → throws BAD_ARGUMENTS
SELECT 'test 8';
SELECT count(), max(value) AS source_table FROM abr_table
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 3 DAY
SETTINGS abr_first_table_suffix = 9999; -- {serverError BAD_ARGUMENTS}

-- Test 9: abr_last_table_suffix = 10, query 2 months → would normally pick table_100 but capped at table_10 (value=10)
SELECT 'test 9';
SELECT count(), max(value) AS source_table FROM abr_table
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 2 MONTH
SETTINGS abr_last_table_suffix = 10;

-- Test 10: abr_last_table_suffix = 1, query 2 months → capped at table_1 (value=1)
SELECT 'test 10';
SELECT count(), max(value) AS source_table FROM abr_table
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 2 MONTH
SETTINGS abr_last_table_suffix = 1;

-- Test 11: both settings combined: first=10, last=10 → only table_10 is eligible (value=10)
SELECT 'test 11';
SELECT count(), max(value) AS source_table FROM abr_table
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 3 DAY
SETTINGS abr_first_table_suffix = 10, abr_last_table_suffix = 10;

-- Test 12: first=10, last=100, query 3 days → skips table_1, picks table_10 (value=10)
SELECT 'test 12';
SELECT count(), max(value) AS source_table FROM abr_table
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 3 DAY
SETTINGS abr_first_table_suffix = 10, abr_last_table_suffix = 100;

-- Test 13: abr_last_table_suffix = 999 (not in intervals) → error
SELECT 'test 13';
SELECT count() FROM abr_table
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 3 DAY
SETTINGS abr_last_table_suffix = 999; -- {serverError BAD_ARGUMENTS}

-- Test 14: abr_first_table_suffix > abr_last_table_suffix → error
SELECT 'test 14';
SELECT count() FROM abr_table
WHERE ts >= toDateTime('2026-01-01 12:00:00') - INTERVAL 3 DAY
SETTINGS abr_first_table_suffix = 100, abr_last_table_suffix = 1; -- {serverError BAD_ARGUMENTS}

DROP TABLE IF EXISTS abr_table;
DROP TABLE IF EXISTS underlying_table_1;
DROP TABLE IF EXISTS underlying_table_10;
DROP TABLE IF EXISTS underlying_table_100;
