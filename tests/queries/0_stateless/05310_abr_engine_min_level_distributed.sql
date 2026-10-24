-- Tags: shard, no-parallel
-- Test: abr_first_table_suffix and abr_last_table_suffix propagation through Distributed table (2 shards).
-- Verifies that both settings set via SETTINGS on the initiator are forwarded
-- to remote shards and each shard independently respects them.
-- Uses test_cluster_two_shards_localhost (2 shards, both localhost:9000).

SET send_logs_level = 'error';
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS dist_abr_minlvl;
DROP TABLE IF EXISTS abr_minlvl;
DROP TABLE IF EXISTS minlvl_1;
DROP TABLE IF EXISTS minlvl_10;
DROP TABLE IF EXISTS minlvl_100;

-- Underlying MergeTree tables with different sample intervals
CREATE TABLE minlvl_1 (
    _sample_interval UInt64 DEFAULT 1,
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

CREATE TABLE minlvl_10 (
    _sample_interval UInt64 DEFAULT 10,
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

CREATE TABLE minlvl_100 (
    _sample_interval UInt64 DEFAULT 100,
    ts DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY ts;

-- 1 week of hourly data in tier 0 (169 rows)
INSERT INTO minlvl_1
SELECT 1, toDateTime('2026-01-08 00:00:00') - INTERVAL number HOUR, 1
FROM system.numbers LIMIT 169;

-- 1 month of hourly data in tier 1 (745 rows)
INSERT INTO minlvl_10
SELECT 10, toDateTime('2026-01-08 00:00:00') - INTERVAL number HOUR, 10
FROM system.numbers LIMIT 745;

-- 1 year of hourly data in tier 2 (8761 rows)
INSERT INTO minlvl_100
SELECT 100, toDateTime('2026-01-08 00:00:00') - INTERVAL number HOUR, 100
FROM system.numbers LIMIT 8761;

CREATE TABLE abr_minlvl
    ENGINE = ABR('minlvl', [1, 10, 100], ('ts'));

CREATE TABLE dist_abr_minlvl AS abr_minlvl
    ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), 'abr_minlvl', rand());

-- ============================================================================
-- Test 1: abr_first_table_suffix = 0 (default), 3-day query → table_1 (value=1)
-- Local count = 73, distributed = 2 * 73 = 146
-- ============================================================================
SELECT 'test 1: default min_level through distributed';
SELECT count(), max(value) AS source_table
FROM dist_abr_minlvl
WHERE ts >= toDateTime('2026-01-08 00:00:00') - INTERVAL 3 DAY
SETTINGS allow_experimental_analyzer = 0;

-- ============================================================================
-- Test 2: abr_first_table_suffix = 10, same 3-day query → skips table_1, picks table_10 (value=10)
-- Local count = 73, distributed = 2 * 73 = 146
-- ============================================================================
SELECT 'test 2: first_table_suffix=10 through distributed';
SELECT count(), max(value) AS source_table
FROM dist_abr_minlvl
WHERE ts >= toDateTime('2026-01-08 00:00:00') - INTERVAL 3 DAY
SETTINGS abr_first_table_suffix = 10, allow_experimental_analyzer = 0;

-- ============================================================================
-- Test 3: abr_first_table_suffix = 100, same 3-day query → skips table_1 and table_10, picks table_100 (value=100)
-- Local count = 73, distributed = 2 * 73 = 146
-- ============================================================================
SELECT 'test 3: first_table_suffix=100 through distributed';
SELECT count(), max(value) AS source_table
FROM dist_abr_minlvl
WHERE ts >= toDateTime('2026-01-08 00:00:00') - INTERVAL 3 DAY
SETTINGS abr_first_table_suffix = 100, allow_experimental_analyzer = 0;

-- ============================================================================
-- Test 4: abr_first_table_suffix = 10 with wider range (2 weeks) → table_10 chosen naturally (value=10)
-- Local count = 337, distributed = 2 * 337 = 674
-- ============================================================================
SELECT 'test 4: first_table_suffix=10 wider range through distributed';
SELECT count(), max(value) AS source_table
FROM dist_abr_minlvl
WHERE ts >= toDateTime('2026-01-08 00:00:00') - INTERVAL 2 WEEK
SETTINGS abr_first_table_suffix = 10, allow_experimental_analyzer = 0;

-- ============================================================================
-- Test 5: abr_first_table_suffix = 10, forced remote execution (prefer_localhost_replica=0)
-- Confirms setting propagation over TCP to remote shards.
-- Same 3-day query → table_10 (value=10), distributed = 2 * 73 = 146
-- ============================================================================
SELECT 'test 5: first_table_suffix=10 remote execution';
SELECT count(), max(value) AS source_table
FROM dist_abr_minlvl
WHERE ts >= toDateTime('2026-01-08 00:00:00') - INTERVAL 3 DAY
SETTINGS abr_first_table_suffix = 10, prefer_localhost_replica = 0, allow_experimental_analyzer = 0;

-- ============================================================================
-- Test 6: sum() aggregation with abr_first_table_suffix = 100 through distributed
-- Verifies aggregation merge works correctly with forced tier.
-- Local sum(value) on table_100 = 73 * 100 = 7300, distributed = 2 * 7300 = 14600
-- ============================================================================
SELECT 'test 6: sum with first_table_suffix=100 through distributed';
SELECT sum(value)
FROM dist_abr_minlvl
WHERE ts >= toDateTime('2026-01-08 00:00:00') - INTERVAL 3 DAY
SETTINGS abr_first_table_suffix = 100, allow_experimental_analyzer = 0;

-- ============================================================================
-- Test 7: abr_last_table_suffix = 10, 2-month query → caps fallback at table_10
-- Normally ABR would pick table_100 (only it covers 2 months), but
-- last_table_suffix=10 prevents that. Falls back to table_10.
-- Local count on table_10 (all 745 rows match) = 745, distributed = 2 * 745 = 1490
-- ============================================================================
SELECT 'test 7: last_table_suffix=10 caps fallback through distributed';
SELECT count(), max(value) AS source_table
FROM dist_abr_minlvl
WHERE ts >= toDateTime('2026-01-08 00:00:00') - INTERVAL 2 MONTH
SETTINGS abr_last_table_suffix = 10, allow_experimental_analyzer = 0;

-- ============================================================================
-- Test 8: abr_last_table_suffix = 1, 2-month query → caps fallback at table_1
-- Falls back to table_1. Local count (all 169 rows match) = 169, distributed = 2 * 169 = 338
-- ============================================================================
SELECT 'test 8: last_table_suffix=1 caps fallback through distributed';
SELECT count(), max(value) AS source_table
FROM dist_abr_minlvl
WHERE ts >= toDateTime('2026-01-08 00:00:00') - INTERVAL 2 MONTH
SETTINGS abr_last_table_suffix = 1, allow_experimental_analyzer = 0;

-- ============================================================================
-- Test 9: abr_last_table_suffix = 10, forced remote execution (prefer_localhost_replica=0)
-- Confirms last_table_suffix propagation over TCP to remote shards.
-- 2-month query → table_10, distributed = 2 * 745 = 1490
-- ============================================================================
SELECT 'test 9: last_table_suffix=10 remote execution';
SELECT count(), max(value) AS source_table
FROM dist_abr_minlvl
WHERE ts >= toDateTime('2026-01-08 00:00:00') - INTERVAL 2 MONTH
SETTINGS abr_last_table_suffix = 10, prefer_localhost_replica = 0, allow_experimental_analyzer = 0;

-- ============================================================================
-- Test 10: both first and last = 10, 3-day query → only table_10 is eligible
-- Distributed = 2 * 73 = 146, max(value) = 10
-- ============================================================================
SELECT 'test 10: first=10 last=10 through distributed';
SELECT count(), max(value) AS source_table
FROM dist_abr_minlvl
WHERE ts >= toDateTime('2026-01-08 00:00:00') - INTERVAL 3 DAY
SETTINGS abr_first_table_suffix = 10, abr_last_table_suffix = 10, allow_experimental_analyzer = 0;

-- Cleanup
DROP TABLE IF EXISTS dist_abr_minlvl;
DROP TABLE IF EXISTS abr_minlvl;
DROP TABLE IF EXISTS minlvl_1;
DROP TABLE IF EXISTS minlvl_10;
DROP TABLE IF EXISTS minlvl_100;
