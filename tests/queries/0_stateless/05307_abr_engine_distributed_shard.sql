-- Tags: shard, no-parallel
-- Test: ABR engine with Distributed table on top (multi-shard query)
-- Verifies that the processed_stage is correctly propagated through ABR
-- so that partial aggregation happens on leaf nodes and merging on root.
-- Uses test_cluster_two_shards_localhost (2 shards, both localhost:9000).

SET send_logs_level = 'error';
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS dist_abr_events;
DROP TABLE IF EXISTS abr_events;
DROP TABLE IF EXISTS events_1;
DROP TABLE IF EXISTS events_10;
DROP TABLE IF EXISTS events_100;

-- Underlying MergeTree tables with different sample intervals (retention tiers)
CREATE TABLE events_1 (
    _sample_interval UInt64 DEFAULT 1,
    namespace UInt64,
    timestamp DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (namespace, timestamp);

CREATE TABLE events_10 (
    _sample_interval UInt64 DEFAULT 10,
    namespace UInt64,
    timestamp DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (namespace, timestamp);

CREATE TABLE events_100 (
    _sample_interval UInt64 DEFAULT 100,
    namespace UInt64,
    timestamp DateTime,
    value UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (namespace, timestamp);

-- Insert 1 week of hourly data into finest tier (169 rows)
INSERT INTO events_1
SELECT
    1,
    200,
    toDateTime('2026-01-08 00:00:00') - INTERVAL number HOUR,
    number
FROM system.numbers
LIMIT 169;

-- Insert 1 month of hourly data into mid tier (745 rows)
INSERT INTO events_10
SELECT
    10,
    200,
    toDateTime('2026-01-08 00:00:00') - INTERVAL number HOUR,
    number
FROM system.numbers
LIMIT 745;

-- Insert 1 year of hourly data into coarsest tier (8761 rows)
INSERT INTO events_100
SELECT
    100,
    200,
    toDateTime('2026-01-08 00:00:00') - INTERVAL number HOUR,
    number
FROM system.numbers
LIMIT 8761;

-- Create the ABR table
CREATE TABLE abr_events
    ENGINE = ABR('events', [1, 10, 100], ('timestamp'));

-- Create a Distributed table on top of the ABR table (2 shards, same server)
CREATE TABLE dist_abr_events AS abr_events
    ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), 'abr_events', rand());

-- ============================================================================
-- Test 1: Simple count(*) through Distributed table with recent time filter
-- The query hits the finest tier (events_1). Local count = 169.
-- Two shards both see the same local data, so distributed count = 2 * 169 = 338.
-- ============================================================================
SELECT 'test 1: count through distributed';
SELECT count(*)
FROM dist_abr_events
WHERE (timestamp >= toDateTime('2026-01-01 00:00:00')) AND (namespace = 200)
SETTINGS allow_experimental_analyzer = 0;

-- ============================================================================
-- Test 2: Distributed count with wider time range (hits mid tier)
-- Local count from events_10 WHERE timestamp >= 2025-12-08 = 745 rows.
-- Distributed = 2 * 745 = 1490.
-- ============================================================================
SELECT 'test 2: count distributed wider range';
SELECT count(*)
FROM dist_abr_events
WHERE (timestamp >= toDateTime('2025-12-08 00:00:00')) AND (namespace = 200)
SETTINGS allow_experimental_analyzer = 0;

-- ============================================================================
-- Test 3: Distributed sum() — tests that aggregation merges correctly
-- sum(value) on events_1 WHERE timestamp >= 2026-01-01 = sum(0..168) = 14196
-- Distributed = 2 * 14196 = 28392
-- ============================================================================
SELECT 'test 3: sum through distributed';
SELECT sum(value)
FROM dist_abr_events
WHERE (timestamp >= toDateTime('2026-01-01 00:00:00')) AND (namespace = 200)
SETTINGS allow_experimental_analyzer = 0;

-- ============================================================================
-- Test 4: Distributed with GROUP BY — partial aggregation on leaves, merge on root
-- Group by namespace, expect one row with namespace=200, count doubled.
-- ============================================================================
SELECT 'test 4: group by through distributed';
SELECT namespace, count(*) AS cnt
FROM dist_abr_events
WHERE (timestamp >= toDateTime('2026-01-01 00:00:00')) AND (namespace = 200)
GROUP BY namespace
SETTINGS allow_experimental_analyzer = 0;

-- ============================================================================
-- Test 5: Distributed max/min — merge of extremes across shards
-- Both shards see same data so max/min are unchanged.
-- ============================================================================
SELECT 'test 5: min/max through distributed';
SELECT
    min(timestamp),
    max(timestamp)
FROM dist_abr_events
WHERE (timestamp >= toDateTime('2026-01-01 00:00:00')) AND (namespace = 200)
SETTINGS allow_experimental_analyzer = 0;

-- ============================================================================
-- Test 6: Non-aggregation query with column aliases through Distributed,
-- forcing remote execution via prefer_localhost_replica=0.
-- When the query goes over TCP, it is serialized as SQL and re-parsed on the
-- remote shard. The Distributed engine requests WithMergeableState, and the
-- ABR engine's buildHeaderBlock may produce a header that mismatches the
-- result_header, causing makeConvertingActions to fail:
-- "Cannot find column `X` in source stream"
-- ============================================================================
SELECT 'test 6: aliases with remote execution through distributed';
SELECT
    timestamp,
    namespace AS ns,
    value AS val
FROM dist_abr_events
WHERE (namespace = 200) AND (timestamp >= toDateTime('2026-01-07 00:00:00'))
ORDER BY timestamp DESC
LIMIT 3
SETTINGS prefer_localhost_replica = 0
SETTINGS allow_experimental_analyzer = 0;

-- ============================================================================
-- Test 7: Aliases used in WHERE with remote execution.
-- The WHERE clause references aliases defined in SELECT.
-- ============================================================================
SELECT 'test 7: aliases in WHERE with remote execution';
SELECT
    timestamp,
    namespace AS ns,
    value AS val
FROM dist_abr_events
WHERE (ns = 200) AND (timestamp >= toDateTime('2026-01-07 00:00:00'))
ORDER BY timestamp DESC
LIMIT 3
SETTINGS prefer_localhost_replica = 0, allow_experimental_analyzer = 0;

-- ============================================================================
-- Test 8: Aliases in SELECT, WHERE filter on alias, ORDER BY alias,
-- with remote execution. Closest to the production query pattern:
-- SELECT blob4 AS userUUID, ... WHERE userUUID = '...' ORDER BY timestamp
-- ============================================================================
SELECT 'test 8: aliases in SELECT + WHERE + ORDER BY with remote execution';
SELECT
    timestamp,
    namespace AS ns,
    value AS val
FROM dist_abr_events
WHERE (ns = 200) AND (val < 5) AND (timestamp >= toDateTime('2026-01-07 20:00:00'))
ORDER BY val ASC
LIMIT 3
SETTINGS prefer_localhost_replica = 0, allow_experimental_analyzer = 0;

-- Cleanup
DROP TABLE IF EXISTS dist_abr_events;
DROP TABLE IF EXISTS abr_events;
DROP TABLE IF EXISTS events_1;
DROP TABLE IF EXISTS events_10;
DROP TABLE IF EXISTS events_100;
