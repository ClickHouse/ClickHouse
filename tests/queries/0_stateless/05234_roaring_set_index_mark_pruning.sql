-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ

SET explain_query_plan_default = 'legacy';

-- Test MergeTreeSetIndex Roaring Bitmap fast path for single integer key mark pruning.
DROP TABLE IF EXISTS t_roaring_index;
DROP TABLE IF EXISTS t_roaring_compound;
DROP TABLE IF EXISTS t_roaring_index_u32;
DROP TABLE IF EXISTS t_roaring_transformed;

CREATE TABLE t_roaring_index (id UInt64, value Float64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_roaring_index SELECT number, number * 1.5 FROM numbers(100);

SELECT '--- Basic IN subset ---';
SELECT id FROM t_roaring_index WHERE id IN (SELECT number FROM numbers(100) WHERE number % 20 == 0) ORDER BY id;

SELECT '--- Point lookup via IN ---';
SELECT id, value FROM t_roaring_index WHERE id IN (SELECT 42::UInt64);

SELECT '--- Narrow range in set ---';
SELECT count() FROM t_roaring_index WHERE id IN (SELECT number FROM numbers(100) WHERE number BETWEEN 10 AND 12);

SELECT '--- Single element NOT IN ---';
SELECT id FROM t_roaring_index WHERE id NOT IN (SELECT number FROM numbers(100) WHERE number != 50) ORDER BY id;

SELECT '--- Transformed key: intDiv(id, 10) IN (2, 4) ---';
SELECT id FROM t_roaring_index WHERE intDiv(id, 10) IN (2, 4) ORDER BY id;

SELECT '--- UInt32 key column ---';
CREATE TABLE t_roaring_index_u32 (id UInt32, value Float64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;
INSERT INTO t_roaring_index_u32 SELECT number, number * 2.0 FROM numbers(50);

SELECT id FROM t_roaring_index_u32 WHERE id IN (SELECT toUInt32(number * 10) FROM numbers(5)) ORDER BY id;

SELECT '--- Transformed key: toWeek(dt) and toMonth(dt) IN (...) ---';
CREATE TABLE t_roaring_transformed (dt DateTime, value Float64) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 8;
INSERT INTO t_roaring_transformed SELECT toDateTime('2026-01-01 00:00:00') + interval (number * 3) day, number * 1.0 FROM numbers(50);

SELECT count() FROM t_roaring_transformed WHERE toWeek(dt) IN (1, 3, 5);
SELECT toDate(dt), toWeek(dt) FROM t_roaring_transformed WHERE toWeek(dt) IN (1, 3) ORDER BY dt;
SELECT count() FROM t_roaring_transformed WHERE toMonth(dt) IN (1, 2);

SELECT '--- Transformed key: toDayOfMonth with timezone and Nullable ---';
CREATE TABLE t_roaring_tz (dt Nullable(DateTime('America/New_York')), value Float64) ENGINE = MergeTree ORDER BY dt DESC SETTINGS index_granularity = 8;
INSERT INTO t_roaring_tz SELECT toDateTime('2026-01-01 00:00:00', 'America/New_York') + interval (number * 3) day, number * 1.0 FROM numbers(50);
SELECT count() FROM t_roaring_tz WHERE toDayOfMonth(dt) IN (1, 3);
DROP TABLE t_roaring_tz;

SELECT '--- Compound key: set column is not the leading key column ---';
-- The sparse primary-key decomposition can hand the set index an inverted range for a
-- non-leading key column. An inverted range is empty, so the fast path has to prune the
-- mark exactly like the general path, instead of falling back to "may be true".
CREATE TABLE t_roaring_compound (team_id UInt64, k UInt8, s String)
ENGINE = MergeTree ORDER BY (team_id, k, s) SETTINGS index_granularity = 8192;
INSERT INTO t_roaring_compound SELECT 1, number % 5, toString(number) FROM numbers(200000);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_roaring_compound WHERE team_id = 1 AND has([0, 3], k)) WHERE explain LIKE '%Granules%';
SELECT count() FROM t_roaring_compound WHERE team_id = 1 AND has([0, 3], k) SETTINGS force_primary_key = 1;

DROP TABLE t_roaring_index;
DROP TABLE t_roaring_index_u32;
DROP TABLE t_roaring_transformed;
DROP TABLE t_roaring_compound;
