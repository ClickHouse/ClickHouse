-- Tags: no-parallel

SET allow_experimental_analyzer = 0, allow_experimental_part_aggregation_cache = 1, optimize_aggregation_in_order = 0, enable_memory_bound_merging_of_aggregation_results = 0;

SYSTEM DROP PART AGGREGATION CACHE;

DROP TABLE IF EXISTS t_part_agg_cache;
CREATE TABLE t_part_agg_cache (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_part_agg_cache VALUES (1, 10), (2, 20), (1, 30);
INSERT INTO t_part_agg_cache VALUES (1, 40), (2, 50);

-- Cache is empty initially.
SELECT count() FROM system.part_aggregation_cache;

-- First query populates cache. k=1: 10+30+40=80, k=2: 20+50=70.
SELECT k, sum(v) FROM t_part_agg_cache GROUP BY k ORDER BY k;

-- Cache is populated.
SELECT count() > 0 FROM system.part_aggregation_cache;

-- Second query uses cache, same result.
SELECT k, sum(v) FROM t_part_agg_cache GROUP BY k ORDER BY k;

-- Insert new part. Cache hit for old parts, miss for new.
INSERT INTO t_part_agg_cache VALUES (1, 100), (2, 200);

-- k=1: 80+100=180, k=2: 70+200=270.
SELECT k, sum(v) FROM t_part_agg_cache GROUP BY k ORDER BY k;

-- SYSTEM DROP clears cache.
SYSTEM DROP PART AGGREGATION CACHE;
SELECT count() FROM system.part_aggregation_cache;

-- WHERE support: k=1: 30+40+100=170, k=2: 20+50+200=270.
SELECT k, sum(v) FROM t_part_agg_cache WHERE v > 10 GROUP BY k ORDER BY k;

SYSTEM DROP PART AGGREGATION CACHE;

-- Test: enable_reads=0 means cache is populated but not used for plan rewrite.
-- Result should be correct (normal aggregation path), and cache should be populated.
SELECT k, sum(v) FROM t_part_agg_cache GROUP BY k ORDER BY k
    SETTINGS enable_reads_from_part_aggregation_cache = 0, enable_writes_to_part_aggregation_cache = 1;
SELECT count() > 0 FROM system.part_aggregation_cache;

SYSTEM DROP PART AGGREGATION CACHE;

-- Test: partial warm cache populates new parts.
-- Populate cache for 3 existing parts.
SELECT k, sum(v) FROM t_part_agg_cache GROUP BY k ORDER BY k;
-- Add a new part.
INSERT INTO t_part_agg_cache VALUES (1, 1000), (2, 2000);
-- Query should populate cache for new part too.
SELECT k, sum(v) FROM t_part_agg_cache GROUP BY k ORDER BY k;
-- All 4 parts should be cached now.
SELECT count() FROM system.part_aggregation_cache;

SYSTEM DROP PART AGGREGATION CACHE;

-- Test: DROP TABLE + CREATE TABLE with same name should not reuse stale cache.
SELECT k, sum(v) FROM t_part_agg_cache GROUP BY k ORDER BY k;
DROP TABLE t_part_agg_cache;
CREATE TABLE t_part_agg_cache (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_part_agg_cache VALUES (1, 999), (2, 888);
-- Should return fresh data, not stale cache.
SELECT k, sum(v) FROM t_part_agg_cache GROUP BY k ORDER BY k;

DROP TABLE t_part_agg_cache;
SYSTEM DROP PART AGGREGATION CACHE;
