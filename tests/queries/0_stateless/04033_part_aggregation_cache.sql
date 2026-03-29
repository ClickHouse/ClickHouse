-- Tags: no-parallel
-- Test basic functionality of the part aggregation cache.

SET allow_experimental_analyzer = 0;

SYSTEM DROP PART AGGREGATION CACHE;

DROP TABLE IF EXISTS t_part_agg_cache;
CREATE TABLE t_part_agg_cache (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;

-- Insert data into two separate parts.
INSERT INTO t_part_agg_cache VALUES (1, 10), (2, 20), (1, 30);
INSERT INTO t_part_agg_cache VALUES (1, 40), (2, 50);

-- Verify no cache entries initially.
SELECT count() FROM system.part_aggregation_cache;

-- First query with cache enabled: populates the cache.
SELECT k, sum(v) FROM t_part_agg_cache GROUP BY k ORDER BY k
    SETTINGS use_part_aggregation_cache = 1;

-- Verify cache is populated (one entry per part).
SELECT count() > 0 FROM system.part_aggregation_cache;

-- Second query: should use cache and return same result.
SELECT k, sum(v) FROM t_part_agg_cache GROUP BY k ORDER BY k
    SETTINGS use_part_aggregation_cache = 1;

-- Insert new data (new part). Cache should still work for old parts.
INSERT INTO t_part_agg_cache VALUES (1, 100), (2, 200);

-- Third query: cache hit for old parts, fresh aggregation for new part.
SELECT k, sum(v) FROM t_part_agg_cache GROUP BY k ORDER BY k
    SETTINGS use_part_aggregation_cache = 1;

-- Clear cache and verify.
SYSTEM DROP PART AGGREGATION CACHE;
SELECT count() FROM system.part_aggregation_cache;

DROP TABLE t_part_agg_cache;
