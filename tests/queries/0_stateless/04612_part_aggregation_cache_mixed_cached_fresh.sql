-- Tags: no-parallel

-- Regression test for the mixed cached+fresh execution path of `optimizeUsePartAggregationCache`:
-- cached states for old parts must be merged (via `convertToAggregatingProjection`) with a fresh
-- read of an uncached part. The default-settings queries in `04033_part_aggregation_cache` never
-- reach this branch, because with `enable_writes_to_part_aggregation_cache = 1` the optimizer
-- backfills every uncached part before rewriting the plan and then takes the all-cached path.
-- Disabling writes for the final query forces `uncached_parts` to stay non-empty, so the plan must
-- go through the mixed branch.

-- The functional-test config (`tests/config/users.d/limits.yaml`) sets `max_rows_to_group_by = 10G`
-- and read limits (`max_rows_to_read`, `max_bytes_to_read`, `*_leaf`), on which the optimization
-- fails closed; pin them all to 0 so the cache is actually exercised (as in
-- `04033_part_aggregation_cache`).
SET allow_experimental_analyzer = 0, allow_experimental_part_aggregation_cache = 1, optimize_aggregation_in_order = 0, enable_memory_bound_merging_of_aggregation_results = 0, max_rows_to_group_by = 0, max_rows_to_read = 0, max_bytes_to_read = 0, max_rows_to_read_leaf = 0, max_bytes_to_read_leaf = 0;

SYSTEM DROP PART AGGREGATION CACHE;

DROP TABLE IF EXISTS t_part_agg_cache_mixed;
CREATE TABLE t_part_agg_cache_mixed (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k;

-- Keep the part layout stable: a background merge between the warm-up query and the mixed query
-- would rename the parts, turn the warmed entries into misses, and silently degrade the final
-- query to the plain aggregation path, so the test would no longer prove anything.
SYSTEM STOP MERGES t_part_agg_cache_mixed;

INSERT INTO t_part_agg_cache_mixed VALUES (1, 10), (2, 20);
INSERT INTO t_part_agg_cache_mixed VALUES (1, 30), (2, 40);

SELECT count() FROM system.part_aggregation_cache;

-- Warm the cache for the two old parts. k=1: 10+30=40, k=2: 20+40=60.
SELECT k, sum(v) FROM t_part_agg_cache_mixed GROUP BY k ORDER BY k;

-- Both old parts are cached.
SELECT count() FROM system.part_aggregation_cache;

-- A new part that is not in the cache.
INSERT INTO t_part_agg_cache_mixed VALUES (1, 100), (2, 200);

-- Reads enabled, writes disabled: the optimizer cannot backfill the new part, so it must merge the
-- two cached states with a fresh read of the new part. k=1: 40+100=140, k=2: 60+200=260 — proving
-- the fresh part's rows are aggregated and combined with the cached states.
SELECT k, sum(v) FROM t_part_agg_cache_mixed GROUP BY k ORDER BY k
    SETTINGS enable_reads_from_part_aggregation_cache = 1, enable_writes_to_part_aggregation_cache = 0;

-- Writes were disabled, so the new part must not have been cached: still exactly the two warmed
-- entries. Together with the correct result above, this proves the query could not have taken the
-- backfill-then-all-cached path and went through the mixed cached+fresh branch.
SELECT count() FROM system.part_aggregation_cache;

DROP TABLE t_part_agg_cache_mixed;
SYSTEM DROP PART AGGREGATION CACHE;
