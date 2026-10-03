-- Tags: no-parallel

-- Regression test for the part aggregation cache and the `GROUP BY` Top-K optimization.
--
-- `GROUP BY k ORDER BY k LIMIT n` lets the planner push a Top-K into the aggregation
-- (`enable_group_by_top_k_optimization`): each aggregation stream keeps only its `n` best groups.
-- The cache key covers the keys, the aggregates and the filter, but not the `LIMIT`, so the state
-- the populator caches for a part must contain every group of that part. If the warmup inherited
-- the Top-K, a later query with a wider `LIMIT` (or none) would reuse a truncated state and miss
-- groups. The populator therefore clears the Top-K before aggregating a part, and the cached state
-- must contain every group regardless of the `LIMIT` of the query that warmed it.

-- The functional-test config (`tests/config/users.d/limits.yaml`) sets `max_rows_to_group_by` and
-- read limits, on which the optimization fails closed; pin them all to 0 so the cache is exercised
-- (as in `04033_part_aggregation_cache`).
SET allow_experimental_part_aggregation_cache = 1, optimize_aggregation_in_order = 0, enable_memory_bound_merging_of_aggregation_results = 0, max_rows_to_group_by = 0, max_rows_to_read = 0, max_bytes_to_read = 0, max_rows_to_read_leaf = 0, max_bytes_to_read_leaf = 0;
SET enable_group_by_top_k_optimization = 1, query_plan_max_limit_for_top_k_optimization = 0, max_threads = 1;

SYSTEM DROP PART AGGREGATION CACHE;

DROP TABLE IF EXISTS t_part_agg_cache_top_k;

CREATE TABLE t_part_agg_cache_top_k (k UInt32) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t_part_agg_cache_top_k;

-- Exactly one part with 8 groups.
INSERT INTO t_part_agg_cache_top_k SELECT number % 8 FROM numbers(64);
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_part_agg_cache_top_k' AND active;
SELECT count() FROM system.part_aggregation_cache;

-- The narrow query really is planned with a Top-K pushed into the aggregation, so the test is not vacuous.
SELECT count() FROM (EXPLAIN actions = 1 SELECT k, count() FROM t_part_agg_cache_top_k GROUP BY k ORDER BY k LIMIT 2) WHERE explain LIKE '%Top-K: limit=2%';

-- Warm the cache with the narrow `LIMIT`. The single cached state must hold all 8 groups of the part.
SELECT k, count() FROM t_part_agg_cache_top_k GROUP BY k ORDER BY k LIMIT 2;
SELECT count(), sum(result_rows) FROM system.part_aggregation_cache;

-- A wider `LIMIT` and no `LIMIT` reuse the cached state and must still see all 8 groups.
SELECT k, count() FROM t_part_agg_cache_top_k GROUP BY k ORDER BY k LIMIT 5;
SELECT k, count() FROM t_part_agg_cache_top_k GROUP BY k ORDER BY k;

-- Still a single cached entry: the broader queries hit the same key and were served from it.
SELECT count() FROM system.part_aggregation_cache;

DROP TABLE t_part_agg_cache_top_k;
SYSTEM DROP PART AGGREGATION CACHE;
