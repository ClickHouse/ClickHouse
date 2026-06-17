-- Regression test: enable_group_by_top_k_optimization with Distinct combinator caused
-- use-after-destroy when trimHeapAndPruneHashTable evicted keys whose
-- aggregate states were already referenced in the places[] array.
-- https://github.com/ClickHouse/ClickHouse/pull/96630

-- The CI test profile sets max_rows_to_group_by, which disables the optimization; reset it.
SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny), which would
-- gate the optimization off for the limits used here; pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;

DROP TABLE IF EXISTS t_gbylimit_distinct;

CREATE TABLE t_gbylimit_distinct (k UInt32, val UInt64) ENGINE = MergeTree ORDER BY k;

-- Insert enough rows with many distinct keys to trigger the top-N heap trim
-- within a single batch.
INSERT INTO t_gbylimit_distinct SELECT number % 1000, number FROM numbers(10000);

-- The crash happened with skewSampDistinct, but any Distinct combinator on a
-- numeric column exercises the same code path (AggregateFunctionDistinctSingleNumericData).
-- Using a small LIMIT ensures enable_group_by_top_k_optimization kicks in and the heap evicts keys.
SELECT k, skewSampDistinct(val)
FROM t_gbylimit_distinct
GROUP BY k
ORDER BY k
LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 1;

DROP TABLE t_gbylimit_distinct;

-- Guard against the environment silently disabling the optimization (e.g. via a
-- profile setting), which would degrade the comparisons above to off-vs-off.
SELECT 'optimization_applied_guard';
SELECT count() FROM (EXPLAIN actions = 1 SELECT number AS k FROM numbers(100) GROUP BY k ORDER BY k LIMIT 5 SETTINGS enable_group_by_top_k_optimization = 1) WHERE explain LIKE '%Top-K%';
