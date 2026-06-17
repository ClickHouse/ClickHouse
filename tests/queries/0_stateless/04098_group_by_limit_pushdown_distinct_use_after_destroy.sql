-- Regression test: enable_group_by_top_k_optimization with Distinct combinator caused
-- use-after-destroy when trimHeapAndPruneHashTable evicted referenced aggregate states.
-- https://github.com/ClickHouse/ClickHouse/pull/96630

SET max_rows_to_group_by = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;

DROP TABLE IF EXISTS t_gbylimit_distinct;

CREATE TABLE t_gbylimit_distinct (k UInt32, val UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_gbylimit_distinct SELECT number % 1000, number FROM numbers(10000);

SELECT k, skewSampDistinct(val)
FROM t_gbylimit_distinct
GROUP BY k
ORDER BY k
LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 1;

DROP TABLE t_gbylimit_distinct;

-- Guard against the environment silently disabling the optimization.
SELECT 'optimization_applied_guard';
SELECT count() FROM (EXPLAIN actions = 1 SELECT number AS k FROM numbers(100) GROUP BY k ORDER BY k LIMIT 5 SETTINGS enable_group_by_top_k_optimization = 1) WHERE explain LIKE '%Top-K%';
