-- `query_plan_hash_join_subset_keys_auto` demotes a high-NDV equality key out of the hash table key
-- set into `JoinOperator::probe_conditions`, where it is evaluated during the probe. The join runtime
-- filter pass only inspects the equalities left in `JoinOperator::expression`, so for `LEFT ANTI` it
-- would build its exact `NOT IN` set on the kept keys alone and exclude left rows whose kept keys
-- appear on the right even though the full key tuple has no match - those rows must survive.
--
-- Cardinalities and NDVs come from `_internal_join_table_stat_hints` instead of real column
-- statistics, so the plan does not depend on how many rows are inserted.

DROP TABLE IF EXISTS jsk_anti_left;
DROP TABLE IF EXISTS jsk_anti_right;

CREATE TABLE jsk_anti_left (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE jsk_anti_right (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple();

-- `a` is near-unique on the build side and gets demoted, `b` has only 10 distinct values and is kept.
INSERT INTO jsk_anti_right SELECT number, number % 10 FROM numbers(5000);

-- `b = 1` occurs on the right (as `(1, 1)`, `(11, 1)`, ...) but never paired with `a = 2`, so this row
-- has no match on the full key tuple and belongs in the `LEFT ANTI` result.
INSERT INTO jsk_anti_left VALUES (2, 1);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';
SET use_statistics = 0;
SET send_logs_level = 'error'; -- Suppress the warning about the statistics hint
SET query_plan_optimize_join_order_randomize = 0; -- Pinned because the test asserts on the join plan
SET query_plan_optimize_join_order_limit = 10; -- Demotion runs from the join-order pass, so it must be enabled
SET query_plan_join_swap_table = 'false';
SET query_plan_hash_join_subset_keys_min_rows = 0;
SET query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001;
SET join_runtime_filter_min_probe_rows = 0;

SET param__internal_join_table_stat_hints = '
{
    "jsk_anti_left":  { "cardinality": 1,    "distinct_keys": { "a": 1,    "b": 1  } },
    "jsk_anti_right": { "cardinality": 5000, "distinct_keys": { "a": 5000, "b": 10 } }
}';

SELECT 'reference';
SELECT a, b FROM jsk_anti_left WHERE (a, b) NOT IN (SELECT a, b FROM jsk_anti_right) ORDER BY a;

-- The row survives in all four combinations of key demotion and the join runtime filter.
SELECT 'demotion off, runtime filter off';
SELECT l.a, l.b FROM jsk_anti_left l LEFT ANTI JOIN jsk_anti_right r ON l.a = r.a AND l.b = r.b
ORDER BY l.a
SETTINGS query_plan_hash_join_subset_keys_auto = 0, enable_join_runtime_filters = 0;

SELECT 'demotion on, runtime filter off';
SELECT l.a, l.b FROM jsk_anti_left l LEFT ANTI JOIN jsk_anti_right r ON l.a = r.a AND l.b = r.b
ORDER BY l.a
SETTINGS query_plan_hash_join_subset_keys_auto = 1, enable_join_runtime_filters = 0;

SELECT 'demotion off, runtime filter on';
SELECT l.a, l.b FROM jsk_anti_left l LEFT ANTI JOIN jsk_anti_right r ON l.a = r.a AND l.b = r.b
ORDER BY l.a
SETTINGS query_plan_hash_join_subset_keys_auto = 0, enable_join_runtime_filters = 1;

SELECT 'demotion on, runtime filter on';
SELECT l.a, l.b FROM jsk_anti_left l LEFT ANTI JOIN jsk_anti_right r ON l.a = r.a AND l.b = r.b
ORDER BY l.a
SETTINGS query_plan_hash_join_subset_keys_auto = 1, enable_join_runtime_filters = 1;

-- The demotion still happens with the runtime filter enabled; only the filter is skipped. The kept
-- clause is `b` alone, the demoted `a` equality is evaluated during the probe, and no
-- `BuildRuntimeFilter` step is planned for this join.
SELECT 'plan of the demoted anti join with runtime filters enabled';
SELECT trimLeft(explain) FROM
(
    EXPLAIN actions = 1, pretty = 0
    SELECT l.a, l.b FROM jsk_anti_left l LEFT ANTI JOIN jsk_anti_right r ON l.a = r.a AND l.b = r.b
    SETTINGS query_plan_hash_join_subset_keys_auto = 1, enable_join_runtime_filters = 1
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%' OR explain ILIKE '%BuildRuntimeFilter%';

-- An INNER join over the same shape keeps its runtime filter: there a set built on the kept keys only
-- lets extra rows through the filter and the join rejects them, so the result stays correct.
SELECT 'runtime filter steps in the demoted inner join';
SELECT countIf(explain ILIKE '%BuildRuntimeFilter%') FROM
(
    EXPLAIN actions = 1, pretty = 0
    SELECT l.a, l.b FROM jsk_anti_left l JOIN jsk_anti_right r ON l.a = r.a AND l.b = r.b
    SETTINGS query_plan_hash_join_subset_keys_auto = 1, enable_join_runtime_filters = 1
);

DROP TABLE jsk_anti_left;
DROP TABLE jsk_anti_right;
