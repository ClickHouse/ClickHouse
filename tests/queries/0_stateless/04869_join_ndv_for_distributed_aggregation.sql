-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Regression test for preserving result-column statistics through a distributed join.
-- A low-NDV GROUP BY after a join must use the join output statistics before
-- applying the generic single-child guard. Otherwise it uses the join row count
-- and selects shuffle aggregation.

DROP TABLE IF EXISTS left_04869;
DROP TABLE IF EXISTS right_04869;

CREATE TABLE left_04869 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE right_04869 (k UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO left_04869 SELECT number % 2, number FROM numbers(100);
INSERT INTO right_04869 SELECT number FROM numbers(2);

-- Disable persisted statistics so the test uses only the deterministic hints below.
SET use_statistics = 0;

-- Provide deterministic table cardinalities and key NDVs for join planning.
SET param__internal_join_table_stat_hints = '
{
    "left_04869": { "cardinality": 100, "distinct_keys": { "k": 2, "v": 100 } },
    "right_04869": { "cardinality": 2, "distinct_keys": { "k": 2 } }
}';

-- Keep the server-side AST fuzzer from rewriting this deterministic EXPLAIN query.
SET ast_fuzzer_runs = 0;
SET ast_fuzzer_any_query = 0;

-- Before the fix, the plan contained 0 partial aggregation steps because the
-- low NDV was lost. Now it contains 1 partial aggregation step.
SELECT count()
FROM
(
    EXPLAIN PLAN
    SELECT l.k, sum(l.v)
    FROM left_04869 AS l
    INNER JOIN right_04869 AS r ON l.k = r.k
    GROUP BY l.k
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
        distributed_plan_max_rows_to_broadcast = 10, enable_parallel_replicas = 0,
        enable_join_runtime_filters = 0, max_rows_to_group_by = 0,
        query_plan_optimize_join_order_randomize = 0,
        query_plan_optimize_join_order_limit = 10,
        distributed_aggregation_memory_efficient = 0
)
WHERE explain LIKE '%Aggregating (partial)%';

DROP TABLE left_04869;
DROP TABLE right_04869;
