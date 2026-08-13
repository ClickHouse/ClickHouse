-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Regression test for #68096. An optimized JoinStepLogical has two children,
-- so estimateReadRowsCount must read its saved result-column statistics before
-- the generic single-child guard. Otherwise this low-NDV GROUP BY falls back
-- to the join row count and uses shuffle aggregation.

DROP TABLE IF EXISTS left_04869;
DROP TABLE IF EXISTS right_04869;

CREATE TABLE left_04869 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE right_04869 (k UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO left_04869 SELECT number % 2, number FROM numbers(100);
INSERT INTO right_04869 SELECT number FROM numbers(2);

SET use_statistics = 0;
SET param__internal_join_table_stat_hints = '
{
    "left_04869": { "cardinality": 100, "distinct_keys": { "k": 2, "v": 100 } },
    "right_04869": { "cardinality": 2, "distinct_keys": { "k": 2 } }
}';

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
