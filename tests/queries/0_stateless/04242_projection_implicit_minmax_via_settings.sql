-- Tags: no-parallel-replicas

SET explain_query_plan_default = 'legacy';

CREATE TABLE test_proj_minmax
(
    a UInt32,
    b UInt32,
    PROJECTION p (SELECT a, b ORDER BY b) WITH SETTINGS (add_minmax_index_for_numeric_columns = 1)
)
ENGINE = MergeTree
PARTITION BY a % 3
ORDER BY a
SETTINGS index_granularity = 1, storage_policy = 'default';

INSERT INTO test_proj_minmax SELECT number, number FROM numbers(100);

SET force_optimize_projection = 1;
SET optimize_use_projections = 1;
SET enable_analyzer = 1;

EXPLAIN indexes = 1, projections = 1
SELECT a, b
FROM test_proj_minmax
WHERE b = 10
ORDER BY b;
