-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

-- { echo }

SET optimize_use_projections = 1;

DROP TABLE IF EXISTS test_distinct_proj_partial_projection;

CREATE TABLE test_distinct_proj_partial_projection
(
    a UInt32,
    b UInt32
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_distinct_proj_partial_projection VALUES (1, 10), (1, 10), (2, 20), (3, 30), (3, 30);

ALTER TABLE test_distinct_proj_partial_projection ADD PROJECTION p (SELECT count() GROUP BY a, b);

INSERT INTO test_distinct_proj_partial_projection VALUES (3, 30), (4, 40), (4, 40), (5, 50), (6, 60), (6, 60);

SELECT count() FROM test_distinct_proj_partial_projection;

SELECT DISTINCT a, b FROM test_distinct_proj_partial_projection ORDER BY a, b;
EXPLAIN SELECT DISTINCT a, b FROM test_distinct_proj_partial_projection ORDER BY a, b SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

SELECT DISTINCT a, b FROM test_distinct_proj_partial_projection WHERE a <= 2 ORDER BY a, b;
EXPLAIN SELECT DISTINCT a, b FROM test_distinct_proj_partial_projection WHERE a <= 2 ORDER BY a, b SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

SELECT DISTINCT a, b FROM test_distinct_proj_partial_projection WHERE a >= 4 ORDER BY a, b;
EXPLAIN SELECT DISTINCT a, b FROM test_distinct_proj_partial_projection WHERE a >= 4 ORDER BY a, b SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

SELECT DISTINCT a, b FROM test_distinct_proj_partial_projection WHERE a = 3 ORDER BY a, b;
EXPLAIN SELECT DISTINCT a, b FROM test_distinct_proj_partial_projection WHERE a = 3 ORDER BY a, b SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

DROP TABLE test_distinct_proj_partial_projection;
