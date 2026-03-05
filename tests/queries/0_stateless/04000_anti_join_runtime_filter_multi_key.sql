-- Regression test: LEFT ANTI JOIN with multiple join keys and enable_join_runtime_filters=1
-- The runtime filter optimization incorrectly applied per-column NOT IN filters combined with AND,
-- which dropped rows that should appear in the result.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

CREATE TABLE t1 (a Int64, b Int64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (aa Int64, bb Int64) ENGINE = MergeTree ORDER BY aa;

INSERT INTO t1 SELECT number % 2, number % 3 FROM numbers(3);
INSERT INTO t2 SELECT number % 3, number % 2 FROM numbers(3);

-- t1 = {(0,0), (1,1), (0,2)}, t2 = {(0,0), (1,1), (2,0)}
-- Correct ANTI JOIN result: (0,2) — no row in t2 has aa=0 AND bb=2 simultaneously

SELECT 'Without runtime filters:';
SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a, t1.b
SETTINGS enable_join_runtime_filters = 0;

SELECT 'With runtime filters (must match):';
SELECT t1.a, t1.b FROM t1 LEFT ANTI JOIN t2 ON t1.a = t2.aa AND t1.b = t2.bb
ORDER BY t1.a, t1.b
SETTINGS enable_join_runtime_filters = 1;
