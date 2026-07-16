SET enable_analyzer = 1;
SET parallel_hash_join_threshold = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET use_statistics = 0;
SET optimize_syntax_fuse_functions = 1;
SET optimize_aggregation_in_order = 0;
SET optimize_read_in_order = 1;
SET optimize_distinct_in_order = 1;
SET optimize_sorting_by_input_stream_properties = 1;
SET allow_reorder_prewhere_conditions = 0;

DROP TABLE IF EXISTS t1;

CREATE TABLE t1 (a UInt64, b String, c Float64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t1 SELECT number, toString(number % 10), number * 1.5 FROM numbers(100);

SELECT '--- Simple aggregation ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT a, count() FROM t1 GROUP BY a;

SELECT '--- Complex aggregation key ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT a + 1, sum(c) FROM t1 GROUP BY a + 1;

SELECT '--- Multiple aggregates ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT a, count(), sum(c), avg(c) FROM t1 GROUP BY a;

SELECT '--- Aggregate with expression argument ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT sum(a * c) FROM t1;

SELECT '--- Sorting simple ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1, sorting = 1
SELECT * FROM t1 ORDER BY a ASC, b DESC;

SELECT '--- Sorting with expression ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1, sorting = 1
SELECT * FROM t1 ORDER BY a + 1;

SELECT '--- DISTINCT ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT DISTINCT a, b FROM t1;

SELECT '--- LIMIT BY ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT * FROM t1 LIMIT 1 BY b;

SELECT '--- HAVING ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT b, sum(c) AS s FROM t1 GROUP BY b HAVING s > 100;

SELECT '--- Window function ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT a, row_number() OVER (PARTITION BY b ORDER BY ((a * (2 + 1)) - 1)) AS rn FROM t1;


SELECT '--- Rollup ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT b, sum(c) FROM t1 GROUP BY ROLLUP(b);

SELECT '--- Cube ---';

EXPLAIN PLAN actions = 1, compact = 1, pretty = 1
SELECT b, sum(c) FROM t1 GROUP BY CUBE(b);

DROP TABLE t1;
