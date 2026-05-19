-- Tests the `join_runtime_filter_min_probe_rows` setting: a JOIN runtime filter is not installed
-- when the probe (left) side is estimated to produce at most this many rows.

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET join_runtime_filter_min_probe_rows=DEFAULT;

DROP TABLE IF EXISTS small_probe;
DROP TABLE IF EXISTS large_probe;
DROP TABLE IF EXISTS build_side;

CREATE TABLE small_probe (x Int32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE large_probe (x Int32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE build_side  (x Int32) ENGINE = MergeTree ORDER BY x;

INSERT INTO small_probe SELECT number FROM numbers(50);
INSERT INTO large_probe SELECT number FROM numbers(5000);
INSERT INTO build_side  SELECT number FROM numbers(10);

SELECT 'default, small probe' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM small_probe p JOIN build_side b ON p.x = b.x);

SELECT 'default, large probe' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM large_probe p JOIN build_side b ON p.x = b.x);

SELECT 'min=0, small probe' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM small_probe p JOIN build_side b ON p.x = b.x
      SETTINGS join_runtime_filter_min_probe_rows = 0);

SELECT 'min=50, 50-row probe' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM small_probe p JOIN build_side b ON p.x = b.x
      SETTINGS join_runtime_filter_min_probe_rows = 50);

SELECT 'chain default'  AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT * FROM (SELECT 1 AS x) t1
      JOIN (SELECT 1 AS x) t2 ON t1.x = t2.x
      JOIN (SELECT 1 AS x) t3 ON t1.x = t3.x
      JOIN (SELECT 1 AS x) t4 ON t1.x = t4.x);

SELECT 'chain min=0'    AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT * FROM (SELECT 1 AS x) t1
      JOIN (SELECT 1 AS x) t2 ON t1.x = t2.x
      JOIN (SELECT 1 AS x) t3 ON t1.x = t3.x
      JOIN (SELECT 1 AS x) t4 ON t1.x = t4.x
      SETTINGS join_runtime_filter_min_probe_rows = 0);

DROP TABLE small_probe;
DROP TABLE large_probe;
DROP TABLE build_side;
