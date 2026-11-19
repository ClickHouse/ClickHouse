-- Tests the `join_runtime_filter_min_probe_rows` setting: a JOIN runtime filter is not installed
-- when the probe (left) side is estimated to produce at most this many rows.

SET enable_join_runtime_filters = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET join_runtime_filter_min_probe_rows=DEFAULT;
SET query_plan_optimize_join_order_limit=16;
SET query_plan_optimize_join_order_randomize=0;

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

-- A probe estimated at or below the limit still gets the filters when a key comes from a larger input of an inner
-- join below: the filter is pushed down to that input. Row estimates come from hints.
DROP TABLE IF EXISTS key_path_p;
DROP TABLE IF EXISTS key_path_b;
DROP TABLE IF EXISTS key_path_s;

CREATE TABLE key_path_p (x Int32, y Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE key_path_b (x Int32, z Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE key_path_s (y Int32, z Int32) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO key_path_p SELECT number, number FROM numbers(100);
INSERT INTO key_path_b SELECT number, number FROM numbers(10);
INSERT INTO key_path_s SELECT number, number FROM numbers(50);

SET use_statistics = 0;
SET send_logs_level = 'error';
SET param__internal_join_table_stat_hints = '
{
    "key_path_p": { "cardinality": 100000, "distinct_keys": { "x": 100000, "y": 100000 } },
    "key_path_b": { "cardinality": 10, "distinct_keys": { "x": 10, "z": 10 } },
    "key_path_s": { "cardinality": 500, "distinct_keys": { "y": 500, "z": 500 } }
}';

SELECT 'key from a larger input below' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_p p JOIN key_path_b b ON p.x = b.x JOIN key_path_s s ON p.y = s.y);

SELECT 'same, LEFT ANTI' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_p p JOIN key_path_b b ON p.x = b.x LEFT ANTI JOIN key_path_s s ON p.y = s.y);

-- The keys of a multi-key LEFT ANTI JOIN form one tuple filter, which is not pushed below a join whose inputs each hold only some of them.
SELECT 'multi-key LEFT ANTI' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_p p JOIN key_path_b b ON p.x = b.x LEFT ANTI JOIN key_path_s s ON p.y = s.y AND b.z = s.z);

DROP TABLE small_probe;
DROP TABLE large_probe;
DROP TABLE build_side;
DROP TABLE key_path_p;
DROP TABLE key_path_b;
DROP TABLE key_path_s;
