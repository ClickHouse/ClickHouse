-- Random settings limits: enable_join_transitive_predicates=(1, None)
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

-- A probe estimated at or below the limit still gets the filters when they are pushed down to a larger input below.
-- Row estimates come from hints.
DROP TABLE IF EXISTS key_path_p;
DROP TABLE IF EXISTS key_path_b;
DROP TABLE IF EXISTS key_path_s;
DROP TABLE IF EXISTS key_path_a;

CREATE TABLE key_path_p (x Int32, y Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE key_path_b (x Int32, z Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE key_path_s (y Int32, z Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE key_path_a (k Int32, v Int32) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO key_path_p SELECT number, number FROM numbers(100);
INSERT INTO key_path_b SELECT number, number FROM numbers(10);
INSERT INTO key_path_s SELECT number, number FROM numbers(50);
INSERT INTO key_path_a SELECT number % 10, number FROM numbers(100);

SET use_statistics = 0;
SET send_logs_level = 'error';
SET param__internal_join_table_stat_hints = '
{
    "key_path_p": { "cardinality": 100000, "distinct_keys": { "x": 100000, "y": 100000 } },
    "key_path_b": { "cardinality": 10, "distinct_keys": { "x": 10, "z": 10 } },
    "key_path_s": { "cardinality": 500, "distinct_keys": { "y": 500, "z": 500 } },
    "key_path_a": { "cardinality": 100000, "distinct_keys": { "k": 100 } }
}';

SELECT 'key from a larger input below' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_p p JOIN key_path_b b ON p.x = b.x JOIN key_path_s s ON p.y = s.y);

SELECT 'same, LEFT ANTI' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_p p JOIN key_path_b b ON p.x = b.x LEFT ANTI JOIN key_path_s s ON p.y = s.y);

-- The keys of a multi-key LEFT ANTI JOIN form one tuple filter, which is not pushed below a join whose inputs each hold only some of them.
SELECT 'multi-key LEFT ANTI' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_p p JOIN key_path_b b ON p.x = b.x LEFT ANTI JOIN key_path_s s ON p.y = s.y AND b.z = s.z);

SELECT 'key from a larger right input below' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_b b JOIN key_path_p p ON b.x = p.x JOIN key_path_s s ON p.y = s.y);

SELECT 'multi-key LEFT ANTI, keys from one input' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_p p JOIN key_path_b b ON p.x = b.x LEFT ANTI JOIN key_path_s s ON p.y = s.y AND p.x = s.z);

-- A filter on a computed or renamed key reads the columns the key is computed from, unless computing it is not deterministic
-- or is stateful.
SELECT 'computed key' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_p p JOIN key_path_b b ON p.x = b.x JOIN key_path_s s ON p.y + 1 = s.y);

SELECT 'computed key above a join kept for its key path' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_p p JOIN key_path_b b ON p.x = b.x JOIN key_path_s s ON p.y = s.y JOIN key_path_s s2 ON p.y + 1 = s2.y);

SELECT 'renamed key' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM (SELECT p.y AS py FROM key_path_p p JOIN key_path_b b ON p.x = b.x) t JOIN key_path_s s ON t.py = s.y);

SELECT 'non-deterministic key' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_p p JOIN key_path_b b ON p.x = b.x JOIN key_path_s s ON p.y + rand() = s.y);

SELECT 'stateful key' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_p p JOIN key_path_b b ON p.x = b.x JOIN key_path_s s ON timeSeriesStoreTags(p.y, []) = s.y);

-- A filter on a key of a join below is pushed down to both of its inputs, with the key replaced by the equal key of the other input.
SELECT 'key equal to a key of a larger input below' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_p p JOIN key_path_b b ON p.x = b.x JOIN key_path_s s ON b.x = s.y);

SELECT 'key of a smaller input below' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_p p JOIN key_path_b b ON p.x = b.x JOIN key_path_s s ON b.z = s.z);

-- A key equal to two keys or expressions of the other input is not followed, as it is not known which of them the filter is
-- pushed down with.
SELECT 'key equal to two keys of an input below' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_s a
      JOIN (SELECT p.y AS py, b.z AS bz FROM key_path_p p JOIN key_path_b b ON p.x = b.x) r ON a.y = r.py AND a.y = r.bz
      JOIN key_path_s c ON a.y = c.y
      SETTINGS query_plan_optimize_join_order_limit = 2);

SELECT 'key equal to a key and an expression of an input below' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_s a
      JOIN (SELECT p.y AS py, b.z AS bz FROM key_path_p p JOIN key_path_b b ON p.x = b.x) r ON a.y = r.py AND a.y = toInt32(r.bz + 1)
      JOIN key_path_s c ON a.y = c.y
      SETTINGS query_plan_optimize_join_order_limit = 2);

-- The push-down replaces a key by an expression of another type only if no key of the same type is equal to it.
SELECT 'key equal to a key and an expression of another type' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_s a
      JOIN (SELECT p.y AS py, b.z AS bz FROM key_path_p p JOIN key_path_b b ON p.x = b.x) r ON a.y = r.py AND a.y = r.bz + 1
      JOIN key_path_s c ON a.y = c.y
      SETTINGS query_plan_optimize_join_order_limit = 2);

-- An outer, SEMI or ANTI join below passes the filter to the input whose rows it keeps, and a SEMI join also to the other input.
SELECT 'LEFT SEMI JOIN below' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_p p LEFT SEMI JOIN key_path_b b ON p.x = b.x JOIN key_path_s s ON p.y = s.y);

SELECT 'right key of a LEFT SEMI JOIN below' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_p p LEFT SEMI JOIN key_path_b b ON p.x = b.x JOIN key_path_s s ON b.x = s.y);

SELECT 'RIGHT SEMI JOIN below' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_b b RIGHT SEMI JOIN key_path_p p ON b.x = p.x JOIN key_path_s s ON p.y = s.y);

SELECT 'key from the input a LEFT JOIN below adds rows to' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM key_path_b b LEFT JOIN key_path_p p ON b.x = p.x JOIN key_path_s s ON p.y = s.y);

-- Other steps pass the filter on if it reads only columns they keep: the keys of an aggregation, the columns that are not array joined.
SELECT 'aggregation key' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM (SELECT p.y AS y, count() AS c FROM key_path_p p JOIN key_path_b b ON p.x = b.x GROUP BY p.y) t
      JOIN key_path_s s ON t.y = s.y);

SELECT 'aggregate' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM (SELECT p.y AS y, count() AS c FROM key_path_p p JOIN key_path_b b ON p.x = b.x GROUP BY p.y) t
      JOIN key_path_s s ON toInt32(t.c) = s.y);

SELECT 'column next to ARRAY JOIN' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM (SELECT p.y AS y, arrayJoin([1, 2]) AS a FROM key_path_p p JOIN key_path_b b ON p.x = b.x) t
      JOIN key_path_s s ON t.y = s.y);

SELECT 'array joined column' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM (SELECT p.y AS y, arrayJoin([1, 2]) AS a FROM key_path_p p JOIN key_path_b b ON p.x = b.x) t
      JOIN key_path_s s ON t.a = s.y);

-- Below a step that drops rows, the input the filter is applied to is estimated, here the table under the aggregation.
SELECT 'aggregation of a larger table' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM (SELECT k, count() AS c FROM key_path_a GROUP BY k) t JOIN key_path_b b ON t.k = b.x);

SELECT 'aggregation of a smaller table' AS label, countIf(explain LIKE '%BuildRuntimeFilter%') AS installed
FROM (EXPLAIN SELECT count() FROM (SELECT x, count() AS c FROM key_path_b GROUP BY x) t JOIN key_path_b b ON t.x = b.x);

DROP TABLE small_probe;
DROP TABLE large_probe;
DROP TABLE build_side;
DROP TABLE key_path_p;
DROP TABLE key_path_b;
DROP TABLE key_path_s;
DROP TABLE key_path_a;
