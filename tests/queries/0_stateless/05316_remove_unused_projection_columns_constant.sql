-- When the outer query uses no column of a subquery, the subquery keeps a constant instead of a column,
-- so only the columns needed by its other clauses are read, or the cheapest column of the table.

DROP VIEW IF EXISTS v_nested;
DROP TABLE IF EXISTS t_smallest;
DROP TABLE IF EXISTS t_nested;
DROP TABLE IF EXISTS t_num;
DROP TABLE IF EXISTS t_memory;
CREATE TABLE t_smallest (s String, id UInt8) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_smallest SELECT randomPrintableASCII(1000), number % 256 FROM numbers(1000);
CREATE TABLE t_memory (s String, id UInt8) ENGINE = Memory;
INSERT INTO t_memory SELECT s, id FROM t_smallest;
CREATE TABLE t_num (number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO t_num SELECT number FROM numbers(4);

-- The 1 MB column s is not read.
SELECT count() FROM (SELECT s, id FROM t_smallest WHERE id < 100) SETTINGS query_plan_remove_unused_columns = 0, max_bytes_to_read = 100000;
SELECT count() FROM (SELECT s, id FROM t_smallest LIMIT 100000) SETTINGS max_bytes_to_read = 100000;
SELECT count() FROM (SELECT toString(id) AS k, cityHash64(s) AS h FROM t_smallest LIMIT 100000) SETTINGS max_bytes_to_read = 100000;
SELECT count() FROM (SELECT cityHash64(s) AS h, id FROM t_smallest LIMIT 100000) SETTINGS max_bytes_to_read = 100000;
SELECT count() FROM (SELECT uniqExact(s) AS c, toString(id) AS k FROM t_smallest GROUP BY k) SETTINGS max_bytes_to_read = 100000;
SELECT count() FROM (SELECT * FROM (SELECT s, id FROM t_smallest)) SETTINGS query_plan_remove_unused_columns = 0, max_bytes_to_read = 100000;
SELECT count() FROM (SELECT s FROM t_smallest UNION ALL SELECT s FROM t_smallest) SETTINGS query_plan_remove_unused_columns = 0, max_bytes_to_read = 100000;
SELECT count() FROM t_smallest WHERE EXISTS (SELECT s, id FROM t_smallest LIMIT 100000) SETTINGS max_bytes_to_read = 100000;
SELECT count() FROM (SELECT s, id FROM t_smallest LIMIT 100000) AS sub(a, b) SETTINGS max_bytes_to_read = 100000;
WITH x(a, b) AS (SELECT s, id FROM t_smallest LIMIT 100000) SELECT count() FROM x SETTINGS max_bytes_to_read = 100000;
SELECT count() FROM (EXPLAIN header = 1 SELECT count() FROM (SELECT s, id FROM t_memory LIMIT 100000)) WHERE explain ILIKE '% s String%';

-- No other column is read when another clause reads s.
SELECT count() FROM (SELECT s, id FROM t_smallest WHERE notEmpty(s)) SETTINGS query_plan_remove_unused_columns = 0, max_columns_to_read = 1;

-- The kept column is still compared by EXCEPT and INTERSECT and read by a recursive CTE.
SELECT count() FROM (SELECT number FROM t_num EXCEPT ALL SELECT number + 10 FROM t_num WHERE number < 2);
SELECT count() FROM (SELECT number FROM t_num INTERSECT ALL SELECT number + 10 FROM t_num);
SELECT count() FROM (SELECT number FROM t_num UNION ALL (SELECT number FROM t_num EXCEPT ALL SELECT number + 10 FROM t_num WHERE number < 2));
WITH RECURSIVE r AS (SELECT number AS n FROM t_num WHERE number = 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT count() FROM r;

-- ARRAY JOIN, also of arrays with different sizes and behind a view, WITH FILL and INTERPOLATE work as before.
SELECT count() FROM (SELECT x FROM (SELECT [1, 2, 3] AS arr) ARRAY JOIN arr AS x) SETTINGS query_plan_remove_unused_columns = 0;
CREATE TABLE t_nested (`n.a` Array(UInt8), `n.c` Array(UInt8)) ENGINE = MergeTree ORDER BY tuple() SETTINGS share_nested_offsets = 0;
INSERT INTO t_nested VALUES ([1, 2], [5]), ([7], [9, 10, 11]);
SELECT count() FROM (SELECT n.c FROM t_nested ARRAY JOIN n);
SELECT count() FROM (SELECT c FROM (SELECT n.a AS a, n.c AS c FROM t_nested ARRAY JOIN n));
CREATE VIEW v_nested AS SELECT n.a AS a, n.c AS c FROM t_nested ARRAY JOIN n;
SELECT count() FROM (SELECT c FROM v_nested LIMIT 100000);
SELECT count() FROM (SELECT toString(number) AS s, number FROM t_num ORDER BY number WITH FILL FROM 0 TO 10) SETTINGS query_plan_remove_unused_columns = 0;
SELECT count() FROM (SELECT [number] AS s, number AS n FROM t_num ORDER BY n WITH FILL FROM 0 TO 10 INTERPOLATE (s AS arrayConcat(s, [1]))) SETTINGS query_plan_remove_unused_columns = 0;
SELECT count() FROM (SELECT [id] AS a, id FROM t_smallest ORDER BY id WITH FILL FROM 0 TO 300 INTERPOLATE (a AS arrayConcat(a, [1])))
SETTINGS query_plan_remove_unused_columns = 0, enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_local_plan = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;

DROP VIEW v_nested;
DROP TABLE t_smallest;
DROP TABLE t_nested;
DROP TABLE t_num;
DROP TABLE t_memory;
