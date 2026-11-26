-- ARRAY JOIN of a Nested column inside a subquery, CTE, UNION branch or view must
-- read only the referenced subcolumns, like the top-level query does.
-- https://github.com/ClickHouse/clickhouse-private/issues/58384

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_nested;
CREATE TABLE t_nested (`n.a` Array(Int64), `n.b` Array(Int64), `n.c` Array(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nested VALUES ([1, 2], [3, 4], [5, 6]), ([], [], []), ([7], [8], [9]);

SELECT '-- subquery';
SELECT * FROM (SELECT n.b FROM t_nested ARRAY JOIN n) ORDER BY 1;
EXPLAIN QUERY TREE SELECT * FROM (SELECT n.b FROM t_nested ARRAY JOIN n) ORDER BY 1;
SELECT * FROM (EXPLAIN header = 1 SELECT count() FROM (SELECT n.b FROM t_nested ARRAY JOIN n)) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header: n.%';

SELECT '-- CTE';
WITH r AS (SELECT n.b AS b, n.c AS c FROM t_nested ARRAY JOIN n) SELECT b, c FROM r ORDER BY 1;
EXPLAIN QUERY TREE WITH r AS (SELECT n.b AS b, n.c AS c FROM t_nested ARRAY JOIN n) SELECT b, c FROM r ORDER BY 1;

SELECT '-- CTE referenced twice with different subcolumns';
WITH r AS (SELECT n.a AS a, n.b AS b, n.c AS c FROM t_nested ARRAY JOIN n) SELECT (SELECT sum(b) FROM r), (SELECT sum(c) FROM r);

SELECT '-- UNION ALL branches';
SELECT * FROM (SELECT n.b AS x FROM t_nested ARRAY JOIN n UNION ALL SELECT n.c FROM t_nested ARRAY JOIN n) ORDER BY 1;
EXPLAIN QUERY TREE SELECT n.b AS x FROM t_nested ARRAY JOIN n UNION ALL SELECT n.c FROM t_nested ARRAY JOIN n;

SELECT '-- view with unused projection column';
DROP VIEW IF EXISTS v_nested;
CREATE VIEW v_nested AS SELECT n.a AS a, n.b AS b, n.c AS c FROM t_nested ARRAY JOIN n;
SELECT b FROM v_nested ORDER BY 1;
SELECT * FROM (EXPLAIN header = 1 SELECT count() FROM (SELECT b FROM v_nested)) WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Header: n.%';
DROP VIEW v_nested;

SELECT '-- numeric tupleElement index inside subquery';
SELECT * FROM (SELECT tupleElement(n, 2), tupleElement(n, 3) FROM t_nested ARRAY JOIN n) ORDER BY 1;

SELECT '-- LEFT ARRAY JOIN inside subquery';
SELECT * FROM (SELECT n.b FROM t_nested LEFT ARRAY JOIN n) ORDER BY 1;

SELECT '-- ARRAY JOIN inside IN subquery, both scopes name the column n';
SELECT n.b FROM t_nested ARRAY JOIN n WHERE n.a IN (SELECT n.a FROM t_nested ARRAY JOIN n WHERE n.c > 5) ORDER BY 1;

SELECT '-- ARRAY JOIN inside IN subquery that is part of an outer ARRAY JOIN expression';
SELECT x FROM t_nested ARRAY JOIN arrayFilter(v -> v IN (SELECT n.b - 2 FROM t_nested ARRAY JOIN n), n.a) AS x ORDER BY 1;

SELECT '-- second ARRAY JOIN references a subcolumn of the first one';
SELECT y FROM t_nested ARRAY JOIN n ARRAY JOIN arrayMap(x -> x + n.b, [100]) AS y ORDER BY 1;
EXPLAIN QUERY TREE SELECT y FROM t_nested ARRAY JOIN n ARRAY JOIN arrayMap(x -> x + n.b, [100]) AS y;

SELECT '-- whole tuple projected from the subquery: nothing to prune';
SELECT n.c FROM (SELECT n FROM t_nested ARRAY JOIN n) ORDER BY 1;
EXPLAIN QUERY TREE SELECT n.c FROM (SELECT n FROM t_nested ARRAY JOIN n);

DROP TABLE t_nested;

SELECT '-- unused subcolumn with mismatched array sizes is not read, same as the top-level query';
DROP TABLE IF EXISTS t_mismatch;
CREATE TABLE t_mismatch (`n.a` Array(Int64), `n.b` Array(Int64), `n.c` Array(Int64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS share_nested_offsets = 0;
INSERT INTO t_mismatch VALUES ([1, 2], [3, 4], [5]), ([7], [8], [9, 10, 11]);
SELECT n.a, n.b FROM t_mismatch ARRAY JOIN n ORDER BY 1;
SELECT * FROM (SELECT n.a, n.b FROM t_mismatch ARRAY JOIN n) ORDER BY 1;
SELECT * FROM (SELECT n.a, n.c FROM t_mismatch ARRAY JOIN n) ORDER BY 1; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
DROP TABLE t_mismatch;
