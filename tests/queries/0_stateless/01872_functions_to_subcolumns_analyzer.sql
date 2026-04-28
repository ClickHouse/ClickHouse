DROP TABLE IF EXISTS t_func_to_subcolumns;

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

CREATE TABLE t_func_to_subcolumns (id UInt64, arr Array(UInt64), n Nullable(String), m Map(String, UInt64))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_func_to_subcolumns VALUES (1, [1, 2, 3], 'abc', map('foo', 1, 'bar', 2)) (2, [], NULL, map());

SELECT id IS NULL, n IS NULL, n IS NOT NULL FROM t_func_to_subcolumns;
EXPLAIN QUERY TREE dump_tree = 1, dump_ast = 1 SELECT id IS NULL, n IS NULL, n IS NOT NULL FROM t_func_to_subcolumns;

SELECT length(arr), empty(arr), notEmpty(arr), empty(n) FROM t_func_to_subcolumns;
EXPLAIN QUERY TREE dump_tree = 1, dump_ast = 1 SELECT length(arr), empty(arr), notEmpty(arr), empty(n) FROM t_func_to_subcolumns;

SELECT mapKeys(m), mapValues(m) FROM t_func_to_subcolumns;
EXPLAIN QUERY TREE dump_tree = 1, dump_ast = 1 SELECT mapKeys(m), mapValues(m) FROM t_func_to_subcolumns;

SELECT count(n) FROM t_func_to_subcolumns;
EXPLAIN QUERY TREE dump_tree = 1, dump_ast = 1 SELECT count(n) FROM t_func_to_subcolumns;

SELECT count(id) FROM t_func_to_subcolumns;
EXPLAIN QUERY TREE dump_tree = 1, dump_ast = 1 SELECT count(id) FROM t_func_to_subcolumns;

SELECT id, left.n IS NULL, right.n IS NULL FROM t_func_to_subcolumns AS left
FULL JOIN (SELECT 1 AS id, 'qqq' AS n UNION ALL SELECT 3 AS id, 'www') AS right USING(id);

EXPLAIN QUERY TREE dump_tree = 1, dump_ast = 1 SELECT id, left.n IS NULL, right.n IS NULL FROM t_func_to_subcolumns AS left
FULL JOIN (SELECT 1 AS id, 'qqq' AS n UNION ALL SELECT 3 AS id, 'www') AS right USING(id);

DROP TABLE t_func_to_subcolumns;

DROP TABLE IF EXISTS t_tuple_null;

CREATE TABLE t_tuple_null (t Tuple(null UInt32)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_tuple_null VALUES ((10)), ((20));

SELECT t IS NULL, t.null FROM t_tuple_null;

DROP TABLE t_tuple_null;
