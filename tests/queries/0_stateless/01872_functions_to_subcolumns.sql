DROP TABLE IF EXISTS t_func_to_subcolumns;

SET allow_experimental_map_type = 1;
SET optimize_functions_to_subcolumns = 1;

CREATE TABLE t_func_to_subcolumns (id UInt64, arr Array(UInt64), n Nullable(String), m Map(String, UInt64))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_func_to_subcolumns VALUES (1, [1, 2, 3], 'abc', map('foo', 1, 'bar', 2)) (2, [22], NULL, map());

SELECT length(arr), n IS NULL, n IS NOT NULL, mapKeys(m), mapValues(m) FROM t_func_to_subcolumns;
EXPLAIN SYNTAX SELECT length(arr), n IS NULL, n IS NOT NULL, mapKeys(m), mapValues(m) FROM t_func_to_subcolumns;

SELECT id, left.n IS NULL, right.n IS NULL FROM t_func_to_subcolumns AS left
FULL JOIN (SELECT 1 AS id, 'qqq' AS n UNION ALL SELECT 3 AS id, 'www') AS right USING(id);

EXPLAIN SYNTAX SELECT id, left.n IS NULL, right.n IS NULL FROM t_func_to_subcolumns AS left
FULL JOIN (SELECT 1 AS id, 'qqq' AS n UNION ALL SELECT 3 AS id, 'www') AS right USING(id);
