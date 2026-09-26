-- `optimize_push_subcolumns_into_subqueries`: a subcolumn read is pushed through every level of nested subqueries,
-- a subquery with `INTERPOLATE` is left alone, and so is a table function whose storage cannot read subcolumns.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_subcolumn_pushdown_nested;
CREATE TABLE t_subcolumn_pushdown_nested (n UInt64, tup Tuple(a String, b Int32), arr Array(UInt32)) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_subcolumn_pushdown_nested VALUES (1, ('x', 1), [1, 2]), (3, ('y', 2), [3]);

SELECT 'nested subqueries: every level reads the subcolumn';
SELECT countIf(explain LIKE '%column_name: tup.a,%'), countIf(explain LIKE '%column_name: tup,%')
FROM (EXPLAIN QUERY TREE SELECT tup.a FROM (SELECT tup FROM (SELECT * FROM t_subcolumn_pushdown_nested)));
SELECT tup.a FROM (SELECT tup FROM (SELECT * FROM t_subcolumn_pushdown_nested)) ORDER BY ALL;
SELECT tup.a FROM (SELECT tup FROM (SELECT * FROM t_subcolumn_pushdown_nested)) ORDER BY ALL SETTINGS optimize_push_subcolumns_into_subqueries = 0;

SELECT 'interpolate in the subquery';
SELECT tup.a FROM (SELECT n, tup FROM t_subcolumn_pushdown_nested ORDER BY n WITH FILL INTERPOLATE (tup AS tup)) ORDER BY ALL;
SELECT tup.a FROM (SELECT n, tup FROM t_subcolumn_pushdown_nested ORDER BY n WITH FILL INTERPOLATE (tup AS tup)) ORDER BY ALL SETTINGS optimize_push_subcolumns_into_subqueries = 0;

SELECT 'aggregation in a nested scope over the same CTE';
WITH foo AS (SELECT tup AS r FROM t_subcolumn_pushdown_nested)
SELECT (SELECT count() FROM foo GROUP BY r HAVING r.a = 'x'), r.a FROM foo ORDER BY ALL;
WITH foo AS (SELECT tup AS r FROM t_subcolumn_pushdown_nested)
SELECT (SELECT count() FROM foo GROUP BY r HAVING r.a = 'x'), r.a FROM foo ORDER BY ALL SETTINGS optimize_push_subcolumns_into_subqueries = 0;

SELECT 'table function that cannot read subcolumns';
INSERT INTO FUNCTION file(currentDatabase() || '_05257.tsv', 'TSV', 'tup Tuple(a String, b Int32), arr Array(UInt32)')
SELECT tup, arr FROM t_subcolumn_pushdown_nested SETTINGS engine_file_truncate_on_insert = 1;
SELECT countIf(explain LIKE '%column_name: tup.a,%'), countIf(explain LIKE '%column_name: arr.size0,%')
FROM (EXPLAIN QUERY TREE SELECT tup.a, length(arr) FROM (SELECT tup, arr FROM file(currentDatabase() || '_05257.tsv', 'TSV', 'tup Tuple(a String, b Int32), arr Array(UInt32)')));
SELECT tup.a, length(arr) FROM (SELECT tup, arr FROM file(currentDatabase() || '_05257.tsv', 'TSV', 'tup Tuple(a String, b Int32), arr Array(UInt32)')) ORDER BY ALL;

DROP TABLE t_subcolumn_pushdown_nested;
