-- `optimize_push_subcolumns_into_subqueries` must not rewrite a subcolumn of a column that a correlated subquery
-- lists among its correlated columns.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS t_subcolumn_pushdown_correlated;
CREATE TABLE t_subcolumn_pushdown_correlated (n UInt64, p Tuple(a String, b Int32)) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_subcolumn_pushdown_correlated VALUES (1, ('x', 1)), (2, ('y', 2)), (3, ('x', 3));

SELECT count() FROM (SELECT p FROM t_subcolumn_pushdown_correlated) AS o
WHERE EXISTS (SELECT 1 FROM t_subcolumn_pushdown_correlated AS i WHERE i.p.a = o.p.a AND i.n != o.p.b);

SELECT o.p.a, o.p.b FROM (SELECT p FROM t_subcolumn_pushdown_correlated) AS o
WHERE EXISTS (SELECT 1 FROM t_subcolumn_pushdown_correlated AS i WHERE i.p.a = o.p.a AND i.n != o.p.b)
ORDER BY ALL;

DROP TABLE t_subcolumn_pushdown_correlated;
