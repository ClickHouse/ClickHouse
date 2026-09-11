-- Pushing a subcolumn read into a subquery must never change the result of the query.
-- Every query below is compared against the same query with the optimization turned off.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_subcolumn_pushdown;

CREATE TABLE t_subcolumn_pushdown
(
    id UInt64,
    tup Tuple(a String, b Int32),
    other Tuple(a String, b Int32)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_subcolumn_pushdown VALUES (1, ('x', 1), ('p', 10)), (2, ('x', 5), ('q', 20)), (3, ('y', 1), ('r', 30));

SELECT 'base column read directly and through a subcolumn';
SELECT tup, tup.a FROM (SELECT * FROM t_subcolumn_pushdown) ORDER BY ALL;
SELECT tup, tup.a FROM (SELECT * FROM t_subcolumn_pushdown) ORDER BY ALL SETTINGS optimize_push_subcolumns_into_subqueries = 0;

SELECT 'distinct in the subquery';
SELECT tup.a FROM (SELECT DISTINCT tup FROM t_subcolumn_pushdown) ORDER BY ALL;
SELECT tup.a FROM (SELECT DISTINCT tup FROM t_subcolumn_pushdown) ORDER BY ALL SETTINGS optimize_push_subcolumns_into_subqueries = 0;

SELECT 'group by in the subquery';
SELECT tup.a FROM (SELECT tup FROM t_subcolumn_pushdown GROUP BY tup) ORDER BY ALL;
SELECT tup.a FROM (SELECT tup FROM t_subcolumn_pushdown GROUP BY tup) ORDER BY ALL SETTINGS optimize_push_subcolumns_into_subqueries = 0;

SELECT 'limit by in the subquery';
SELECT tup.a FROM (SELECT tup FROM t_subcolumn_pushdown ORDER BY tup LIMIT 1 BY tup.a) ORDER BY ALL;
SELECT tup.a FROM (SELECT tup FROM t_subcolumn_pushdown ORDER BY tup LIMIT 1 BY tup.a) ORDER BY ALL SETTINGS optimize_push_subcolumns_into_subqueries = 0;

SELECT 'the subquery exposes the column under an alias';
SELECT tup.a FROM (SELECT other AS tup FROM t_subcolumn_pushdown) ORDER BY ALL;
SELECT tup.a FROM (SELECT other AS tup FROM t_subcolumn_pushdown) ORDER BY ALL SETTINGS optimize_push_subcolumns_into_subqueries = 0;

SELECT 'the alias reads a column of the same name';
SELECT tup.a FROM (SELECT other AS tup, tup AS other FROM t_subcolumn_pushdown) ORDER BY ALL;
SELECT tup.a FROM (SELECT other AS tup, tup AS other FROM t_subcolumn_pushdown) ORDER BY ALL SETTINGS optimize_push_subcolumns_into_subqueries = 0;

SELECT 'the same subcolumn is read twice';
SELECT tup.a, tup.a FROM (SELECT * FROM t_subcolumn_pushdown) ORDER BY ALL;
SELECT tup.a, tup.a FROM (SELECT * FROM t_subcolumn_pushdown) ORDER BY ALL SETTINGS optimize_push_subcolumns_into_subqueries = 0;

SELECT 'the cte is referenced from a correlated-free scalar subquery as well';
WITH foo AS (SELECT * FROM t_subcolumn_pushdown) SELECT (SELECT max(tup.b) FROM foo), tup.a FROM foo ORDER BY ALL;
WITH foo AS (SELECT * FROM t_subcolumn_pushdown) SELECT (SELECT max(tup.b) FROM foo), tup.a FROM foo ORDER BY ALL SETTINGS optimize_push_subcolumns_into_subqueries = 0;

SELECT 'the optimization is still applied where it is safe';
SELECT count() > 0 FROM (EXPLAIN header = 1 WITH foo AS (SELECT * FROM t_subcolumn_pushdown) SELECT tup.a FROM foo) WHERE explain LIKE '%tup.a String%';
SELECT count() = 0 FROM (EXPLAIN header = 1 WITH foo AS (SELECT * FROM t_subcolumn_pushdown) SELECT tup.a FROM foo) WHERE explain LIKE '%tup Tuple%';

SELECT 'and it reads the underlying column when the subquery renames it';
SELECT count() > 0 FROM (EXPLAIN header = 1 SELECT tup.a FROM (SELECT other AS tup FROM t_subcolumn_pushdown)) WHERE explain LIKE '%other.a String%';

SELECT 'the setting turns it off';
SELECT count() = 0 FROM (EXPLAIN header = 1 WITH foo AS (SELECT * FROM t_subcolumn_pushdown) SELECT tup.a FROM foo SETTINGS optimize_push_subcolumns_into_subqueries = 0) WHERE explain LIKE '%tup.a String%';

DROP TABLE t_subcolumn_pushdown;
