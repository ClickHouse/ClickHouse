-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_correlated;

CREATE TABLE t_push_subcolumns_correlated (id UInt32, json JSON, tup Tuple(a UInt32, b String))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_correlated VALUES (1, '{"a": 1, "b": "x"}', (1, 'one')), (2, '{"a": 2, "b": "y"}', (2, 'two'));

-- A correlated subquery keeps the whole column alive: RemoveUnusedProjectionColumnsPass preserves
-- the parent projection column, so pushing the subcolumn next to it would only make the subquery
-- read both the whole column and the subcolumn from the table.
SELECT 'correlated EXISTS uses the whole column, no pushdown';
SELECT trimLeft(explain) FROM (EXPLAIN QUERY TREE dump_ast = 0 SELECT tup.a FROM (SELECT * FROM t_push_subcolumns_correlated) AS s WHERE EXISTS (SELECT 1 WHERE s.tup = (1, 'one'))) WHERE explain LIKE '%COLUMN%tup%' OR explain LIKE '%getSubcolumn%';
SELECT tup.a FROM (SELECT * FROM t_push_subcolumns_correlated) AS s WHERE EXISTS (SELECT 1 WHERE s.tup = (1, 'one'));

SELECT 'correlated EXISTS uses another column, the subcolumn is pushed';
SELECT trimLeft(explain) FROM (EXPLAIN QUERY TREE dump_ast = 0 SELECT tup.a FROM (SELECT * FROM t_push_subcolumns_correlated) AS s WHERE EXISTS (SELECT 1 WHERE s.id = 1)) WHERE explain LIKE '%COLUMN%tup%' OR explain LIKE '%getSubcolumn%';
SELECT tup.a FROM (SELECT * FROM t_push_subcolumns_correlated) AS s WHERE EXISTS (SELECT 1 WHERE s.id = 1) ORDER BY 1;

SELECT 'correlated scalar subquery uses a subcolumn of the same column, no pushdown';
SELECT trimLeft(explain) FROM (EXPLAIN QUERY TREE dump_ast = 0 SELECT tup.a FROM (SELECT * FROM t_push_subcolumns_correlated) AS s WHERE (SELECT s.tup.b) = 'one') WHERE explain LIKE '%COLUMN%tup%' OR explain LIKE '%getSubcolumn%';
SELECT tup.a FROM (SELECT * FROM t_push_subcolumns_correlated) AS s WHERE (SELECT s.tup.b) = 'one';

DROP TABLE t_push_subcolumns_correlated;
