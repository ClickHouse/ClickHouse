-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_shared_cte;

CREATE TABLE t_push_subcolumns_shared_cte (id UInt32, json JSON, tup Tuple(a UInt32, b String))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_shared_cte VALUES (1, '{"a": 1, "b": "x"}', (1, 'one')), (2, '{"a": 2, "b": "y"}', (2, 'two'));

-- An ordinary CTE referenced several times, with an extra subquery layer inside. Different
-- subcolumns requested through different references must all be pushed down to the base table.
SELECT 'reused CTE in UNION ALL branches, both subcolumns are pushed to the table';
SELECT trimLeft(explain) FROM (EXPLAIN QUERY TREE dump_ast = 0 WITH c AS (SELECT * FROM (SELECT * FROM t_push_subcolumns_shared_cte)) SELECT tup.a::String FROM c UNION ALL SELECT tup.b FROM c) WHERE explain LIKE '%COLUMN%tup%' OR explain LIKE '%getSubcolumn%';
SELECT * FROM (WITH c AS (SELECT * FROM (SELECT * FROM t_push_subcolumns_shared_cte)) SELECT tup.a::String AS v FROM c UNION ALL SELECT tup.b FROM c) ORDER BY v;

SELECT 'reused CTE on both sides of a JOIN, both subcolumns are pushed to the table';
SELECT trimLeft(explain) FROM (EXPLAIN QUERY TREE dump_ast = 0 WITH c AS (SELECT * FROM (SELECT * FROM t_push_subcolumns_shared_cte)) SELECT l.tup.a, r.tup.b FROM c AS l INNER JOIN c AS r USING id) WHERE explain LIKE '%COLUMN%tup%' OR explain LIKE '%getSubcolumn%';
WITH c AS (SELECT * FROM (SELECT * FROM t_push_subcolumns_shared_cte)) SELECT l.tup.a, r.tup.b FROM c AS l INNER JOIN c AS r USING id ORDER BY 1;

SELECT 'reused CTE in sibling scalar subqueries';
WITH c AS (SELECT * FROM (SELECT * FROM t_push_subcolumns_shared_cte) WHERE id = 1) SELECT (SELECT tup.a FROM c), (SELECT tup.b FROM c);

DROP TABLE t_push_subcolumns_shared_cte;
