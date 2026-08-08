-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_subcolumns_cte_join;

CREATE TABLE t_push_subcolumns_cte_join (id UInt32, n Nullable(UInt32))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_subcolumns_cte_join VALUES (1, 1), (2, NULL);

-- An ordinary CTE used both in an eligible position and on the default-filled side of a
-- LEFT JOIN. The subcolumn read through the default-filled reference must not be rewritten:
-- for the non-matched row (id = 1) the JOIN fills r.n with the default NULL, whose `null`
-- subcolumn is 1, while a column added to the shared subquery would be filled with the
-- default UInt8 value 0.
SELECT 'shared CTE on the default-filled side of a LEFT JOIN, not rewritten';
SELECT trimLeft(explain) FROM (EXPLAIN QUERY TREE dump_ast = 0 WITH c AS (SELECT * FROM t_push_subcolumns_cte_join) SELECT l.id, r.n.null FROM c AS l LEFT JOIN c AS r ON l.id = r.id + 1) WHERE explain LIKE '%getSubcolumn%';
WITH c AS (SELECT * FROM t_push_subcolumns_cte_join)
SELECT l.id, r.n.null FROM c AS l LEFT JOIN c AS r ON l.id = r.id + 1 ORDER BY l.id;

-- The same shape with a subcolumn also read through the non-defaultable left reference.
-- The resolver clones an ordinary CTE per reference, so the eligible left reference may still
-- be pushed down, but the default-filled right reference must keep its `getSubcolumn`.
SELECT 'subcolumns on both sides, the default-filled side is not rewritten';
SELECT trimLeft(explain) FROM (EXPLAIN QUERY TREE dump_ast = 0 WITH c AS (SELECT * FROM t_push_subcolumns_cte_join) SELECT l.n.null, r.n.null FROM c AS l LEFT JOIN c AS r ON l.id = r.id + 1) WHERE explain LIKE '%getSubcolumn%';
WITH c AS (SELECT * FROM t_push_subcolumns_cte_join)
SELECT l.n.null, r.n.null FROM c AS l LEFT JOIN c AS r ON l.id = r.id + 1 ORDER BY l.id;

DROP TABLE t_push_subcolumns_cte_join;
