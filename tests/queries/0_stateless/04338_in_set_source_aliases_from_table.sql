-- Tags: no-parallel-replicas

-- Regression test for "No set is registered for key" LOGICAL_ERROR.
-- A subquery used both as a FROM table expression and as the right argument of an
-- IN / NOT IN / GLOBAL IN function is one shared query-tree node. The set is registered in
-- PreparedSets keyed by that node's tree hash; planning the node as the FROM source then
-- mutates it in place (e.g. the QUALIFY -> HAVING -> WHERE rewrite, or constant-WHERE
-- removal), changing its tree hash so the later set lookup aborted with "No set is registered".

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t04338;
CREATE TABLE t04338 (y Int32) ENGINE = MergeTree ORDER BY y;
INSERT INTO t04338 SELECT number FROM numbers(5);

-- y IN (the same source): every row of the source is in the source, so all 5 rows pass.
SELECT y FROM (SELECT y FROM t04338 QUALIFY 1) AS t04338 WHERE y IN t04338 ORDER BY y;
SELECT '---';
-- y NOT IN (the same source): no row passes.
SELECT count() FROM (SELECT y FROM t04338 QUALIFY 1) AS t04338 WHERE y NOT IN t04338;
SELECT '---';
-- GLOBAL IN over the shared source alias, also exercised under QUALIFY.
SELECT y FROM (SELECT y FROM t04338 QUALIFY 1) AS src WHERE y GLOBAL IN src ORDER BY y;
SELECT '---';
SELECT count() FROM (SELECT y FROM t04338 QUALIFY 1) AS t04338 QUALIFY globalNotIn((SELECT 1), t04338);

DROP TABLE t04338;
