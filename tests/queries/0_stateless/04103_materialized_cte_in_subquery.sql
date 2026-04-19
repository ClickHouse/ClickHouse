-- Test that IN-subqueries referencing materialized CTEs do not crash when
-- evaluated eagerly during plan optimization (via KeyCondition /
-- FutureSetFromSubquery::buildOrderedSetInplace, or via
-- DelayedCreatingSetsStep::makePlansForSets), and that multiple sub-pipelines
-- reading the same CTE never race against its materialization.
--
-- Regression test for #101940 and #102320. See
-- DelayedMaterializingCTEsStep::makePlansForCTEs for the mechanism: each
-- claimed CTE is materialized synchronously inside the optimization pass so
-- any subsequent reader -- an eager sub-pipeline fired from the same
-- plan.optimize() call, a sibling UNION branch, or the main pipeline itself
-- -- sees is_built=true and reads populated storage.

SET enable_materialized_cte = 1;

DROP TABLE IF EXISTS t_04103;
CREATE TABLE t_04103 (c Int32) ENGINE = MergeTree ORDER BY c;
INSERT INTO t_04103 SELECT number FROM numbers(100);

-- Case 1 (issue #101940): the IN-subquery is inside a materialized CTE's
-- body, and the outer CTE is referenced more than once in the main query.
-- The inner sub-plan for the IN fires during the outer CTE's plan
-- optimization.
WITH
    ct AS MATERIALIZED (SELECT c FROM t_04103 LIMIT 10),
    rs AS MATERIALIZED (SELECT * FROM t_04103 WHERE c IN (SELECT c FROM ct))
SELECT count() FROM rs AS a ANY LEFT JOIN rs AS b USING c;

-- Case 2 (issue #102320): nested IN-subqueries reaching back to a CTE
-- through a MergeTree table. The inner sub-plan fires via
-- `DelayedCreatingSetsStep::makePlansForSets` → outer subquery's
-- `optimize` → `KeyCondition` on `test`.
DROP TABLE IF EXISTS test_04103;
CREATE TABLE test_04103 (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_04103 SELECT number FROM numbers(1000);

WITH a AS MATERIALIZED (SELECT number AS id FROM numbers(100))
SELECT count() FROM a
WHERE id IN (
    SELECT id FROM test_04103
    WHERE id IN (SELECT id FROM a GROUP BY id)
    GROUP BY id
);

-- Case 3: same shape as Case 1 but with an ANY INNER JOIN.
WITH
    ct AS MATERIALIZED (SELECT c FROM t_04103 LIMIT 10),
    rs AS MATERIALIZED (SELECT * FROM t_04103 WHERE c IN (SELECT c FROM ct))
SELECT count() FROM rs AS a ANY INNER JOIN rs AS b USING c;

-- Case 4: UNION of two branches that both reference the same materialized
-- CTE. Exercises the same-CTE-in-multiple-sub-Planner-plans shape; with
-- synchronous materialization in `makePlansForCTEs`, the first claimer
-- populates the storage and sibling branches read from it without racing.
WITH ct AS MATERIALIZED (SELECT c FROM t_04103 LIMIT 10)
SELECT count() FROM (
    SELECT c FROM ct WHERE c < 5
    UNION ALL
    SELECT c FROM ct WHERE c >= 5
);

-- Case 5: two independent IN-subqueries referencing the same CTE. The first
-- set source's `makePlansForCTEs` materializes; the second finds
-- `is_built=true` and simply reads the populated storage.
WITH ct AS MATERIALIZED (SELECT c FROM t_04103 LIMIT 10)
SELECT count() FROM t_04103
WHERE c IN (SELECT c FROM ct WHERE c < 5)
  AND c IN (SELECT c FROM ct WHERE c < 8);

-- Case 6: JOIN of two subqueries both referencing the same CTE. Both
-- references live in a single main query tree, so there's one
-- `DelayedMaterializingCTEsStep` at the top; this case still exercises
-- synchronous materialization from the normal main-plan path.
WITH ct AS MATERIALIZED (SELECT c FROM t_04103 LIMIT 10)
SELECT count() FROM
    (SELECT c FROM ct WHERE c < 5) AS a
    INNER JOIN (SELECT c FROM ct WHERE c < 8) AS b ON a.c = b.c;

DROP TABLE t_04103;
DROP TABLE test_04103;
