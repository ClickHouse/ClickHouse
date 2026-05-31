-- Regression test for block structure mismatch in removeUnusedColumns optimization.
-- After ExpressionStep::removeUnusedColumns updates the output header via ActionsDAG::updateHeader,
-- constant folding may change a column from materialized to Const representation, but the parent
-- step's input header retains the stale (materialized) representation. This causes
-- assertBlocksHaveEqualStructure to fail in debug builds.
-- The fix syncs the parent's input header with the child's output header when column names match
-- but representations differ.
-- See https://github.com/ClickHouse/ClickHouse/issues/92845

-- Minimal reproducer: a nullable expression referencing columns from both sides of the join
-- becomes Const after constant folding when the WHERE clause makes the inner comparison always true.
-- DISTINCT (or GROUP BY ALL) is needed to create a plan structure where removeUnusedColumns
-- triggers the mismatch.
SELECT DISTINCT and(t1.b = t2.b, NULL), t2.a FROM (SELECT 1 AS a, 2 AS b) t1 INNER JOIN (SELECT 1 AS a, 2 AS b) t2 ON t1.a = t2.a WHERE t1.b = t2.b;

-- Same pattern with explicit CAST to Nullable
SELECT DISTINCT CAST(t1.b = t2.b AS Nullable(UInt8)), t2.a FROM (SELECT 1 AS a, 2 AS b) t1 INNER JOIN (SELECT 1 AS a, 2 AS b) t2 ON t1.a = t2.a WHERE t1.b = t2.b;

-- GROUP BY ALL instead of DISTINCT
SELECT and(t1.b = t2.b, NULL), t2.a FROM (SELECT 1 AS a, 2 AS b) t1 INNER JOIN (SELECT 1 AS a, 2 AS b) t2 ON t1.a = t2.a WHERE t1.b = t2.b GROUP BY ALL;

-- Multiple nullable expressions
SELECT DISTINCT and(t1.b = t2.b, NULL), or(t1.b = t2.b, NULL), t2.a FROM (SELECT 1 AS a, 2 AS b) t1 INNER JOIN (SELECT 1 AS a, 2 AS b) t2 ON t1.a = t2.a WHERE t1.b = t2.b;

-- Also exercise the original test patterns: joins where no output columns are used
-- (triggers __join_result_dummy path in JoinStepLogical).
SELECT count() FROM (SELECT 1 AS a) AS t1 INNER JOIN (SELECT 1 AS a) AS t2 ON t1.a = t2.a;
SELECT 1 FROM (SELECT 1 AS a) AS t1 INNER JOIN (SELECT 1 AS a) AS t2 ON t1.a = t2.a;
SELECT count() FROM (SELECT * FROM (SELECT 1 AS a) AS t1 INNER JOIN (SELECT 1 AS a) AS t2 ON t1.a = t2.a);
SELECT count() FROM (SELECT 1 AS x FROM (SELECT 1 AS a) AS t1 INNER JOIN (SELECT 1 AS a) AS t2 ON t1.a = t2.a);

-- Various join types
SELECT count() FROM (SELECT 1 AS a) AS t1 LEFT JOIN (SELECT 1 AS a) AS t2 ON t1.a = t2.a;
SELECT count() FROM (SELECT 1 AS a) AS t1 CROSS JOIN (SELECT 1 AS a) AS t2;
