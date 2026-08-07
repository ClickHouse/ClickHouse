-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/103785
-- DISTINCT inside a CTE branch was being silently weakened when an outer
-- query projected only a subset of the CTE's columns. The new analyzer's
-- `RemoveUnusedProjectionColumnsPass` correctly skipped DISTINCT
-- query-as-FROM-children, but did not skip DISTINCT query-as-children
-- of a `UNION ALL`, so the unused column was pruned out of the inner
-- DISTINCT projection — turning `SELECT DISTINCT ID, Qty` into
-- effectively `SELECT DISTINCT Qty` and dropping rows that share the
-- same `Qty` value but have different `ID`.

DROP TABLE IF EXISTS t_qty_103785;
CREATE TABLE t_qty_103785 (ID UInt32, Qty UInt64) ENGINE = Memory;
INSERT INTO t_qty_103785 VALUES (115, 100000), (116, 130000), (117, 150000), (118, 150000);

SELECT '-- Query A: full projection (ID, Qty) - was already correct';
WITH a AS (
    SELECT DISTINCT ID, Qty FROM t_qty_103785 WHERE ID IN (116, 117, 118)
    UNION ALL
    SELECT ID, Qty FROM t_qty_103785 WHERE ID = 115
)
SELECT ID, Qty FROM a ORDER BY ID;

SELECT '-- Query B: subset projection (Qty only) - used to drop a row';
WITH a AS (
    SELECT DISTINCT ID, Qty FROM t_qty_103785 WHERE ID IN (116, 117, 118)
    UNION ALL
    SELECT ID, Qty FROM t_qty_103785 WHERE ID = 115
)
SELECT Qty FROM a ORDER BY Qty;

SELECT '-- Query C: subset projection through nested SELECT - used to drop a row';
WITH a AS (
    SELECT DISTINCT ID, Qty FROM t_qty_103785 WHERE ID IN (116, 117, 118)
    UNION ALL
    SELECT ID, Qty FROM t_qty_103785 WHERE ID = 115
)
SELECT Qty FROM (SELECT ID, Qty FROM a) ORDER BY Qty;

SELECT '-- Query D: DISTINCT in second branch of UNION ALL';
WITH a AS (
    SELECT ID, Qty FROM t_qty_103785 WHERE ID = 115
    UNION ALL
    SELECT DISTINCT ID, Qty FROM t_qty_103785 WHERE ID IN (116, 117, 118)
)
SELECT Qty FROM a ORDER BY Qty;

SELECT '-- Query E: DISTINCT inside a nested UNION ALL';
WITH a AS (
    SELECT ID, Qty FROM t_qty_103785 WHERE ID = 115
    UNION ALL
    (
        SELECT DISTINCT ID, Qty FROM t_qty_103785 WHERE ID IN (116, 117, 118)
        UNION ALL
        SELECT ID, Qty FROM t_qty_103785 WHERE ID = 117
    )
)
SELECT Qty FROM a ORDER BY Qty;

SELECT '-- Query F: workaround setting still works as before';
WITH a AS (
    SELECT DISTINCT ID, Qty FROM t_qty_103785 WHERE ID IN (116, 117, 118)
    UNION ALL
    SELECT ID, Qty FROM t_qty_103785 WHERE ID = 115
)
SELECT Qty FROM a ORDER BY Qty SETTINGS enable_analyzer = 0, optimize_duplicate_order_by_and_distinct = 0;

SELECT '-- Query G: existing 03023 case - DISTINCT as direct FROM-child (already worked)';
SELECT product_id
FROM (
    SELECT DISTINCT product_id, section_id
    FROM (
        SELECT
            concat('product_', number % 2) AS product_id,
            concat('section_', number % 3) AS section_id
        FROM numbers(10)
    )
)
ORDER BY product_id;

SELECT '-- Query H: den-crane VALUES-based count(c2) repro from PR #104114';
-- Pre-fix: count(c2) returned 2 because the inner SELECT DISTINCT c1, c2 had
-- its `c1` column pruned by `RemoveUnusedProjectionColumnsPass` (the outer
-- query only references `c2`), turning the DISTINCT into `DISTINCT c2` and
-- collapsing (117, 150000) and (118, 150000) into a single row.
-- Post-fix: returns 3 (2 distinct rows from the LHS + 1 row from the RHS).
SELECT count(c2) FROM
(
    SELECT DISTINCT c1, c2 FROM VALUES ((117, 150000), (118, 150000))
    UNION ALL
    SELECT c1, c2 FROM VALUES ((118, 150000))
);

SELECT '-- Query I: den-crane VALUES-based count() repro from PR #104114';
-- Same shape as Query H but with `count()` (no argument) instead of
-- `count(c2)`. Pre-fix: returned 2 because `RemoveUnusedProjectionColumnsPass`
-- pruned both `c1` and `c2` from the inner `SELECT DISTINCT c1, c2` (the outer
-- `count()` references no column at all), collapsing the entire DISTINCT side
-- into a single empty-projection row.
-- Post-fix: returns 3 (2 distinct rows from the LHS DISTINCT side + 1 from the RHS).
SELECT count() FROM
(
    SELECT DISTINCT c1, c2 FROM VALUES ((1, 1), (1, 2))
    UNION ALL
    SELECT c1, c2 FROM VALUES ((1, 1))
);

DROP TABLE t_qty_103785;
