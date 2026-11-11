-- Regression test for issue #100695
-- ROW POLICY with a subquery in the USING clause caused a LOGICAL_ERROR crash
-- in QueryAnalyzer::resolve() because the subquery appeared as a top-level
-- QUERY/UNION node with table_expression set.  The fix wraps standalone
-- subquery filters with notEquals(<subquery>, 0) in buildFilterInfo() so they
-- are resolved as normal scalar-subquery function arguments while preserving
-- boolean semantics (any non-zero value is truthy).

DROP TABLE IF EXISTS t_04073;
DROP ROW POLICY IF EXISTS p_04073 ON t_04073;

CREATE TABLE t_04073 (c0 Int) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO t_04073 VALUES (1), (2), (3);

-- Test 1: USING (SELECT 1) — all rows visible (filter always true)
CREATE ROW POLICY p_04073 ON t_04073 USING (SELECT 1) TO ALL;
SELECT * FROM t_04073 ORDER BY c0;
DROP ROW POLICY p_04073 ON t_04073;

-- Test 2: USING (SELECT 0) — no rows visible (filter always false)
CREATE ROW POLICY p_04073 ON t_04073 USING (SELECT 0) TO ALL;
SELECT count() FROM t_04073;
DROP ROW POLICY p_04073 ON t_04073;

-- Test 3: USING (SELECT 2) — all rows visible (non-zero = truthy)
CREATE ROW POLICY p_04073 ON t_04073 USING (SELECT 2) TO ALL;
SELECT * FROM t_04073 ORDER BY c0;
DROP ROW POLICY p_04073 ON t_04073;

-- Test 4: Mixed expression with scalar subquery — c0 > (SELECT 1)
CREATE ROW POLICY p_04073 ON t_04073 USING c0 > (SELECT 1) TO ALL;
SELECT * FROM t_04073 ORDER BY c0;
DROP ROW POLICY p_04073 ON t_04073;

-- Test 5: USING NULL — NULL is falsy, no rows visible
CREATE ROW POLICY p_04073 ON t_04073 USING NULL TO ALL;
SELECT count() FROM t_04073;
DROP ROW POLICY p_04073 ON t_04073;

-- Test 6: USING toNullable(1) — nullable truthy, all rows visible
CREATE ROW POLICY p_04073 ON t_04073 USING toNullable(1) TO ALL;
SELECT * FROM t_04073 ORDER BY c0;
DROP ROW POLICY p_04073 ON t_04073;

-- Test 7: USING (SELECT NULL) — NULL subquery is falsy, no rows visible
CREATE ROW POLICY p_04073 ON t_04073 USING (SELECT NULL) TO ALL;
SELECT count() FROM t_04073;
DROP ROW POLICY p_04073 ON t_04073;

-- Test 8: USING (SELECT toNullable(1)) — nullable subquery, truthy, all rows visible
CREATE ROW POLICY p_04073 ON t_04073 USING (SELECT toNullable(1)) TO ALL;
SELECT * FROM t_04073 ORDER BY c0;
DROP ROW POLICY p_04073 ON t_04073;

DROP TABLE t_04073;
