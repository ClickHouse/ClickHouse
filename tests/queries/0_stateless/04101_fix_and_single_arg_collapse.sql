-- Test for AND single-argument collapse after deduplication in LogicalExpressionOptimizerPass.
-- When all OR arguments deduplicate to a single expression, the wrapping AND(single_expr)
-- should collapse to just that expression, not produce an invalid single-arg AND node.

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int32) ENGINE = Memory;
INSERT INTO t0 VALUES (1), (2), (3);

-- Case 1: Full degeneration - issue #99832 original scenario
-- OR(c0>=c0, c0>=c0) deduplicates to single (c0>=c0), AND wrapping must collapse
SELECT c0 FROM t0 WHERE ((c0 >= c0) OR (c0 >= c0)) AND (c0 >= c0) ORDER BY c0;

-- Case 2: Partial degeneration - AND(OR(A,A), A, B) should become AND(A, B)
SELECT c0 FROM t0 WHERE ((c0 > 0) OR (c0 > 0)) AND (c0 > 0) AND (c0 < 10) ORDER BY c0;

-- Case 3: No degeneration - normal AND expression should be unaffected
SELECT c0 FROM t0 WHERE (c0 > 0) AND (c0 < 10) ORDER BY c0;

DROP TABLE t0;
