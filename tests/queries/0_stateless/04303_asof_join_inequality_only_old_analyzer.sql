-- Regression test for ASOF JOIN with no equality key under the old analyzer.
--
-- An `ASOF` JOIN with only an inequality (no equality key) is an invalid
-- expression that must be reported as `INVALID_JOIN_ON_EXPRESSION`. In the
-- old analyzer, the `chassert(analyzed_join.oneDisjunct())` in
-- `TreeRewriter::collectJoinedColumns` was too strict and fired in debug
-- builds before the proper `INVALID_JOIN_ON_EXPRESSION` could be thrown,
-- aborting the server. The AST fuzzer surfaced this as
-- "Logical error: 'analyzed_join.oneDisjunct()'." in builds with
-- `chassert` enabled.

DROP TABLE IF EXISTS t0_04303;
DROP TABLE IF EXISTS t1_04303;

CREATE TABLE t0_04303 (x Int, y Int) ENGINE = Memory;
CREATE TABLE t1_04303 (x Int, y Int) ENGINE = Memory;

INSERT INTO t0_04303 VALUES (1, 10);
INSERT INTO t1_04303 VALUES (1, 5);

-- Pure inequality: must throw INVALID_JOIN_ON_EXPRESSION, not abort.
SELECT * FROM t0_04303 ASOF LEFT JOIN t1_04303 ON t0_04303.y > t1_04303.y SETTINGS enable_analyzer = 0; -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- The shape the AST fuzzer found: single inequality wrapped in `and(...)`.
SELECT * FROM t0_04303 ASOF LEFT JOIN t1_04303 ON and((t0_04303.y > t1_04303.y)) SETTINGS enable_analyzer = 0; -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- Comparison of queries with enabled analyzer.

SELECT * FROM t0_04303 ASOF LEFT JOIN t1_04303 ON t0_04303.y > t1_04303.y SETTINGS enable_analyzer = 1; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM t0_04303 ASOF LEFT JOIN t1_04303 ON and((t0_04303.y > t1_04303.y)) SETTINGS enable_analyzer = 1; -- { serverError TOO_FEW_ARGUMENTS_FOR_FUNCTION }

DROP TABLE t0_04303;
DROP TABLE t1_04303;
