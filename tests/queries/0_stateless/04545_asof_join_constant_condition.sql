-- Regression test for ASOF JOIN with a constant (always-NULL) ON expression.
--
-- An expression like `ON 1 = NULL` is folded to a constant, and the join
-- was marked as a join with constant, leaving `table_join_clauses` empty.
-- For ASOF strictness this tripped `chassert(table_join_clauses.size() == 1)`
-- in `buildPhysicalJoinImpl`, aborting debug builds with
-- "Logical error: 'table_join_clauses.size() == 1'." (found by the AST fuzzer:
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=108289&sha=6abb6c1cbb3741d85328de6b80f9979e2dc8029a&name_0=PR&name_1=AST%20fuzzer%20%28amd_debug%2C%20targeted%29 ).
-- A constant ON expression cannot contain the inequality predicate required
-- by ASOF JOIN, so it must be rejected with INVALID_JOIN_ON_EXPRESSION.

DROP TABLE IF EXISTS t0_04545;
DROP TABLE IF EXISTS t1_04545;

CREATE TABLE t0_04545 (id Int) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t1_04545 (id Int) ENGINE = Memory;

INSERT INTO t0_04545 VALUES (1), (2);

-- Minimal repro: must throw INVALID_JOIN_ON_EXPRESSION, not abort.
SELECT * FROM t0_04545 ASOF LEFT JOIN t1_04545 ON 1 = NULL SETTINGS enable_analyzer = 1; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT * FROM t0_04545 ASOF INNER JOIN t1_04545 ON 1 = NULL SETTINGS enable_analyzer = 1; -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- The shape the AST fuzzer found.
SELECT * FROM t0_04545 GLOBAL ASOF LEFT JOIN t1_04545 ON 9223372036854775806 = NULL ORDER BY ALL ASC LIMIT 614 SETTINGS enable_analyzer = 1; -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- Always-true constant expression is rejected earlier during analysis.
SELECT * FROM t0_04545 ASOF LEFT JOIN t1_04545 ON 1 SETTINGS enable_analyzer = 1; -- { serverError INVALID_JOIN_ON_EXPRESSION }

DROP TABLE t0_04545;
DROP TABLE t1_04545;
