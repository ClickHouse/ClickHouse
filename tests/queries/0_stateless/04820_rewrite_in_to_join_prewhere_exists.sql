-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/114026
-- rewrite_in_to_join rewrites "x IN (subquery)" into an EXISTS FunctionNode that is
-- resolved outside FunctionFactory. When PREWHERE later calls rerunFunctionResolve on
-- that node it used to call FunctionFactory::instance().get("exists", ...) and throw
-- Code: 46. DB::Exception: Unknown function exists. (UNKNOWN_FUNCTION).
-- The same query with WHERE (control arm) always worked.

CREATE TABLE t_114026 (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_114026 SELECT number, toString(number % 2) FROM numbers(1000);

-- PREWHERE IN (subquery) must not throw and must return the same result as WHERE
SELECT count() FROM t_114026 PREWHERE s IN (SELECT '1')     SETTINGS rewrite_in_to_join = 1, allow_experimental_correlated_subqueries = 1;
SELECT count() FROM t_114026 WHERE   s IN (SELECT '1')     SETTINGS rewrite_in_to_join = 1, allow_experimental_correlated_subqueries = 1;

-- NOT IN variant
SELECT count() FROM t_114026 PREWHERE s NOT IN (SELECT '1') SETTINGS rewrite_in_to_join = 1, allow_experimental_correlated_subqueries = 1;

DROP TABLE t_114026;
