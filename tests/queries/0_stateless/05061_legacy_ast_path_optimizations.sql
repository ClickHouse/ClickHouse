-- Coverage tests for two old AST-path optimizers bypassed when enable_analyzer=1 (default):
--   1. SubstituteColumnOptimizer.cpp lines 239-317: substitutes equivalent columns
--      using ASSUME constraints, choosing the cheapest one by column size.
--   2. RewriteSumFunctionWithSumAndCountVisitor.cpp lines 43-44, 108-124:
--      rewrites sum(k + col) → k*count(col) + sum(col) when literal is the first operand.
-- Tags: no-parallel-replicas

-- ===========================================================================
-- 1. SubstituteColumnOptimizer: a = b constraint → substitute b with a (a is ORDER BY key)
-- ===========================================================================
CREATE TABLE t_subst_legacy (a UInt64, b UInt64, CONSTRAINT c1 ASSUME a = b)
ENGINE = MergeTree ORDER BY a;
INSERT INTO t_subst_legacy SELECT number, number FROM numbers(100);

SET enable_analyzer = 0;
SET convert_query_to_cnf = 1;
SET optimize_using_constraints = 1;
SET optimize_substitute_columns = 1;

-- b is substituted with a (a is in the primary key → cheaper)
-- Verify substitution happened: result is correct and the query executes without errors
SELECT a, b FROM t_subst_legacy WHERE b = 42;

-- Multiple references to b
SELECT b, b + 1 FROM t_subst_legacy WHERE b < 3 ORDER BY b;

-- PREWHERE clause: exercises lines 243-244 of SubstituteColumnOptimizer (refPrewhere path)
SELECT a, b FROM t_subst_legacy PREWHERE b < 50 WHERE b = 42;

-- HAVING clause with aggregate: exercises lines 247-248 of SubstituteColumnOptimizer (refHaving path)
-- count() in HAVING cannot be moved to WHERE by CNF, so having() is non-null when perform() runs
SELECT a, b FROM t_subst_legacy GROUP BY a, b HAVING count() > 0 ORDER BY a LIMIT 3;

DROP TABLE t_subst_legacy;

-- ===========================================================================
-- 2. RewriteSumFunctionWithSumAndCount: literal-first path (column_id = 1)
-- sum(k + col) → plus(multiply(k, count(col)), sum(col))
-- ===========================================================================
CREATE TABLE t_sum_legacy (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sum_legacy SELECT number FROM numbers(20);

SET enable_analyzer = 0;
SET optimize_arithmetic_operations_in_aggregate_functions = 1;

-- Literal first (triggers column_id=1 branch, lines 43-44 and 108-124)
EXPLAIN SYNTAX SELECT sum(3 + x) FROM t_sum_legacy;
SELECT sum(3 + x) FROM t_sum_legacy;

-- Verify result matches the unoptimized form
SET optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT sum(3 + x) FROM t_sum_legacy;

DROP TABLE t_sum_legacy;
