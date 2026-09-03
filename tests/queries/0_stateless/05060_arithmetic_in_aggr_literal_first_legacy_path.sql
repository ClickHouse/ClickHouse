-- Coverage test for ArithmeticOperationsInAgrFuncOptimize.cpp — literal-first and negative-reversal paths
-- With enable_analyzer=1 (default), the equivalent pass runs in the analyzer.
-- Exercises:
--   lines 31-44  exchangeExtractFirstArgument (literal is the first operand)
--   lines 67     zeroField for Int64 literals (negative literal comparison)
--   lines 111-116 get_reverse_aggregate_function_name (min↔max flip, and sum passthrough)
--   lines 115    get_reverse returns unchanged name for sum (sum(-2*a) → need_reverse=true but sum stays sum)
--   lines 118-129 first_literal && !second_literal branch
--   lines 122    literal-first divide skipped (no optimization for sum(k/col))
--   lines 134-135 need_reverse for second_literal (negative literal, second operand)
-- Tags: no-parallel-replicas

CREATE TABLE t_arith_legacy (a Int64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_arith_legacy SELECT (number::Int64 - 5) FROM numbers(10);

SET enable_analyzer = 0;
SET optimize_arithmetic_operations_in_aggregate_functions = 1;

-- 1. Literal-first multiply: sum(2 * a) → multiply(2, sum(a))
EXPLAIN SYNTAX SELECT sum(2 * a) FROM t_arith_legacy;
SELECT sum(2 * a) FROM t_arith_legacy;

-- 2. min with literal-first plus: min(1 + a) → plus(1, min(a))
EXPLAIN SYNTAX SELECT min(1 + a) FROM t_arith_legacy;
SELECT min(1 + a) FROM t_arith_legacy;

-- 3. min with literal-first minus: min(1 - a) → need_reverse → minus(1, max(a))
EXPLAIN SYNTAX SELECT min(1 - a) FROM t_arith_legacy;
SELECT min(1 - a) FROM t_arith_legacy;

-- 4. max with negative first-literal multiply: max(-2 * a) → need_reverse(max→min) → multiply(-2, min(a))
--    (zeroField Int64 line 67, need_reverse=true, get_reverse_aggregate_function_name: max→min)
EXPLAIN SYNTAX SELECT max(-2 * a) FROM t_arith_legacy;
SELECT max(-2 * a) FROM t_arith_legacy;

-- 5. min with negative first-literal multiply: min(-3 * a) → need_reverse(min→max) → multiply(-3, max(a))
EXPLAIN SYNTAX SELECT min(-3 * a) FROM t_arith_legacy;
SELECT min(-3 * a) FROM t_arith_legacy;

-- 6. max with negative second-literal multiply: max(a * -2) → need_reverse → multiply(min(a), -2)
--    (need_reverse for second_literal branch, lines 134-135)
EXPLAIN SYNTAX SELECT max(a * -2) FROM t_arith_legacy;
SELECT max(a * -2) FROM t_arith_legacy;

-- 7. sum with negative first-literal: sum(-2 * a) → need_reverse=true but get_reverse("sum")="sum" (line 115)
--    result: multiply(-2, sum(a))
EXPLAIN SYNTAX SELECT sum(-2 * a) FROM t_arith_legacy;
SELECT sum(-2 * a) FROM t_arith_legacy;

-- 8. Literal-first divide is skipped (line 122: return {}): EXPLAIN shows unchanged form
--    Use a+10 (values 5..14) to avoid division-by-zero at runtime
EXPLAIN SYNTAX SELECT min(2 / (a + 10)) FROM t_arith_legacy;
SELECT min(2 / (a + 10)) FROM t_arith_legacy;

DROP TABLE t_arith_legacy;
