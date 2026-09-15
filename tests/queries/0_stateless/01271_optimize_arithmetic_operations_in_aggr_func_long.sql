-- Tags: long

SET enable_analyzer = 1;
SET optimize_arithmetic_operations_in_aggregate_functions = 1;

EXPLAIN SYNTAX SELECT sum(n + 1), sum(1 + n), sum(n - 1), sum(1 - n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT sum(n * 2), sum(2 * n), sum(n / 2), sum(1 / n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT min(n + 1), min(1 + n), min(n - 1), min(1 - n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT min(n * 2), min(2 * n), min(n / 2), min(1 / n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT max(n + 1), max(1 + n), max(n - 1), max(1 - n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT max(n * 2), max(2 * n), max(n / 2), max(1 / n) FROM (SELECT number n FROM numbers(10));

EXPLAIN SYNTAX SELECT sum(n + -1), sum(-1 + n), sum(n - -1), sum(-1 - n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT sum(n * -2), sum(-2 * n), sum(n / -2), sum(-1 / n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT min(n + -1), min(-1 + n), min(n - -1), min(-1 - n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT min(n * -2), min(-2 * n), min(n / -2), min(-1 / n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT max(n + -1), max(-1 + n), max(n - -1), max(-1 - n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT max(n * -2), max(-2 * n), max(n / -2), max(-1 / n) FROM (SELECT number n FROM numbers(10));

EXPLAIN SYNTAX SELECT sum(abs(2) + 1), sum(abs(2) + n), sum(n - abs(2)), sum(1 - abs(2)) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT sum(abs(2) * 2), sum(abs(2) * n), sum(n / abs(2)), sum(1 / abs(2)) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT min(abs(2) + 1), min(abs(2) + n), min(n - abs(2)), min(1 - abs(2)) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT min(abs(2) * 2), min(abs(2) * n), min(n / abs(2)), min(1 / abs(2)) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT max(abs(2) + 1), max(abs(2) + n), max(n - abs(2)), max(1 - abs(2)) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT max(abs(2) * 2), max(abs(2) * n), max(n / abs(2)), max(1 / abs(2)) FROM (SELECT number n FROM numbers(10));

EXPLAIN SYNTAX SELECT sum(abs(n) + 1), sum(abs(n) + n), sum(n - abs(n)), sum(1 - abs(n)) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT sum(abs(n) * 2), sum(abs(n) * n), sum(n / abs(n)), sum(1 / abs(n)) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT min(abs(n) + 1), min(abs(n) + n), min(n - abs(n)), min(1 - abs(n)) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT min(abs(n) * 2), min(abs(n) * n), min(n / abs(n)), min(1 / abs(n)) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT max(abs(n) + 1), max(abs(n) + n), max(n - abs(n)), max(1 - abs(n)) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT max(abs(n) * 2), max(abs(n) * n), max(n / abs(n)), max(1 / abs(n)) FROM (SELECT number n FROM numbers(10));

EXPLAIN SYNTAX SELECT sum(n*n + 1), sum(1 + n*n), sum(n*n - 1), sum(1 - n*n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT sum(n*n * 2), sum(2 * n*n), sum(n*n / 2), sum(1 / n*n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT min(n*n + 1), min(1 + n*n), min(n*n - 1), min(1 - n*n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT min(n*n * 2), min(2 * n*n), min(n*n / 2), min(1 / n*n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT max(n*n + 1), max(1 + n*n), max(n*n - 1), max(1 - n*n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT max(n*n * 2), max(2 * n*n), max(n*n / 2), max(1 / n*n) FROM (SELECT number n FROM numbers(10));

EXPLAIN SYNTAX SELECT sum(1 + n + 1), sum(1 + 1 + n), sum(1 + n - 1), sum(1 + 1 - n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT sum(1 + n * 2), sum(1 + 2 * n), sum(1 + n / 2), sum(1 + 1 / n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT min(1 + n + 1), min(1 + 1 + n), min(1 + n - 1), min(1 + 1 - n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT min(1 + n * 2), min(1 + 2 * n), min(1 + n / 2), min(1 + 1 / n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT max(1 + n + 1), max(1 + 1 + n), max(1 + n - 1), max(1 + 1 - n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT max(1 + n * 2), max(1 + 2 * n), max(1 + n / 2), max(1 + 1 / n) FROM (SELECT number n FROM numbers(10));

EXPLAIN SYNTAX SELECT sum(n + -1 + -1), sum(-1 + n + -1), sum(n - -1 + -1), sum(-1 - n + -1) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT sum(n * -2 * -1), sum(-2 * n * -1), sum(n / -2 / -1), sum(-1 / n / -1) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT min(n + -1 + -1), min(-1 + n + -1), min(n - -1 + -1), min(-1 - n + -1) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT min(n * -2 * -1), min(-2 * n * -1), min(n / -2 / -1), min(-1 / n / -1) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT max(n + -1 + -1), max(-1 + n + -1), max(n - -1 + -1), max(-1 - n + -1) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT max(n * -2 * -1), max(-2 * n * -1), max(n / -2 / -1), max(-1 / n / -1) FROM (SELECT number n FROM numbers(10));

EXPLAIN SYNTAX SELECT sum(n + 1) + sum(1 + n) + sum(n - 1) + sum(1 - n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT sum(n * 2) + sum(2 * n) + sum(n / 2) + sum(1 / n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT min(n + 1) + min(1 + n) + min(n - 1) + min(1 - n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT min(n * 2) + min(2 * n) + min(n / 2) + min(1 / n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT max(n + 1) + max(1 + n) + max(n - 1) + max(1 - n) FROM (SELECT number n FROM numbers(10));
EXPLAIN SYNTAX SELECT max(n * 2) + max(2 * n) + max(n / 2) + max(1 / n) FROM (SELECT number n FROM numbers(10));


SELECT sum(n + 1), sum(1 + n), sum(n - 1), sum(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT sum(n * 2), sum(2 * n), sum(n / 2), sum(1 / n) FROM (SELECT number n FROM numbers(10));
SELECT min(n + 1), min(1 + n), min(n - 1), min(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT min(n * 2), min(2 * n), min(n / 2), min(1 / n) FROM (SELECT number n FROM numbers(10));
SELECT max(n + 1), max(1 + n), max(n - 1), max(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT max(n * 2), max(2 * n), max(n / 2), max(1 / n) FROM (SELECT number n FROM numbers(10));

SELECT sum(n + -1), sum(-1 + n), sum(n - -1), sum(-1 - n) FROM (SELECT number n FROM numbers(10));
SELECT sum(n * -2), sum(-2 * n), sum(n / -2), sum(-1 / n) FROM (SELECT number n FROM numbers(10));
SELECT min(n + -1), min(-1 + n), min(n - -1), min(-1 - n) FROM (SELECT number n FROM numbers(10));
SELECT min(n * -2), min(-2 * n), min(n / -2), min(-1 / n) FROM (SELECT number n FROM numbers(10));
SELECT max(n + -1), max(-1 + n), max(n - -1), max(-1 - n) FROM (SELECT number n FROM numbers(10));
SELECT max(n * -2), max(-2 * n), max(n / -2), max(-1 / n) FROM (SELECT number n FROM numbers(10));

SELECT sum(abs(2) + 1), sum(abs(2) + n), sum(n - abs(2)), sum(1 - abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT sum(abs(2) * 2), sum(abs(2) * n), sum(n / abs(2)), sum(1 / abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT min(abs(2) + 1), min(abs(2) + n), min(n - abs(2)), min(1 - abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT min(abs(2) * 2), min(abs(2) * n), min(n / abs(2)), min(1 / abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT max(abs(2) + 1), max(abs(2) + n), max(n - abs(2)), max(1 - abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT max(abs(2) * 2), max(abs(2) * n), max(n / abs(2)), max(1 / abs(2)) FROM (SELECT number n FROM numbers(10));

SELECT sum(abs(n) + 1), sum(abs(n) + n), sum(n - abs(n)), sum(1 - abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT sum(abs(n) * 2), sum(abs(n) * n), sum(n / abs(n)), sum(1 / abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT min(abs(n) + 1), min(abs(n) + n), min(n - abs(n)), min(1 - abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT min(abs(n) * 2), min(abs(n) * n), min(n / abs(n)), min(1 / abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT max(abs(n) + 1), max(abs(n) + n), max(n - abs(n)), max(1 - abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT max(abs(n) * 2), max(abs(n) * n), max(n / abs(n)), max(1 / abs(n)) FROM (SELECT number n FROM numbers(10));

SELECT sum(n*n + 1), sum(1 + n*n), sum(n*n - 1), sum(1 - n*n) FROM (SELECT number n FROM numbers(10));
SELECT sum(n*n * 2), sum(2 * n*n), sum(n*n / 2), sum(1 / n*n) FROM (SELECT number n FROM numbers(10));
SELECT min(n*n + 1), min(1 + n*n), min(n*n - 1), min(1 - n*n) FROM (SELECT number n FROM numbers(10));
SELECT min(n*n * 2), min(2 * n*n), min(n*n / 2), min(1 / n*n) FROM (SELECT number n FROM numbers(10));
SELECT max(n*n + 1), max(1 + n*n), max(n*n - 1), max(1 - n*n) FROM (SELECT number n FROM numbers(10));
SELECT max(n*n * 2), max(2 * n*n), max(n*n / 2), max(1 / n*n) FROM (SELECT number n FROM numbers(10));

SELECT sum(1 + n + 1), sum(1 + 1 + n), sum(1 + n - 1), sum(1 + 1 - n) FROM (SELECT number n FROM numbers(10));
SELECT sum(1 + n * 2), sum(1 + 2 * n), sum(1 + n / 2), sum(1 + 1 / n) FROM (SELECT number n FROM numbers(10));
SELECT min(1 + n + 1), min(1 + 1 + n), min(1 + n - 1), min(1 + 1 - n) FROM (SELECT number n FROM numbers(10));
SELECT min(1 + n * 2), min(1 + 2 * n), min(1 + n / 2), min(1 + 1 / n) FROM (SELECT number n FROM numbers(10));
SELECT max(1 + n + 1), max(1 + 1 + n), max(1 + n - 1), max(1 + 1 - n) FROM (SELECT number n FROM numbers(10));
SELECT max(1 + n * 2), max(1 + 2 * n), max(1 + n / 2), max(1 + 1 / n) FROM (SELECT number n FROM numbers(10));

SELECT sum(n + -1 + -1), sum(-1 + n + -1), sum(n - -1 + -1), sum(-1 - n + -1) FROM (SELECT number n FROM numbers(10));
SELECT sum(n * -2 * -1), sum(-2 * n * -1), sum(n / -2 / -1), sum(-1 / n / -1) FROM (SELECT number n FROM numbers(10));
SELECT min(n + -1 + -1), min(-1 + n + -1), min(n - -1 + -1), min(-1 - n + -1) FROM (SELECT number n FROM numbers(10));
SELECT min(n * -2 * -1), min(-2 * n * -1), min(n / -2 / -1), min(-1 / n / -1) FROM (SELECT number n FROM numbers(10));
SELECT max(n + -1 + -1), max(-1 + n + -1), max(n - -1 + -1), max(-1 - n + -1) FROM (SELECT number n FROM numbers(10));
SELECT max(n * -2 * -1), max(-2 * n * -1), max(n / -2 / -1), max(-1 / n / -1) FROM (SELECT number n FROM numbers(10));

SELECT sum(n + 1) + sum(1 + n) + sum(n - 1) + sum(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT sum(n * 2) + sum(2 * n) + sum(n / 2) + sum(1 / n) FROM (SELECT number n FROM numbers(10));
SELECT min(n + 1) + min(1 + n) + min(n - 1) + min(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT min(n * 2) + min(2 * n) + min(n / 2) + min(1 / n) FROM (SELECT number n FROM numbers(10));
SELECT max(n + 1) + max(1 + n) + max(n - 1) + max(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT max(n * 2) + max(2 * n) + max(n / 2) + max(1 / n) FROM (SELECT number n FROM numbers(10));


SELECT sum(number * -3) + min(2 * number * -3) - max(-1 * -2 * number * -3) FROM numbers(100);
SELECT max(log(2) * number) FROM numbers(100);
SELECT round(max(log(2) * 3 * sin(0.3) * number * 4)) FROM numbers(100);

SET optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT sum(n + 1), sum(1 + n), sum(n - 1), sum(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT sum(n * 2), sum(2 * n), sum(n / 2), sum(1 / n) FROM (SELECT number n FROM numbers(10));
SELECT min(n + 1), min(1 + n), min(n - 1), min(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT min(n * 2), min(2 * n), min(n / 2), min(1 / n) FROM (SELECT number n FROM numbers(10));
SELECT max(n + 1), max(1 + n), max(n - 1), max(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT max(n * 2), max(2 * n), max(n / 2), max(1 / n) FROM (SELECT number n FROM numbers(10));

SELECT sum(n + -1), sum(-1 + n), sum(n - -1), sum(-1 - n) FROM (SELECT number n FROM numbers(10));
SELECT sum(n * -2), sum(-2 * n), sum(n / -2), sum(-1 / n) FROM (SELECT number n FROM numbers(10));
SELECT min(n + -1), min(-1 + n), min(n - -1), min(-1 - n) FROM (SELECT number n FROM numbers(10));
SELECT min(n * -2), min(-2 * n), min(n / -2), min(-1 / n) FROM (SELECT number n FROM numbers(10));
SELECT max(n + -1), max(-1 + n), max(n - -1), max(-1 - n) FROM (SELECT number n FROM numbers(10));
SELECT max(n * -2), max(-2 * n), max(n / -2), max(-1 / n) FROM (SELECT number n FROM numbers(10));

SELECT sum(abs(2) + 1), sum(abs(2) + n), sum(n - abs(2)), sum(1 - abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT sum(abs(2) * 2), sum(abs(2) * n), sum(n / abs(2)), sum(1 / abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT min(abs(2) + 1), min(abs(2) + n), min(n - abs(2)), min(1 - abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT min(abs(2) * 2), min(abs(2) * n), min(n / abs(2)), min(1 / abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT max(abs(2) + 1), max(abs(2) + n), max(n - abs(2)), max(1 - abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT max(abs(2) * 2), max(abs(2) * n), max(n / abs(2)), max(1 / abs(2)) FROM (SELECT number n FROM numbers(10));

SELECT sum(abs(n) + 1), sum(abs(n) + n), sum(n - abs(n)), sum(1 - abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT sum(abs(n) * 2), sum(abs(n) * n), sum(n / abs(n)), sum(1 / abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT min(abs(n) + 1), min(abs(n) + n), min(n - abs(n)), min(1 - abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT min(abs(n) * 2), min(abs(n) * n), min(n / abs(n)), min(1 / abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT max(abs(n) + 1), max(abs(n) + n), max(n - abs(n)), max(1 - abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT max(abs(n) * 2), max(abs(n) * n), max(n / abs(n)), max(1 / abs(n)) FROM (SELECT number n FROM numbers(10));

SELECT sum(n*n + 1), sum(1 + n*n), sum(n*n - 1), sum(1 - n*n) FROM (SELECT number n FROM numbers(10));
SELECT sum(n*n * 2), sum(2 * n*n), sum(n*n / 2), sum(1 / n*n) FROM (SELECT number n FROM numbers(10));
SELECT min(n*n + 1), min(1 + n*n), min(n*n - 1), min(1 - n*n) FROM (SELECT number n FROM numbers(10));
SELECT min(n*n * 2), min(2 * n*n), min(n*n / 2), min(1 / n*n) FROM (SELECT number n FROM numbers(10));
SELECT max(n*n + 1), max(1 + n*n), max(n*n - 1), max(1 - n*n) FROM (SELECT number n FROM numbers(10));
SELECT max(n*n * 2), max(2 * n*n), max(n*n / 2), max(1 / n*n) FROM (SELECT number n FROM numbers(10));

SELECT sum(1 + n + 1), sum(1 + 1 + n), sum(1 + n - 1), sum(1 + 1 - n) FROM (SELECT number n FROM numbers(10));
SELECT sum(1 + n * 2), sum(1 + 2 * n), sum(1 + n / 2), sum(1 + 1 / n) FROM (SELECT number n FROM numbers(10));
SELECT min(1 + n + 1), min(1 + 1 + n), min(1 + n - 1), min(1 + 1 - n) FROM (SELECT number n FROM numbers(10));
SELECT min(1 + n * 2), min(1 + 2 * n), min(1 + n / 2), min(1 + 1 / n) FROM (SELECT number n FROM numbers(10));
SELECT max(1 + n + 1), max(1 + 1 + n), max(1 + n - 1), max(1 + 1 - n) FROM (SELECT number n FROM numbers(10));
SELECT max(1 + n * 2), max(1 + 2 * n), max(1 + n / 2), max(1 + 1 / n) FROM (SELECT number n FROM numbers(10));

SELECT sum(n + -1 + -1), sum(-1 + n + -1), sum(n - -1 + -1), sum(-1 - n + -1) FROM (SELECT number n FROM numbers(10));
SELECT sum(n * -2 * -1), sum(-2 * n * -1), sum(n / -2 / -1), sum(-1 / n / -1) FROM (SELECT number n FROM numbers(10));
SELECT min(n + -1 + -1), min(-1 + n + -1), min(n - -1 + -1), min(-1 - n + -1) FROM (SELECT number n FROM numbers(10));
SELECT min(n * -2 * -1), min(-2 * n * -1), min(n / -2 / -1), min(-1 / n / -1) FROM (SELECT number n FROM numbers(10));
SELECT max(n + -1 + -1), max(-1 + n + -1), max(n - -1 + -1), max(-1 - n + -1) FROM (SELECT number n FROM numbers(10));
SELECT max(n * -2 * -1), max(-2 * n * -1), max(n / -2 / -1), max(-1 / n / -1) FROM (SELECT number n FROM numbers(10));

SELECT sum(n + 1) + sum(1 + n) + sum(n - 1) + sum(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT sum(n * 2) + sum(2 * n) + sum(n / 2) + sum(1 / n) FROM (SELECT number n FROM numbers(10));
SELECT min(n + 1) + min(1 + n) + min(n - 1) + min(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT min(n * 2) + min(2 * n) + min(n / 2) + min(1 / n) FROM (SELECT number n FROM numbers(10));
SELECT max(n + 1) + max(1 + n) + max(n - 1) + max(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT max(n * 2) + max(2 * n) + max(n / 2) + max(1 / n) FROM (SELECT number n FROM numbers(10));


SELECT sum(number * -3) + min(2 * number * -3) - max(-1 * -2 * number * -3) FROM numbers(100);
SELECT max(log(2) * number) FROM numbers(100);
SELECT round(max(log(2) * 3 * sin(0.3) * number * 4)) FROM numbers(100);

-- Coverage for ArithmeticOperationsInAgrFuncOptimize.cpp — literal-first and negative-reversal paths.
-- The analyzer handles this pass itself; with enable_analyzer=1 this code is bypassed entirely.
-- Exercises: lines 31-44 (exchangeExtractFirstArgument, literal-first operand),
--   line 67 (zeroField Int64), lines 111-116 (get_reverse_aggregate_function_name),
--   lines 118-129 (first_literal && !second_literal branch),
--   line 122 (literal-first divide skipped), lines 134-135 (need_reverse second_literal).
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

-- 4. max with negative first-literal multiply: max(-2 * a) → need_reverse → multiply(-2, min(a))
EXPLAIN SYNTAX SELECT max(-2 * a) FROM t_arith_legacy;
SELECT max(-2 * a) FROM t_arith_legacy;

-- 5. min with negative first-literal multiply: min(-3 * a) → need_reverse → multiply(-3, max(a))
EXPLAIN SYNTAX SELECT min(-3 * a) FROM t_arith_legacy;
SELECT min(-3 * a) FROM t_arith_legacy;

-- 6. max with negative second-literal multiply: max(a * -2) → need_reverse
EXPLAIN SYNTAX SELECT max(a * -2) FROM t_arith_legacy;
SELECT max(a * -2) FROM t_arith_legacy;

-- 7. sum with negative first-literal: sum(-2 * a) → get_reverse("sum")="sum" (line 115)
EXPLAIN SYNTAX SELECT sum(-2 * a) FROM t_arith_legacy;
SELECT sum(-2 * a) FROM t_arith_legacy;

-- 8. Literal-first divide is skipped (line 122: return {}): EXPLAIN shows unchanged form
EXPLAIN SYNTAX SELECT min(2 / (a + 10)) FROM t_arith_legacy;
SELECT min(2 / (a + 10)) FROM t_arith_legacy;

DROP TABLE t_arith_legacy;
