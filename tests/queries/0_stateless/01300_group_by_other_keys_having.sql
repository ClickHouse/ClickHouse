set optimize_group_by_function_keys = 1;
set optimize_syntax_fuse_functions = 0;
set optimize_arithmetic_operations_in_aggregate_functions = 1;
set enable_analyzer = 1;
SET enable_optimize_predicate_expression = 1; -- CI may inject False; HAVING condition on GROUP BY key not pushed to WHERE → stays as HAVING instead of WHERE in EXPLAIN SYNTAX (enable_analyzer=0 path)

-- { echoOn }
SELECT round(avg(log(2) * number), 6) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 HAVING avg(log(2) * number) > 3465735.3 ORDER BY k;
SELECT round(avg(log(2) * number), 6) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 HAVING avg(log(2) * number) > 3465735.3 ORDER BY k SETTINGS enable_analyzer=1;

SELECT round(avg(log(2) * number), 6) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k;
SELECT round(avg(log(2) * number), 6) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k SETTINGS enable_analyzer=1;

SELECT (number % 5) * (number % 5) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k;
SELECT (number % 5) * (number % 5) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k SETTINGS enable_analyzer=1;

-- { echoOff }

EXPLAIN SYNTAX SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 HAVING avg(log(2) * number) > 3465735.3 ORDER BY k;
EXPLAIN SYNTAX SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 HAVING avg(log(2) * number) > 3465735.3 ORDER BY k SETTINGS enable_analyzer=0;
EXPLAIN QUERY TREE run_passes=1 SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 HAVING avg(log(2) * number) > 3465735.3 ORDER BY k;
EXPLAIN SYNTAX SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k;
EXPLAIN SYNTAX SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k SETTINGS enable_analyzer=0;
EXPLAIN QUERY TREE run_passes=1 SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k;
EXPLAIN SYNTAX SELECT (number % 5) * (number % 5) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k;
EXPLAIN SYNTAX SELECT (number % 5) * (number % 5) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k SETTINGS enable_analyzer=0;
EXPLAIN QUERY TREE run_passes=1 SELECT (number % 5) * (number % 5) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k;

set optimize_group_by_function_keys = 0;

SELECT round(avg(log(2) * number), 6) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 HAVING avg(log(2) * number) > 3465735.3 ORDER BY k;
SELECT round(avg(log(2) * number), 6) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k;
SELECT (number % 5) * (number % 5) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k;

EXPLAIN SYNTAX SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 HAVING avg(log(2) * number) > 3465735.3 ORDER BY k;
EXPLAIN QUERY TREE run_passes=1 SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 HAVING avg(log(2) * number) > 3465735.3 ORDER BY k;
EXPLAIN SYNTAX SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k;
EXPLAIN SYNTAX SELECT (number % 5) * (number % 5) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k;
