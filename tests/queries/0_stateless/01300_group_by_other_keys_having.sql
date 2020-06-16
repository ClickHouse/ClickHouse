set optimize_group_by_function_keys = 1;
set enable_debug_queries = 1;

SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 HAVING avg(log(2) * number) > 3465735.3 ORDER BY k;
SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k;
SELECT (number % 5) * (number % 5) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k;


analyze SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 HAVING avg(log(2) * number) > 3465735.3 ORDER BY k;
analyze SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k;
analyze SELECT (number % 5) * (number % 5) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k;

set optimize_group_by_function_keys = 0;

SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 HAVING avg(log(2) * number) > 3465735.3 ORDER BY k;
SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k;
SELECT (number % 5) * (number % 5) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k;

analyze SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 HAVING avg(log(2) * number) > 3465735.3 ORDER BY k;
analyze SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k;
analyze SELECT (number % 5) * (number % 5) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) HAVING ((number % 5) * (number % 5)) < 5 ORDER BY k;
