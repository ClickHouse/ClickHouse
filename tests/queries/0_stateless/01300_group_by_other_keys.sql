set optimize_group_by_function_keys = 1;
set enable_debug_queries = 1;

SELECT max(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 2, number % 3, (number % 2 + number % 3) % 2 ORDER BY k;
SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) ORDER BY k;
SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3 ORDER BY k;
SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 ORDER BY k;
SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) % 3, number % 2 ORDER BY k;


analyze SELECT max(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 2, number % 3, (number % 2 + number % 3) % 2 ORDER BY k;
analyze SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) ORDER BY k;
analyze SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3 ORDER BY k;
analyze SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 ORDER BY k;
analyze SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) % 3, number % 2 ORDER BY k;

set optimize_group_by_function_keys = 0;

SELECT max(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 2, number % 3, (number % 2 + number % 3) % 2 ORDER BY k;
SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) ORDER BY k;
SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3 ORDER BY k;
SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 ORDER BY k;
SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) % 3, number % 2 ORDER BY k;

analyze SELECT max(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 2, number % 3, (number % 2 + number % 3) % 2 ORDER BY k;
analyze SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) ORDER BY k;
analyze SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3 ORDER BY k;
analyze SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 ORDER BY k;
analyze SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) % 3, number % 2 ORDER BY k;
-- TODO - test with similar variables of different tables (collision)
