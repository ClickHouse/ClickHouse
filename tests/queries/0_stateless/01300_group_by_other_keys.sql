set optimize_group_by_function_keys = 1;

SELECT round(max(log(2) * number), 6) AS k FROM numbers(10000000) GROUP BY number % 2, number % 3, (number % 2 + number % 3) % 2 ORDER BY k;
SELECT round(avg(log(2) * number), 6) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) ORDER BY k;
SELECT round(avg(log(2) * number), 6) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3 ORDER BY k;
SELECT round(avg(log(2) * number), 6) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 ORDER BY k;
SELECT round(avg(log(2) * number), 6) AS k FROM numbers(10000000) GROUP BY (number % 2) % 3, number % 2 ORDER BY k;


EXPLAIN SYNTAX SELECT max(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 2, number % 3, (number % 2 + number % 3) % 2 ORDER BY k;
EXPLAIN SYNTAX SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) ORDER BY k;
EXPLAIN SYNTAX SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3 ORDER BY k;
EXPLAIN SYNTAX SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 ORDER BY k;
EXPLAIN SYNTAX SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) % 3, number % 2 ORDER BY k;

set optimize_group_by_function_keys = 0;

SELECT round(max(log(2) * number), 6) AS k FROM numbers(10000000) GROUP BY number % 2, number % 3, (number % 2 + number % 3) % 2 ORDER BY k;
SELECT round(avg(log(2) * number), 6) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) ORDER BY k;
SELECT round(avg(log(2) * number), 6) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3 ORDER BY k;
SELECT round(avg(log(2) * number), 6) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 ORDER BY k;
SELECT round(avg(log(2) * number), 6) AS k FROM numbers(10000000) GROUP BY (number % 2) % 3, number % 2 ORDER BY k;

EXPLAIN SYNTAX SELECT max(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 2, number % 3, (number % 2 + number % 3) % 2 ORDER BY k;
EXPLAIN SYNTAX SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) ORDER BY k;
EXPLAIN SYNTAX SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3 ORDER BY k;
EXPLAIN SYNTAX SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 ORDER BY k;
EXPLAIN SYNTAX SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) % 3, number % 2 ORDER BY k;
-- TODO - test with similar variables of different tables (collision)
