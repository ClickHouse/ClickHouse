SELECT max(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 2 ORDER BY k;
SELECT max(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 2, number % 3 ORDER BY k;
SELECT max(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 2, number % 3, (number % 2 + number % 3) % 2 ORDER BY k;
SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5)) ORDER BY k;
SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3 ORDER BY k;
SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2 ORDER BY k;
SELECT avg(log(2) * number) AS k FROM numbers(10000000) GROUP BY (number % 2) % 3, number % 2 ORDER BY k;
