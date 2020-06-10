SELECT max(log(2) * number) FROM numbers(10000000) GROUP BY number % 2;
SELECT max(log(2) * number) FROM numbers(10000000) GROUP BY number % 2, number % 3;
SELECT max(log(2) * number) FROM numbers(10000000) GROUP BY number % 2, number % 3, (number % 2 + number % 3) % 2;
SELECT avg(log(2) * number) FROM numbers(10000000) GROUP BY number % 5, ((number % 5) * (number % 5));
SELECT avg(log(2) * number) FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3;
SELECT avg(log(2) * number) FROM numbers(10000000) GROUP BY (number % 2) * (number % 3), number % 3, number % 2;
SELECT avg(log(2) * number) FROM numbers(10000000) GROUP BY (number % 2) % 3, number % 2;