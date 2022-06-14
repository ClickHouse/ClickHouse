-- { echoOn }
SELECT number, sum(number) + 1 OVER (PARTITION BY number % 10)
FROM numbers(100)
ORDER BY number;

SELECT sum(number) + 1 AS x
FROM numbers(100)
GROUP BY number % 10
ORDER BY x;

SELECT
    number,
    sum(number) / count() OVER (PARTITION BY number % 10),
    avg(number) OVER (PARTITION BY number % 10)
FROM numbers(100)
ORDER BY number ASC;
