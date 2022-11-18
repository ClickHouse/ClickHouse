-- { echoOn }
EXPLAIN actions=1
    (
        SELECT round(avg(log(2) * number), 6) AS k
        FROM numbers(10000000)
        GROUP BY number % 3, number % 2
    )
SETTINGS allow_experimental_analyzer=1;

EXPLAIN actions=1
    (
        SELECT round(log(2) * avg(number), 6) AS k
        FROM numbers(10000000)
        GROUP BY number % 3, number % 2
    )
SETTINGS allow_experimental_analyzer=1;

SELECT round(avg(log(2) * number), 6) AS k
FROM numbers(10000000)
GROUP BY number % 3, number % 2
SETTINGS allow_experimental_analyzer=1;

SELECT round(log(2) * avg(number), 6) AS k
FROM numbers(10000000)
GROUP BY number % 3, number % 2
SETTINGS allow_experimental_analyzer=0;
