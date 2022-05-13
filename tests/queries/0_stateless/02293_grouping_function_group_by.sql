SELECT
    number,
    grouping(number, number % 2, number % 3) = 6
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    number,
    number % 2
ORDER BY number;

SELECT
    number,
    grouping(number),
    GROUPING(number % 2)
FROM remote('127.0.0.{2,3}', numbers(10))
GROUP BY
    number,
    number % 2
ORDER BY number;
