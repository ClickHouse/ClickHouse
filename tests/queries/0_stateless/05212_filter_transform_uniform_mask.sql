SELECT count(), sum(number)
FROM numbers(10)
WHERE (number % 2) + 1;

SELECT count()
FROM numbers(10)
WHERE toUInt8((number % 2) + 1);

SELECT count()
FROM numbers(10)
WHERE number % 2 = 2;

SELECT count(), sum(number)
FROM numbers(10)
WHERE number % 2;

SELECT number
FROM
(
    SELECT number, toUInt8(number % 2) AS condition
    FROM numbers(6)
)
WHERE condition = 1
ORDER BY number;
