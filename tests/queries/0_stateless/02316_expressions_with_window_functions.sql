SELECT sum(number) + 1
FROM numbers(10000)
GROUP BY number % 10;
