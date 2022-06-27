SELECT number, number % 2, sum(number) AS val
FROM numbers(10)
GROUP BY ROLLUP(number, number % 2)
ORDER BY (number, number % 2, val);
