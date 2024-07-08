SELECT number FROM numbers_mt(10000)
WHERE (number % 10) = 0
ORDER BY number ASC
LIMIT 990, 3;

