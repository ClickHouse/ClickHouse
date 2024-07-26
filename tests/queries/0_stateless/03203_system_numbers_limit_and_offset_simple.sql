SELECT 'case 1';
SELECT number FROM numbers_mt(10000)
WHERE (number % 10) = 0
ORDER BY number ASC
LIMIT 990, 3;

SELECT 'case 2';
SELECT number FROM numbers_mt(10000)
WHERE (number % 10) = 0
ORDER BY number ASC
LIMIT 999, 20 SETTINGS max_block_size = 31;
