SET max_block_size = 4213;

SELECT DISTINCT (blockSize() <= 4213)
FROM
(
    SELECT number
    FROM numbers(100000)
    GROUP BY number
);
