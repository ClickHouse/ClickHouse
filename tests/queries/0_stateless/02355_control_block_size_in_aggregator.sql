SET max_block_size = 4213;

--- We allocate space for one more row in case nullKeyData is present.
SELECT DISTINCT (blockSize() <= 4214)
FROM
(
    SELECT number
    FROM numbers(100000)
    GROUP BY number
);
