SET max_block_size = 8192;

SELECT DISTINCT blockSize() <= 8192
FROM
(
    SELECT n
    FROM
    (
        SELECT range(0, rand() % 10) AS x
        FROM numbers(1000000)
    )
    LEFT ARRAY JOIN x AS n
)
