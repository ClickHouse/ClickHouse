-- Tags: no-fasttest, no-random-settings

SET max_threads = 8;
SET max_block_size = 65536;

SELECT arrayFold((acc, x, y, z) -> (((acc + x) + y) + z), a, b, c, seed)
FROM
(
    SELECT
        arrayMap(x -> toUInt64(x + (number % 31)), range((number % 128) + 32)) AS a,
        arrayMap(x -> toUInt64(x + (number % 17)), range((number % 128) + 32)) AS b,
        arrayMap(x -> toUInt64(x + (number % 7)), range((number % 128) + 32)) AS c,
        toUInt64(number % 97) AS seed
    FROM numbers_mt(60000)
)
SETTINGS max_memory_usage = 430000000
FORMAT Null;
