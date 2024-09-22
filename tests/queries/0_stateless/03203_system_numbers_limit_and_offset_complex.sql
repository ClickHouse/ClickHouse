--- The following query was buggy before, so let's use it as a test case
WITH
    (num > 1) AND (arraySum(arrayMap(y -> ((y > 1) AND (y < num) AND ((num % y) = 0)), range(toUInt64(sqrt(num)) + 1))) = 0) AS is_prime_slow
SELECT
    num,
    ds,
FROM
(
    WITH
        arraySum(arrayMap(y -> toUInt8(y), splitByString('', toString(num)))) AS digits_sum
    SELECT
        1 + (number * 2) AS num,
        digits_sum AS ds
    FROM numbers_mt(10000)
    WHERE ds IN (
        WITH
            (number > 1) AND (arraySum(arrayMap(y -> ((y > 1) AND (y < number) AND ((number % y) = 0)), range(toUInt64(sqrt(number)) + 1))) = 0) AS is_prime_slow
        SELECT number
        FROM numbers(180 + 1)
        WHERE is_prime_slow
    )
)
WHERE is_prime_slow
ORDER BY num ASC
LIMIT 998, 1
SETTINGS max_block_size = 64, max_threads=16;

SELECT number
FROM numbers_mt(120)
WHERE (number % 10) = 0
ORDER BY number ASC
SETTINGS max_block_size = 31, max_threads = 11;

SELECT number
FROM numbers_mt(4242, 9)
WHERE (number % 10) = 0
ORDER BY number ASC
SETTINGS max_block_size = 31, max_threads = 11;
