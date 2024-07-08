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

--- The following query was buggy before, so let's use it as a test case
WITH
    toUInt64(-1) AS umax,
    toUInt8(ceil(log10(umax))) AS max_digits,
    9 * max_digits AS max_digits_sum,
    (x -> ((x > 1) AND (arraySum(arrayMap(y -> ((y > 1) AND (y < x) AND ((x % y) = 0)), range(toUInt64(sqrt(x)) + 1))) = 0))) AS is_prime_slow
SELECT
    num,
    ds
FROM
(
    WITH x -> arraySum(arrayMap(y -> toUInt8(y), splitByString('', toString(x)))) AS digits_sum
    SELECT
        1 + (number * 2) AS num,
        digits_sum(num) AS ds
    FROM numbers_mt(10000)
    WHERE ds IN (
        WITH x -> ((x > 1) AND (arraySum(arrayMap(y -> ((y > 1) AND (y < x) AND ((x % y) = 0)), range(toUInt64(sqrt(x)) + 1))) = 0)) AS is_prime_slow
        SELECT number
        FROM numbers(max_digits_sum + 1)
        WHERE is_prime_slow(number)
    )
)
WHERE is_prime_slow(num)
ORDER BY num ASC
LIMIT 998, 1
SETTINGS max_block_size = 64, max_threads=16;

SELECT number
FROM numbers_mt(120)
WHERE (number % 10) = 0
ORDER BY number ASC
SETTINGS max_block_size = 31, max_threads = 11
