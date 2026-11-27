-- Tags: long

SET max_rows_to_read = 150000000;

SELECT 'First 100 twin primes greater than 10^15';
WITH
    toUInt64(1e15) AS lo,
    lo + 2000000 AS hi
SELECT
    p.prime,
    q.prime
FROM
    (SELECT prime FROM system.primes WHERE prime BETWEEN lo AND hi) AS p
INNER JOIN
    (SELECT prime FROM system.primes WHERE prime BETWEEN lo AND hi + 2) AS q
ON q.prime = p.prime + 2
ORDER BY p.prime
LIMIT 100;

SELECT 'First 100 Sophie Germain primes greater than 10^15';
WITH
    toUInt64(1e15) AS lo,
    lo + 2000000 AS hi
SELECT
    p.prime,
    q.prime
FROM
    (SELECT prime FROM system.primes WHERE prime BETWEEN lo AND hi) AS p
INNER JOIN
    (SELECT prime FROM system.primes WHERE prime BETWEEN 2 * lo + 1 AND 2 * hi + 1) AS q
ON q.prime = 2 * p.prime + 1
ORDER BY p.prime
LIMIT 100;

SELECT 'First 7 Mersenne primes';
SELECT prime
FROM system.primes
WHERE bitAnd(prime, prime + 1) = 0
LIMIT 7;

SELECT '4 Mersenne primes after skipping first 3';
SELECT prime
FROM system.primes
WHERE bitAnd(prime, prime + 1) = 0
LIMIT 4 OFFSET 3;

SELECT 'Number of primes less than 10^9';
SELECT count()
FROM system.primes
WHERE prime < 1000000000;
