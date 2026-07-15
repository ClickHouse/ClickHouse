-- Tags: long

-- { echo }

SELECT count() FROM primes(1000000) WHERE NOT isPrime(prime);

SELECT
    (SELECT count() FROM numbers(1000000) WHERE isPrime(number))
  = (SELECT count() FROM primes() WHERE prime < 1000000);

SELECT count() FROM numbers(1000000)
WHERE isPrime(number) != (number IN (SELECT prime FROM primes() WHERE prime < 1000000));

SELECT count() = 0 FROM primes() WHERE prime < 1000000 AND NOT isPrime(prime);
SELECT count() = 0 FROM primes() WHERE prime BETWEEN toUInt64(1e12) AND toUInt64(1e12 + 1e5)
                                   AND NOT isPrime(prime);
SELECT count() = 0 FROM primes() WHERE prime BETWEEN toUInt64(1e15) AND toUInt64(1e15 + 1e5)
                                   AND NOT isPrime(prime);

SELECT
    (SELECT count() FROM numbers(1000000) WHERE isPrime(toUInt32(number)))
  = (SELECT count() FROM primes() WHERE prime < 1000000);

SELECT
    (SELECT count() FROM numbers(toUInt64(1e10), 10001) WHERE isPrime(number))
  = (SELECT count() FROM primes() WHERE prime BETWEEN toUInt64(1e10) AND toUInt64(1e10 + 10000));

SELECT
    (SELECT count() FROM numbers(toUInt64(1e12), 10001) WHERE isPrime(number))
  = (SELECT count() FROM primes() WHERE prime BETWEEN toUInt64(1e12) AND toUInt64(1e12 + 10000));

SELECT
    (SELECT count() FROM numbers(toUInt64(1.5e15), 10001) WHERE isPrime(number))
  = (SELECT count() FROM primes() WHERE prime BETWEEN toUInt64(1.5e15) AND toUInt64(1.5e15 + 10000));

SELECT count() FROM numbers(1000000) AS n
LEFT ANTI JOIN (SELECT prime FROM primes() WHERE prime < 1000000) AS p ON p.prime = n.number
WHERE isPrime(toUInt32(n.number)) = 1;

SELECT count() = 0 FROM (
    SELECT toUInt64(rand64()) AS n FROM numbers(1000000)
)
WHERE isPrime(n) != isProbablePrime(n);

WITH 1000000 AS bound
SELECT count() = 0 FROM (
    SELECT toUInt64(rand64()) AS n FROM numbers(bound)
)
WHERE isProbablePrime(n, 1) != isProbablePrime(n, 25);

SELECT count() = 0 FROM (
    SELECT toUInt256(toString(rand64()) || toString(rand64()) || toString(rand64()) || toString(rand64())) AS n
    FROM numbers(10000)
)
WHERE isProbablePrime(n) != isProbablePrime(n);

SELECT
(
    SELECT cityHash64(groupArray(isProbablePrime(toUInt256(number * 1000000007 + 1) * toUInt256(7919), 25)))
    FROM numbers(50000)
    SETTINGS max_threads = 16
)
=
(
    SELECT cityHash64(groupArray(isProbablePrime(toUInt256(number * 1000000007 + 1) * toUInt256(7919), 25)))
    FROM numbers(50000)
    SETTINGS max_threads = 1
);
