-- { echo }

SELECT prime
FROM system.primes
WHERE prime BETWEEN 1e14 AND 1e14 + 100;

SELECT prime FROM system.primes
WHERE prime > 1e15 LIMIT 1;

SELECT prime FROM system.primes
   WHERE (prime BETWEEN 1e5 AND 1e5 + 100)
   OR (prime BETWEEN 1e6 AND 1e6 + 100)
   OR (prime BETWEEN 1e7 AND 1e7 + 100)
   OR (prime BETWEEN 1e8 AND 1e8 + 100)
   OR (prime BETWEEN 1e9 AND 1e9 + 100)
   OR (prime BETWEEN 1e11 AND 1e11 + 100)
   OR (prime BETWEEN 1e12 AND 1e12 + 100)
   OR (prime BETWEEN 1e13 AND 1e13 + 100)
   OR (prime BETWEEN 1e14 AND 1e14 + 100)
   OR (prime BETWEEN 1e14 + 1000 AND 1e14 + 1100)
   OR (prime BETWEEN 1e14 + 100000 AND 1e14 + 100100)
   OR (prime BETWEEN 1e14 + 100000000 AND 1e14 + 100000100)
   OR (prime BETWEEN 1e14 + 10000000000 AND 1e14 + 10000000100)
   OR (prime BETWEEN 1e14 + 1000000000000 AND 1e14 + 1000000000100)
   OR (prime BETWEEN 1e14 + 100000000000000 AND 1e14 + 100000000000100)
   OR (prime IN [2])
   OR prime == 3;

SELECT prime FROM primes() WHERE prime > 1e15 LIMIT 1;

SELECT prime FROM primes() WHERE prime > 1e16 LIMIT 1;

SELECT prime
FROM primes()
WHERE prime >= 1e15
  AND prime % 65537 = 1
LIMIT 1;

SELECT sum(prime)
FROM primes()
WHERE prime BETWEEN 1e6 AND 1e6 + 100
   OR prime BETWEEN 1e12 AND 1e12 + 100
   OR prime BETWEEN 1e15 AND 1e15 + 100
   OR prime IN (9999999967, 9999999971, 9999999973)
   OR prime = 1000000000000037;

SELECT number
FROM system.numbers
WHERE number BETWEEN 1e14 AND 1e14 + 20;

SELECT number FROM system.numbers
WHERE number > 1e15 LIMIT 1;

SELECT number FROM numbers() WHERE number > 1e15 LIMIT 1;

SELECT number
FROM numbers()
WHERE number >= 1e15
  AND number < 1e15 + 65537
  AND number % 65537 = 1
LIMIT 1;

SELECT sum(number)
FROM numbers()
WHERE number BETWEEN 1e6 AND 1e6 + 100
   OR number BETWEEN 1e12 AND 1e12 + 100
   OR number BETWEEN 1e15 AND 1e15 + 100
   OR number IN (9999999967, 9999999971, 9999999973)
   OR number = 1000000000000037;

SELECT number
FROM numbers()
WHERE number >= 1e15
  AND number < 1e15 + 100000
  AND bitAnd(sipHash64(number), 0xFFFFF) = 0
LIMIT 1;
