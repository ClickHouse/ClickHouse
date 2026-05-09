-- { echo }

SELECT count() FROM system.numbers WHERE number % 2 = 0 AND number < 100;

SELECT number FROM system.numbers WHERE number % 2 = 0 AND number < 100 LIMIT 10;

SELECT count() FROM system.numbers WHERE number % 2 = 0 AND number > 10 AND number < 20;

SELECT arraySort(groupArray(number))
FROM system.numbers
WHERE number < 10 AND (number < 5 OR number > 20) AND number % 2 = 0;

SELECT count() FROM system.numbers WHERE number > 10 AND number < 5 AND number % 2 = 0;

SELECT number FROM system.numbers WHERE number % 2 = 0 LIMIT 5;

SELECT arraySort(groupArray(number)) FROM system.numbers WHERE number % 2 = 0 AND number IN (8, 0, 6, 4, 2);
SELECT arraySort(groupArray(number)) FROM system.numbers WHERE number % 2 = 0 AND number NOT IN (2, 4) AND number < 10;

SELECT arraySort(groupArray(number)) FROM system.numbers WHERE number % 2 = 0 AND number = 6;
SELECT arraySort(groupArray(number)) FROM system.numbers WHERE number % 2 = 0 AND number != 2 AND number < 10;
SELECT arraySort(groupArray(number)) FROM system.numbers WHERE number % 2 = 0 AND number <= 6;
SELECT arraySort(groupArray(number)) FROM system.numbers WHERE number % 2 = 0 AND number >= 5 AND number < 10;

SELECT arraySort(groupArray(number)) FROM system.numbers WHERE number % 2 = 0 AND NOT (number BETWEEN 2 AND 6) AND number < 10;

SELECT count() FROM system.numbers WHERE isNull(number) AND number % 2 = 0;
SELECT count() FROM system.numbers WHERE isNotNull(number) AND number % 2 = 0 AND number < 10;

SELECT arraySort(groupArray(number))
FROM system.numbers
WHERE number % 2 = 0 AND ((number BETWEEN 0 AND 3) OR (number BETWEEN 10 AND 13));

SELECT count() FROM system.numbers_mt WHERE number % 2 = 0 AND number < 100;

SELECT count() FROM numbers() WHERE number % 2 = 0 AND number < 100;
SELECT number FROM numbers() WHERE number % 2 = 0 AND number < 100 LIMIT 10;
SELECT arraySort(groupArray(number)) FROM numbers() WHERE number % 2 = 0 AND number IN (8, 0, 6, 4, 2);

SELECT number FROM numbers(100) WHERE number % 2 = 0 AND number < 100 LIMIT 10;

SELECT arraySort(groupArray(number))
FROM numbers(18446744073709551614, 5)
WHERE number < 3 AND number % 2 = 0;

SELECT arraySort(groupArray(number))
FROM numbers(1, 20, 3)
WHERE number < 10 AND number % 2 = 1;

SELECT count() FROM system.primes WHERE prime % 2 = 1 AND prime < 100;

SELECT prime FROM system.primes WHERE prime % 2 = 1 AND prime < 100 LIMIT 10;

SELECT count() FROM system.primes WHERE prime % 2 = 1 AND prime > 10 AND prime < 30;

SELECT arraySort(groupArray(prime))
FROM system.primes
WHERE prime < 30 AND (prime < 5 OR prime > 100) AND prime % 2 = 1;

SELECT prime
FROM system.primes
WHERE prime / 3 > 1 AND prime % 2 = 1 AND prime < 30;

SELECT arraySort(groupArray(prime)) FROM system.primes WHERE prime % 2 = 1 AND prime IN (11, 3, 5, 7);
SELECT arraySort(groupArray(prime)) FROM system.primes WHERE prime % 2 = 1 AND prime NOT IN (3, 5, 7, 11) AND prime < 20;

SELECT arraySort(groupArray(prime)) FROM system.primes WHERE prime % 2 = 1 AND prime = 29;
SELECT arraySort(groupArray(prime)) FROM system.primes WHERE prime % 2 = 1 AND prime != 3 AND prime < 20;
SELECT arraySort(groupArray(prime)) FROM system.primes WHERE prime % 2 = 1 AND prime <= 13;
SELECT arraySort(groupArray(prime)) FROM system.primes WHERE prime % 2 = 1 AND prime >= 20 AND prime < 30;

SELECT arraySort(groupArray(prime)) FROM system.primes WHERE prime % 2 = 1 AND NOT (prime BETWEEN 5 AND 13) AND prime < 20;

SELECT count() FROM system.primes WHERE isNull(prime) AND prime % 2 = 1;
SELECT count() FROM system.primes WHERE isNotNull(prime) AND prime % 2 = 1 AND prime < 20;

SELECT arraySort(groupArray(prime))
FROM system.primes
WHERE prime % 2 = 1 AND ((prime BETWEEN 2 AND 10) OR (prime BETWEEN 20 AND 30));

SELECT arraySort(groupArray(prime)) FROM system.primes WHERE prime % 2 = 0 AND prime = 2;

SELECT count() FROM primes() WHERE prime % 2 = 1 AND prime < 100;
SELECT arraySort(groupArray(prime)) FROM primes() WHERE prime % 2 = 1 AND prime IN (11, 3, 5, 7);

SELECT count() FROM primes(10) WHERE prime > toUInt64(1e15);

SELECT count(), min(prime), max(prime) FROM primes(10) WHERE prime % 2 = 1 AND prime < 100;

SELECT * FROM primes(10) WHERE prime > 80 and prime < 2000;

SELECT * FROM numbers(10) WHERE number > 80 and number < 2000;

SELECT * FROM primes(10, 3) WHERE prime > 80 and prime < 2000;

SELECT * FROM numbers(10, 3) WHERE number > 80 and number < 2000;

SELECT * FROM primes(10, 3, 3) WHERE prime > 80 and prime < 2000;

SELECT * FROM numbers(10, 3, 3) WHERE number > 80 and number < 2000;

SELECT * FROM system.numbers LIMIT 0;
SELECT * FROM system.numbers_mt LIMIT 0;

SELECT arraySort(groupArray(number)) FROM system.numbers WHERE number < 10;
SELECT arraySort(groupArray(number)) FROM system.numbers_mt WHERE number < 10;

SELECT * FROM system.primes LIMIT 0;
SELECT * FROM primes() LIMIT 0;

SELECT arraySort(groupArray(prime)) FROM system.primes WHERE prime < 20;
SELECT arraySort(groupArray(prime)) FROM primes() WHERE prime < 20;

SELECT arraySort(groupArray(number))
FROM system.numbers
WHERE number % 2 = 0 AND number < 10;

SELECT arraySort(groupArray(number))
FROM system.numbers
WHERE number % 2 = 0 AND number BETWEEN 3 AND 12;

SELECT arraySort(groupArray(number))
FROM system.numbers
WHERE number % 2 = 0 AND number >= 10 AND number < 20;

SELECT arraySort(groupArray(number))
FROM system.numbers
WHERE number < 20 AND (number % 2 = 0 OR number % 3 = 0);

SELECT arraySort(groupArray(number))
FROM system.numbers
WHERE number % 2 = 0 AND ((number BETWEEN 0 AND 10) OR (number BETWEEN 100 AND 105));

SELECT arraySort(groupArray(number))
FROM system.numbers
WHERE number % 2 = 0
  AND ((number BETWEEN 0 AND 100) OR (number BETWEEN 1000 AND 1100))
  AND number < 30;

SELECT arraySort(groupArray(number))
FROM system.numbers
WHERE toString(number) LIKE '%7%' AND number < 30;

SELECT arraySort(groupArray(number))
FROM system.numbers
WHERE intDiv(number, 10) = 3 AND number < 50;

SELECT count(), min(number), max(number), sum(number)
FROM system.numbers
WHERE number % 3 = 0 AND number < 100000;

SELECT count(), min(number), max(number), sum(number)
FROM system.numbers
WHERE number % 7 = 0 AND number < 1000000;

SELECT count(), min(number), max(number), sum(number)
FROM system.numbers
WHERE number % 5 = 0
  AND ((number < 1000) OR (number BETWEEN 10000 AND 11000));

SELECT count()
FROM system.numbers
WHERE number % 2 = 0 AND number < 10 AND number >= 10;

SELECT arraySort(groupArray(number))
FROM system.numbers
WHERE number % 2 = 0 AND number < 0;

SELECT count()
FROM system.numbers
WHERE number < 5 AND number > 20 AND number % 2 = 0;

SELECT count()
FROM
(
    SELECT number
    FROM system.numbers
    WHERE number < 100 AND number % 2 = 0
    LIMIT 10
);

SELECT count()
FROM
(
    SELECT number
    FROM system.numbers
    WHERE number < 100000 AND number % 97 = 0
    LIMIT 10
);

SELECT count()
FROM
(
    SELECT number
    FROM system.numbers
    WHERE number < 100 AND number % 2 = 0
    LIMIT 10 OFFSET 5
);

SELECT count()
FROM
(
    SELECT number
    FROM system.numbers
    WHERE number < 100 AND number % 2 = 0
    LIMIT 0
);

SET max_threads = 10;

SELECT arraySort(groupArray(number))
FROM system.numbers_mt
WHERE number % 2 = 0 AND number < 10;

SELECT arraySort(groupArray(number))
FROM system.numbers_mt
WHERE number % 3 = 1 AND number >= 5 AND number < 30;

SELECT count(), min(number), max(number), sum(number)
FROM system.numbers_mt
WHERE number % 3 = 0 AND number < 1000000;

SELECT count(), min(number), max(number), sum(number)
FROM system.numbers_mt
WHERE number % 7 = 0 AND number < 1000000;

SELECT count(), min(number), max(number), sum(number)
FROM system.numbers_mt
WHERE number % 11 = 0
  AND ((number BETWEEN 0 AND 10000) OR (number BETWEEN 500000 AND 510000));

SELECT count()
FROM system.numbers_mt
WHERE number % 2 = 0 AND number < 10 AND number >= 10;

SELECT count()
FROM
(
    SELECT number
    FROM system.numbers_mt
    WHERE number < 100 AND number % 2 = 0
    LIMIT 10
);

SELECT count()
FROM
(
    SELECT number
    FROM system.numbers_mt
    WHERE number < 100000 AND number % 97 = 0
    LIMIT 10
);

SET max_block_size = 1000000;

SELECT count(), min(number), max(number), sum(number)
FROM system.numbers_mt
WHERE number % 2 = 0 AND number < 100;

SET max_block_size = 8192;

SELECT count(), min(number), max(number), sum(number)
FROM numbers(0, 100000, 1)
WHERE number % 3 = 0 AND number < 1000;

SELECT count(), min(number), max(number), sum(number)
FROM numbers_mt(0, 100000, 1)
WHERE number % 3 = 0 AND number < 1000;

SELECT arraySort(groupArray(number))
FROM numbers(0, 200, 1)
WHERE number % 2 = 0 AND ((number BETWEEN 0 AND 10) OR (number BETWEEN 100 AND 110));

SELECT arraySort(groupArray(number))
FROM numbers_mt(0, 200, 1)
WHERE number % 2 = 0 AND ((number BETWEEN 0 AND 10) OR (number BETWEEN 100 AND 110));

SELECT arraySort(groupArray(number))
FROM numbers(18446744073709551610, 10, 3)
WHERE number % 2 = 0 AND number < 10;

SELECT arraySort(groupArray(number))
FROM numbers_mt(18446744073709551610, 10, 3)
WHERE number % 2 = 0 AND number < 10;

SELECT arraySort(groupArray(number))
FROM numbers(18446744073709551610, 10, 3)
WHERE number % 2 = 1 AND number >= 18446744073709551612;

SELECT arraySort(groupArray(number))
FROM numbers_mt(18446744073709551610, 10, 3)
WHERE number % 2 = 1 AND number >= 18446744073709551612;

SELECT arraySort(groupArray(number))
FROM numbers(100, 10, 1)
WHERE number < 50;

SELECT arraySort(groupArray(number))
FROM numbers_mt(100, 10, 1)
WHERE number < 50;

SELECT arraySort(groupArray(prime))
FROM system.primes
WHERE prime % 2 = 1 AND prime < 20;

SELECT arraySort(groupArray(prime))
FROM primes()
WHERE prime % 2 = 1 AND prime < 20;

SELECT arraySort(groupArray(prime))
FROM system.primes
WHERE prime % 3 = 2 AND prime < 50;

SELECT arraySort(groupArray(prime))
FROM primes()
WHERE prime % 3 = 2 AND prime < 50;

SELECT arraySort(groupArray(prime))
FROM system.primes
WHERE bitAnd(prime, 7) = 3 AND prime < 200;

SELECT arraySort(groupArray(prime))
FROM primes()
WHERE bitAnd(prime, 7) = 3 AND prime < 200;

SELECT count(), min(prime), max(prime), sum(prime)
FROM system.primes
WHERE prime % 2 = 1 AND prime < 10000;

SELECT count(), min(prime), max(prime), sum(prime)
FROM primes()
WHERE prime % 2 = 1 AND prime < 10000;

SELECT count(), min(prime), max(prime), sum(prime)
FROM system.primes
WHERE prime % 2 = 1
  AND ((prime BETWEEN 2 AND 50) OR (prime BETWEEN 1000 AND 1100));

SELECT count(), min(prime), max(prime), sum(prime)
FROM system.primes
WHERE prime % 2 = 1
  AND ((prime BETWEEN 2 AND 50) OR (prime BETWEEN 40 AND 60));

SELECT count()
FROM system.primes
WHERE prime % 2 = 1 AND prime < 10 AND prime >= 10;

SELECT count()
FROM primes()
WHERE prime % 2 = 1 AND prime < 10 AND prime >= 10;

SELECT count()
FROM system.primes
WHERE prime % 2 = 1 AND prime < 2;

SELECT count()
FROM primes()
WHERE prime % 2 = 1 AND prime < 2;

SELECT count()
FROM
(
    SELECT prime
    FROM system.primes
    WHERE prime % 2 = 1 AND prime < 100
    LIMIT 10
);

SELECT count()
FROM
(
    SELECT prime
    FROM system.primes
    WHERE prime % 2 = 1 AND prime < 100
    LIMIT 10 OFFSET 5
);

SELECT count()
FROM
(
    SELECT prime
    FROM system.primes
    WHERE prime % 2 = 1
    LIMIT 10
);

SELECT count()
FROM
(
    SELECT prime
    FROM primes()
    WHERE prime % 2 = 1
    LIMIT 10
);

SELECT count(), min(prime), max(prime), sum(prime)
FROM primes(100)
WHERE prime < 100;

SELECT arraySort(groupArray(prime))
FROM primes(30)
WHERE prime < 30;
