-- { echoOn }

-- Logical error query
SELECT DISTINCT number * 1
FROM numbers(10, sipHash64(sipHash64(sipHash64(2), 1), 1, 2, *), sipHash64(sipHash64(29103473, sipHash64(1), '3', sipHash64(1), 1)))
GROUP BY
    1,
    isNullable(1)
    WITH TOTALS
ORDER BY 1 ASC SETTINGS enable_analyzer = 1;

-- Simplified version of the above query
SELECT number FROM numbers(10, 14630045721179951620, 6670599363308407409);

SELECT number FROM numbers(10, 14630045721179951620, 6670599363308407409) LIMIT 10;

SELECT count(), min(number), max(number), sum(number) FROM numbers(0, 1000, 1);

SELECT count(), min(number), max(number), sum(number) FROM numbers(5, 1000, 7);

SELECT number FROM numbers(18446744073709551614, 5, 1);

SELECT number FROM numbers(18446744073709551610, 10, 1);

SELECT number FROM numbers(18446744073709551610, 10, 3);

SELECT number FROM numbers(18446744073709551615, 18446744073709551615, 18446744073709551615);

SELECT number FROM numbers(18446744073709551615, 18446744073709551615, 1844674407370955161);

SELECT number FROM numbers(5, 18446744073709551615, 1) LIMIT 10;

SELECT number FROM numbers(0, 1000, 2) WHERE number BETWEEN 10 AND 40;

SELECT number FROM numbers(18446744073709551610, 10, 3) WHERE number >= 18446744073709551612;

SELECT number FROM numbers(18446744073709551615, 10, 3) WHERE number >= 5;

SELECT number FROM numbers(18446744073709551610, 10, 3) WHERE number <= 5;

SELECT number FROM numbers(18446744073709551610, 10, 3) WHERE number >= 18446744073709551612 OR number <= 5;

SELECT count(), min(number), max(number), sum(number) FROM numbers(0, 1000, 1) WHERE number % 3 = 0;

SELECT number FROM numbers(100, 10, 1) WHERE number < 50;

SELECT number FROM system.numbers WHERE number < 10;

SELECT * FROM numbers(10) LIMIT 0;

SELECT * FROM system.numbers LIMIT 0;

SET max_threads = 10;

SELECT number FROM numbers_mt(10, 14630045721179951620, 6670599363308407409);

SELECT number FROM numbers_mt(10, 14630045721179951620, 6670599363308407409) LIMIT 10;

SELECT count(), min(number), max(number), sum(number) FROM numbers_mt(0, 1000, 1);

SELECT count(), min(number), max(number), sum(number) FROM numbers_mt(5, 1000, 7);

SELECT number FROM numbers_mt(18446744073709551614, 5, 1);

SELECT number FROM numbers_mt(18446744073709551610, 10, 1);

SELECT number FROM numbers_mt(18446744073709551610, 10, 3);

SELECT number FROM numbers_mt(18446744073709551615, 18446744073709551615, 18446744073709551615);

SELECT number FROM numbers_mt(18446744073709551615, 18446744073709551615, 1844674407370955161);

SELECT number FROM numbers_mt(5, 18446744073709551615, 1) LIMIT 10;

SELECT number FROM numbers_mt(0, 1000, 2) WHERE number BETWEEN 10 AND 40;

SELECT number FROM numbers_mt(18446744073709551610, 10, 3) WHERE number >= 18446744073709551612;

SELECT number FROM numbers_mt(18446744073709551615, 10, 3) WHERE number >= 5;

SELECT number FROM numbers_mt(18446744073709551610, 10, 3) WHERE number <= 5;

SELECT number FROM numbers_mt(18446744073709551610, 10, 3) WHERE number >= 18446744073709551612 OR number <= 5;

SELECT count(), min(number), max(number), sum(number) FROM numbers_mt(0, 1000, 1) WHERE number % 3 = 0;

SELECT number FROM numbers_mt(100, 10, 1) WHERE number < 50;

SELECT number FROM system.numbers_mt WHERE number < 10;

SELECT * FROM system.numbers_mt LIMIT 0;

SELECT number FROM system.numbers_mt WHERE number < 100 LIMIT 5;

SELECT number FROM system.numbers_mt WHERE number < 1000 LIMIT 5;

SELECT count(), min(number), max(number), sum(number) FROM system.numbers_mt WHERE number < 1000000;

SELECT count(), min(number), max(number), sum(number) FROM system.numbers_mt
WHERE number >= 5 AND number < 7000;

SELECT number FROM system.numbers_mt
WHERE (number BETWEEN 10 AND 15) OR (number BETWEEN 100 AND 105);

SELECT number FROM system.numbers_mt
WHERE (number BETWEEN 10 AND 20) OR (number BETWEEN 100 AND 110) LIMIT 7;

SELECT count(), min(number), max(number), sum(number) FROM system.numbers_mt
WHERE number >= 100000 AND number < 100000 + 1000000;

SELECT count(), min(number), max(number), sum(number) FROM system.numbers_mt
WHERE number >= 500 AND number < 2000;

SELECT count(), min(number), max(number), sum(number) FROM system.numbers_mt
WHERE (number BETWEEN 0 AND 50)
   OR (number BETWEEN 1000 AND 1100)
   OR (number BETWEEN 100000 AND 100100);

SELECT count(), min(number), max(number), sum(number) FROM system.numbers_mt
WHERE (number BETWEEN 2 AND 50)
   OR (number BETWEEN 30 AND 60)
   OR (number BETWEEN 50 AND 70);

SELECT count(), min(number), max(number), sum(number)
FROM system.numbers_mt
WHERE number < 1000;

SELECT number FROM system.numbers_mt WHERE number BETWEEN 123456 AND 1000000 LIMIT 10;

SELECT * FROM system.numbers_mt LIMIT 0;
