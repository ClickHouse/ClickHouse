-- `min` / `max` on 128/256-bit types use a batch kernel; cross-check it against a sort-based reference.

SELECT 'value-vs-sort', t, ok
FROM
(
    SELECT 'Int128' AS t,
           (min(x) = arraySort(groupArray(x))[1]) AND (max(x) = arraySort(groupArray(x))[-1]) AS ok
    FROM (SELECT toInt128(sipHash64(number)) * toInt128(-1 + (number % 2) * 2) AS x FROM numbers(200000))
    UNION ALL
    SELECT 'UInt128',
           (min(x) = arraySort(groupArray(x))[1]) AND (max(x) = arraySort(groupArray(x))[-1])
    FROM (SELECT toUInt128(sipHash64(number)) * toUInt128(number % 5 + 1) AS x FROM numbers(200000))
    UNION ALL
    SELECT 'Int256',
           (min(x) = arraySort(groupArray(x))[1]) AND (max(x) = arraySort(groupArray(x))[-1])
    FROM (SELECT toInt256(sipHash64(number)) * toInt256(-1 + (number % 2) * 2) AS x FROM numbers(100000))
    UNION ALL
    SELECT 'UInt256',
           (min(x) = arraySort(groupArray(x))[1]) AND (max(x) = arraySort(groupArray(x))[-1])
    FROM (SELECT toUInt256(sipHash64(number)) AS x FROM numbers(100000))
    UNION ALL
    SELECT 'Decimal128',
           (min(x) = arraySort(groupArray(x))[1]) AND (max(x) = arraySort(groupArray(x))[-1])
    FROM (SELECT toDecimal128(sipHash64(number) % 1000000000, 4) - toDecimal128(number % 7, 4) AS x FROM numbers(200000))
    UNION ALL
    SELECT 'Decimal256',
           (min(x) = arraySort(groupArray(x))[1]) AND (max(x) = arraySort(groupArray(x))[-1])
    FROM (SELECT toDecimal256(sipHash64(number) % 1000000000, 8) AS x FROM numbers(100000))
)
ORDER BY t;

SELECT 'minIf-maxIf', t, ok
FROM
(
    SELECT 'Int128' AS t,
           (minIf(x, c) = arraySort(groupArrayIf(x, c))[1]) AND (maxIf(x, c) = arraySort(groupArrayIf(x, c))[-1]) AS ok
    FROM (SELECT toInt128(sipHash64(number)) AS x, number % 7 = 0 AS c FROM numbers(200000))
    UNION ALL
    SELECT 'Decimal128',
           (minIf(x, c) = arraySort(groupArrayIf(x, c))[1]) AND (maxIf(x, c) = arraySort(groupArrayIf(x, c))[-1])
    FROM (SELECT toDecimal128(sipHash64(number) % 1000000000, 3) AS x, number % 7 = 0 AS c FROM numbers(200000))
    UNION ALL
    SELECT 'UInt256',
           (minIf(x, c) = arraySort(groupArrayIf(x, c))[1]) AND (maxIf(x, c) = arraySort(groupArrayIf(x, c))[-1])
    FROM (SELECT toUInt256(sipHash64(number)) AS x, number % 3 = 0 AS c FROM numbers(100000))
)
ORDER BY t;

-- Nullable input, with and without an `if` condition.
SELECT 'nullable', t, ok
FROM
(
    SELECT 'Int128' AS t,
           (min(x) = arraySort(groupArray(x))[1]) AND (max(x) = arraySort(groupArray(x))[-1])
       AND (minIf(x, c) = arraySort(groupArrayIf(x, c))[1]) AND (maxIf(x, c) = arraySort(groupArrayIf(x, c))[-1]) AS ok
    FROM (SELECT if(number % 5 = 0, NULL, toInt128(sipHash64(number))) AS x, number % 3 = 0 AS c FROM numbers(200000))
    UNION ALL
    SELECT 'Decimal128',
           (min(x) = arraySort(groupArray(x))[1]) AND (max(x) = arraySort(groupArray(x))[-1])
       AND (minIf(x, c) = arraySort(groupArrayIf(x, c))[1]) AND (maxIf(x, c) = arraySort(groupArrayIf(x, c))[-1])
    FROM (SELECT if(number % 5 = 0, NULL, toDecimal128(sipHash64(number) % 1000000000, 2)) AS x, number % 3 = 0 AS c FROM numbers(200000))
    UNION ALL
    SELECT 'Int256',
           (min(x) = arraySort(groupArray(x))[1]) AND (max(x) = arraySort(groupArray(x))[-1])
       AND (minIf(x, c) = arraySort(groupArrayIf(x, c))[1]) AND (maxIf(x, c) = arraySort(groupArrayIf(x, c))[-1])
    FROM (SELECT if(number % 5 = 0, NULL, toInt256(sipHash64(number))) AS x, number % 3 = 0 AS c FROM numbers(100000))
)
ORDER BY t;

SELECT 'extremes',
       min(i128) = toInt128('-170141183460469231731687303715884105728'),
       max(i128) = toInt128('170141183460469231731687303715884105727'),
       min(u128) = toUInt128(0),
       max(u128) = toUInt128('340282366920938463463374607431768211455'),
       min(i256) = toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968'),
       max(i256) = toInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967'),
       min(u256) = toUInt256(0),
       max(u256) = toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935')
FROM
(
    SELECT
        arrayJoin([toInt128('-170141183460469231731687303715884105728'), toInt128('170141183460469231731687303715884105727'), toInt128(0)]) AS i128,
        arrayJoin([toUInt128(0), toUInt128('340282366920938463463374607431768211455')]) AS u128,
        arrayJoin([toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968'), toInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967')]) AS i256,
        arrayJoin([toUInt256(0), toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935')]) AS u256
);

-- Values that only differ in the low limb must not be confused with each other.
SELECT 'limb-boundary', min(x), max(x)
FROM (SELECT arrayJoin([toUInt128('18446744073709551615'), toUInt128('18446744073709551616'), toUInt128(0)]) AS x);

SELECT 'sign-boundary', min(x), max(x)
FROM (SELECT arrayJoin([toInt128(-1), toInt128(0), toInt128(1), toInt128('18446744073709551616'), toInt128('-18446744073709551616')]) AS x);

SELECT 'empty', min(toInt128(number)), max(toInt128(number)), min(toDecimal128(number, 2)) FROM numbers(0);
SELECT 'single', min(toInt128(-5)), max(toInt128(-5)), min(toUInt256(7)), max(toDecimal256(-3, 5));
SELECT 'all-null', min(x), max(x) FROM (SELECT CAST(NULL, 'Nullable(Int128)') AS x FROM numbers(1000));
SELECT 'if-none', minIf(toInt128(number), 0), maxIf(toDecimal128(number, 1), 0) FROM numbers(1000);
SELECT 'all-equal', min(x), max(x) FROM (SELECT toInt128(42) AS x FROM numbers(300000));

SELECT 'group-by', sum(mn), sum(mx)
FROM (SELECT number % 1000 AS k, min(toInt128(sipHash64(number))) AS mn, max(toInt128(sipHash64(number))) AS mx FROM numbers(200000) GROUP BY k);

SELECT 'argmin-argmax', t, ok
FROM
(
    SELECT 'Int128' AS t, (argMin(number, x) = 1234) AND (argMax(number, x) = 5678) AS ok
    FROM (SELECT number, toInt128(if(number = 1234, -1000000, if(number = 5678, 1000000, 0))) AS x FROM numbers(100000))
    UNION ALL
    SELECT 'Decimal128', (argMin(number, x) = 1234) AND (argMax(number, x) = 5678)
    FROM (SELECT number, toDecimal128(if(number = 1234, -1000000, if(number = 5678, 1000000, 0)), 2) AS x FROM numbers(100000))
    UNION ALL
    SELECT 'UInt256', (argMinIf(number, x, number % 2 = 0) = 1234) AND (argMaxIf(number, x, number % 2 = 0) = 5678)
    FROM (SELECT number, toUInt256(if(number = 1234, 1, if(number = 5678, 1000000, 500))) AS x FROM numbers(100000))
)
ORDER BY t;

SELECT 'order-limit1',
       (SELECT toInt128(sipHash64(number)) AS x FROM numbers(100000) ORDER BY x ASC LIMIT 1) = (SELECT min(toInt128(sipHash64(number))) FROM numbers(100000)),
       (SELECT toInt128(sipHash64(number)) AS x FROM numbers(100000) ORDER BY x DESC LIMIT 1) = (SELECT max(toInt128(sipHash64(number))) FROM numbers(100000));

SELECT 'array-minmax', t, ok
FROM
(
    SELECT 'Int128' AS t, countIf(arrayMin(a) != arraySort(a)[1] OR arrayMax(a) != arraySort(a)[-1]) = 0 AS ok
    FROM (SELECT arrayMap(i -> toInt128(sipHash64(number, i)) * toInt128(-1 + (i % 2) * 2), range(1 + number % 17)) AS a FROM numbers(20000))
    UNION ALL
    SELECT 'UInt128', countIf(arrayMin(a) != arraySort(a)[1] OR arrayMax(a) != arraySort(a)[-1]) = 0
    FROM (SELECT arrayMap(i -> toUInt128(sipHash64(number, i)), range(1 + number % 17)) AS a FROM numbers(20000))
    UNION ALL
    SELECT 'Int256', countIf(arrayMin(a) != arraySort(a)[1] OR arrayMax(a) != arraySort(a)[-1]) = 0
    FROM (SELECT arrayMap(i -> toInt256(sipHash64(number, i)) * toInt256(-1 + (i % 2) * 2), range(1 + number % 17)) AS a FROM numbers(20000))
    UNION ALL
    SELECT 'UInt256', countIf(arrayMin(a) != arraySort(a)[1] OR arrayMax(a) != arraySort(a)[-1]) = 0
    FROM (SELECT arrayMap(i -> toUInt256(sipHash64(number, i)), range(1 + number % 17)) AS a FROM numbers(20000))
)
ORDER BY t;

-- Empty arrays mixed into a batch must still yield the default, not a neighbour's extreme.
SELECT 'array-empty', groupArray(arrayMin(a)), groupArray(arrayMax(a))
FROM (SELECT CAST(if(number % 2 = 0, [], [toInt128(-7), toInt128(3)]), 'Array(Int128)') AS a FROM numbers(4));
