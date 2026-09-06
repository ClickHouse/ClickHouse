-- Basic construction, execution shapes, and membership checks for `groupBloomFilter` and `bloomFilterContains`.

-- Positive checks and state type name.
WITH
    (SELECT groupBloomFilterState(1000)(number) FROM numbers(100)) AS bf,
    (SELECT groupBloomFilterState(1000)(number) AS state FROM numbers(100)) AS subquery_bf,
    (SELECT groupBloomFilterState(1000)(number) FROM numbers(10)) AS type_bf
SELECT
    bloomFilterContains(bf, toUInt64(42)),
    bloomFilterContains(bf, toUInt64(200)),
    bloomFilterContains(subquery_bf, toUInt64(42)),
    toTypeName(type_bf) LIKE 'AggregateFunction(1, groupBloomFilter%';

-- Const Bloom filter states from `WITH` clauses.
WITH
    (SELECT groupBloomFilterState(1000)(number) FROM numbers(100)) AS uint64_bf,
    (SELECT groupBloomFilterState(1000)(toString(number)) FROM numbers(100)) AS string_bf,
    (SELECT groupBloomFilterState(1000)(toDateTime64('2023-01-01 12:00:00.123', 3)) FROM numbers(1)) AS datetime64_bf
SELECT
    bloomFilterContains(uint64_bf, toUInt64(42)),
    bloomFilterContains(string_bf, '42'),
    bloomFilterContains(datetime64_bf, toDateTime64('2023-01-01 12:00:00.123', 3));

-- Const Bloom filter states with non-const probe columns.
WITH
    (SELECT groupBloomFilterState(1000)(toString(number)) FROM numbers(100)) AS string_bf,
    (SELECT groupBloomFilterState(1000)(toDateTime64('2023-01-01 12:00:00.123', 3)) FROM numbers(1)) AS datetime64_bf
SELECT
    (SELECT sum(bloomFilterContains(string_bf, toString(number))) FROM numbers(10)),
    (SELECT sum(bloomFilterContains(datetime64_bf, toDateTime64('2023-01-01 12:00:00.123', 3) + toIntervalMillisecond(number - number))) FROM numbers(10));

-- Bloom filter and probe are both columns.
SELECT bloomFilterContains(bf, val) AS result
FROM
(
    SELECT
        groupBloomFilterState(1000)(number) AS bf,
        toUInt64(42) AS val
    FROM numbers(100)
);

-- Values in 100..199 are absent from the filter built on 0..99.
WITH
(
    SELECT groupBloomFilterState(1000)(number)
    FROM numbers(100)
) AS old_bloom
SELECT
    count() AS new_values_count,
    count() = 100 AS all_new_values_found
FROM numbers(200)
WHERE number >= 100
  AND NOT bloomFilterContains(old_bloom, number);

-- Values from 0..99 must have no false negatives.
WITH
(
    SELECT groupBloomFilterState(1000)(number)
    FROM numbers(100)
) AS old_bloom
SELECT count() = 0 AS no_false_negatives
FROM numbers(100)
WHERE NOT bloomFilterContains(old_bloom, number);

-- Build and probe Bloom filter states per group.
SELECT key, bloomFilterContains(bf, toUInt64(key + 4)) AS result
FROM
(
    SELECT number % 2 AS key, groupBloomFilterState(1000)(number) AS bf
    FROM numbers(10)
    GROUP BY key
)
ORDER BY key;

-- Empty and boundary values.
WITH
    (SELECT groupBloomFilterState(1000)(number) FROM numbers(0)) AS empty_bf,
    (SELECT groupBloomFilterState(1000)(toString(number)) FROM numbers(10)) AS string_bf,
    (SELECT groupBloomFilterState(1000)(s) FROM (SELECT '' AS s)) AS empty_string_bf,
    (SELECT groupBloomFilterState(1000)(repeat('x', 1000)) FROM numbers(1)) AS long_string_bf
SELECT
    bloomFilterContains(empty_bf, toUInt64(42)),
    bloomFilterContains(string_bf, ''),
    bloomFilterContains(empty_string_bf, ''),
    bloomFilterContains(long_string_bf, repeat('x', 1000));
