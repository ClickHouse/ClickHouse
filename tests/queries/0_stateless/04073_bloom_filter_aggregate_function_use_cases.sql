-- Tests for groupBloomFilter aggregate function -- const/non-const arguments and main use case

-- Const bloom states from WITH clauses
WITH
    (SELECT groupBloomFilterState(1000)(number) FROM numbers(100)) AS uint64_bf,
    (SELECT groupBloomFilterState(1000)(toString(number)) FROM numbers(100)) AS string_bf,
    (SELECT groupBloomFilterState(1000)(toDateTime64('2023-01-01 12:00:00.123', 3)) FROM numbers(1)) AS datetime64_bf
SELECT
    bloomFilterContains(uint64_bf, toUInt64(42)),
    bloomFilterContains(string_bf, '42'),
    bloomFilterContains(datetime64_bf, toDateTime64('2023-01-01 12:00:00.123', 3));

-- Const bloom states with non-const probe columns
WITH
    (SELECT groupBloomFilterState(1000)(toString(number)) FROM numbers(100)) AS string_bf,
    (SELECT groupBloomFilterState(1000)(toDateTime64('2023-01-01 12:00:00.123', 3)) FROM numbers(1)) AS datetime64_bf
SELECT
    (SELECT sum(bloomFilterContains(string_bf, toString(number))) FROM numbers(10)),
    (SELECT sum(bloomFilterContains(datetime64_bf, toDateTime64('2023-01-01 12:00:00.123', 3) + toIntervalMillisecond(number - number))) FROM numbers(10));

-- Bloom is column, value is column (per-row check)
SELECT bloomFilterContains(bf, val) AS result
FROM (
    SELECT
        groupBloomFilterState(1000)(number) AS bf,
        toUInt64(42) AS val
    FROM numbers(100)
);

-- Values in 100..199 are absent from filter built on 0..99
WITH (
    SELECT groupBloomFilterState(1000)(number)
    FROM numbers(100)
) AS old_bloom
SELECT
    count() AS new_values_count,
    count() = 100 AS all_new_values_found
FROM numbers(200)
WHERE number >= 100
  AND NOT (bloomFilterContains(old_bloom, number));

-- No values from 0..99 must appear as new (no false negatives)
WITH (
    SELECT groupBloomFilterState(1000)(number)
    FROM numbers(100)
) AS old_bloom
SELECT count() = 0 AS no_false_negatives
FROM numbers(100)
WHERE NOT (bloomFilterContains(old_bloom, number));

-- Bloom states built per group and then probed per group.
SELECT key, bloomFilterContains(bf, toUInt64(key + 4)) AS result
FROM
(
    SELECT number % 2 AS key, groupBloomFilterState(1000)(number) AS bf
    FROM numbers(10)
    GROUP BY key
)
ORDER BY key;
