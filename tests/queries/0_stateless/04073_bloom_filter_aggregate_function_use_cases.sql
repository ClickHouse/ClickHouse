-- Tests for groupBloomFilter aggregate function -- const/non-const arguments and main use case

-- Both arguments are constants (bloom from WITH clause, value is literal)
WITH (SELECT groupBloomFilterState(1000)(number) FROM numbers(100)) AS bf
SELECT bloomFilterContains(bf, toUInt64(42)) AS result;

-- Const String bloom state from WITH clause
WITH (SELECT groupBloomFilterState(1000)(toString(number)) FROM numbers(100)) AS bf
SELECT bloomFilterContains(bf, '42') AS result;

-- Const String bloom state with non-const String probe column
WITH (SELECT groupBloomFilterState(1000)(toString(number)) FROM numbers(100)) AS bf
SELECT sum(bloomFilterContains(bf, toString(number))) AS result
FROM numbers(10);

-- Const DateTime64 bloom state from WITH clause
WITH (SELECT groupBloomFilterState(1000)(toDateTime64('2023-01-01 12:00:00.123', 3)) FROM numbers(1)) AS bf
SELECT bloomFilterContains(bf, toDateTime64('2023-01-01 12:00:00.123', 3)) AS result;

-- Const DateTime64 bloom state with non-const DateTime64 probe column
WITH (SELECT groupBloomFilterState(1000)(toDateTime64('2023-01-01 12:00:00.123', 3)) FROM numbers(1)) AS bf
SELECT sum(bloomFilterContains(bf, toDateTime64('2023-01-01 12:00:00.123', 3) + toIntervalMillisecond(number - number))) AS result
FROM numbers(10);

-- Bloom is column, value is column (per-row check)
SELECT bloomFilterContains(bf, val) AS result
FROM (
    SELECT
        groupBloomFilterState(1000)(number) AS bf,
        toUInt64(42) AS val
    FROM numbers(100)
);

-- Count values in 100..199 that are absent from filter built on 0..99
WITH (
    SELECT groupBloomFilterState(1000)(number)
    FROM numbers(100)
) AS old_bloom
SELECT count() AS new_values_count
FROM numbers(200)
WHERE number >= 100
  AND NOT (bloomFilterContains(old_bloom, number));

-- All 100 values from 100..199 must be found as new (no false negatives)
WITH (
    SELECT groupBloomFilterState(1000)(number)
    FROM numbers(100)
) AS old_bloom
SELECT count() = 100 AS all_new_values_found
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
