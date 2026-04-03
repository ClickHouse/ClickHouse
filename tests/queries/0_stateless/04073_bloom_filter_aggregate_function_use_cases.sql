-- Tests for groupBloomFilter aggregate function -- const/non-const arguments and main use case

-- Both arguments are constants (bloom from WITH clause, value is literal)
WITH (SELECT groupBloomFilterState(1000)(number) FROM numbers(100)) AS bf
SELECT bloomFilterContains(bf, toUInt64(42)) AS result;

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
  AND NOT bloomFilterContains(old_bloom, number);

-- All 100 values from 100..199 must be found as new (no false negatives)
WITH (
    SELECT groupBloomFilterState(1000)(number)
    FROM numbers(100)
) AS old_bloom
SELECT count() = 100 AS all_new_values_found
FROM numbers(200)
WHERE number >= 100
  AND NOT bloomFilterContains(old_bloom, number);

-- No values from 0..99 must appear as new (no false negatives)
WITH (
    SELECT groupBloomFilterState(1000)(number)
    FROM numbers(100)
) AS old_bloom
SELECT count() = 0 AS no_false_negatives
FROM numbers(100)
WHERE NOT bloomFilterContains(old_bloom, number);
