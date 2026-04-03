-- Tests for groupBloomFilter aggregate function -- parameters and validation

-- Explicit false positive rate
SELECT bloomFilterContains(groupBloomFilterState(1000, 0.01)(number), toUInt64(50)) AS result
FROM numbers(100);

-- Explicit filter size and num hashes
SELECT bloomFilterContains(groupBloomFilterState(4096, 5)(number), toUInt64(50)) AS result
FROM numbers(100);

-- Explicit seed parameter
SELECT bloomFilterContains(groupBloomFilterState(1000, 0.01, 42)(number), toUInt64(50)) AS result
FROM numbers(100);

-- No parameters: uses defaults (expected_elements=10000, false_positive_rate=0.025)
SELECT bloomFilterContains(groupBloomFilterState()(number), toUInt64(42)) AS result
FROM numbers(100);

-- No parameters: value absent from filter
SELECT bloomFilterContains(groupBloomFilterState()(number), toUInt64(200)) AS result
FROM numbers(100);

-- No parameters: type name is still correct
SELECT toTypeName(groupBloomFilterState()(number)) LIKE 'AggregateFunction(groupBloomFilter%' AS is_correct_type
FROM numbers(10);

-- groupBloomFilter() with same params are merge-compatible
SELECT bloomFilterContains(
    groupBloomFilterMergeState(state),
    toUInt64(5)
) AS result
FROM (
    SELECT groupBloomFilterState()(toUInt64(number)) AS state FROM numbers(10)
    UNION ALL
    SELECT groupBloomFilterState()(toUInt64(number + 100)) AS state FROM numbers(10)
);

-- (1000, 1): second param is integer >= 1, treated as num_hashes (not false_positive_rate)
SELECT bloomFilterContains(groupBloomFilterState(1000, 1)(number), toUInt64(50)) AS result
FROM numbers(100);

-- (1000, 0.999): second param is float in (0,1), treated as false_positive_rate (very high FPR = tiny filter)
SELECT toTypeName(groupBloomFilterState(1000, 0.999)(number)) LIKE 'AggregateFunction(groupBloomFilter%' AS is_valid
FROM numbers(10);

-- expected_elements = 0 must throw
SELECT groupBloomFilterState(0)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }

-- filter_size_bytes = 0 must throw
SELECT groupBloomFilterState(0, 5)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }

-- num_hashes = 0 must throw
SELECT groupBloomFilterState(1000, 0)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }

-- false_positive_rate = 0.0 must throw
SELECT groupBloomFilterState(1000, 0.0)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }

-- More than 3 parameters must throw
SELECT groupBloomFilterState(1000, 0.01, 0, 99)(number) FROM numbers(10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
