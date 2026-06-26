-- Tests for groupBloomFilter aggregate function -- empty/boundary data and merge

-- Empty and boundary values
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

-- Merge of incompatible filters (different size) must throw.
-- New analyzer: UNION ALL produces a Variant, rejected by groupBloomFilterMerge (ILLEGAL_TYPE_OF_ARGUMENT).
-- Old analyzer: UNION ALL fails at type-unification stage (NO_COMMON_TYPE).
SELECT groupBloomFilterMerge(state) FROM (
    SELECT groupBloomFilterState(100)(number) AS state FROM numbers(10)
    UNION ALL
    SELECT groupBloomFilterState(200)(number) AS state FROM numbers(10)
); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT,NO_COMMON_TYPE }

-- Merge of incompatible filters (different seed) must throw.
SELECT groupBloomFilterMerge(state) FROM (
    SELECT groupBloomFilterState(1000, 0.01, 0)(number) AS state FROM numbers(10)
    UNION ALL
    SELECT groupBloomFilterState(1000, 0.01, 42)(number) AS state FROM numbers(10)
); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT,NO_COMMON_TYPE }

-- groupBloomFilterMerge finalization has no meaningful scalar result.
SELECT groupBloomFilterMerge(1000)(state) FROM (
    SELECT groupBloomFilterState(1000)(number) AS state FROM numbers(10)
    UNION ALL
    SELECT groupBloomFilterState(1000)(number + 10) AS state FROM numbers(10)
); -- { serverError BAD_ARGUMENTS }

-- Merge with empty rhs/lhs
WITH
    (
        SELECT groupBloomFilterMergeState(1000)(state)
        FROM
        (
            SELECT groupBloomFilterState(1000)(number) AS state FROM numbers(100)
            UNION ALL
            SELECT groupBloomFilterState(1000)(number) AS state FROM numbers(0)
        )
    ) AS merge_with_empty_rhs_bf,
    (
        SELECT groupBloomFilterMergeState(1000)(state)
        FROM
        (
            SELECT groupBloomFilterState(1000)(number) AS state FROM numbers(0)
            UNION ALL
            SELECT groupBloomFilterState(1000)(number) AS state FROM numbers(100)
        )
    ) AS merge_into_empty_lhs_bf
SELECT
    bloomFilterContains(merge_with_empty_rhs_bf, toUInt64(42)),
    bloomFilterContains(merge_into_empty_lhs_bf, toUInt64(42));

-- Malformed serialized state: filter_size_bytes = 0 must throw.
SELECT finalizeAggregation(CAST(unhex('00010000'), 'AggregateFunction(groupBloomFilter(1000), UInt64)')); -- { serverError INCORRECT_DATA }

-- Malformed serialized state: filter_size_bytes above maximum must throw.
SELECT finalizeAggregation(CAST(unhex('8180808001010000'), 'AggregateFunction(groupBloomFilter(1000), UInt64)')); -- { serverError INCORRECT_DATA }

-- Malformed serialized state: num_hashes = 0 must throw.
SELECT finalizeAggregation(CAST(unhex('08000000'), 'AggregateFunction(groupBloomFilter(1000), UInt64)')); -- { serverError INCORRECT_DATA }

-- Malformed serialized state: num_hashes above maximum must throw.
SELECT finalizeAggregation(CAST(unhex('08150000'), 'AggregateFunction(groupBloomFilter(1000), UInt64)')); -- { serverError INCORRECT_DATA }

-- Malformed serialized state: has_data must be 0 or 1.
SELECT finalizeAggregation(CAST(unhex('08010002'), 'AggregateFunction(groupBloomFilter(1000), UInt64)')); -- { serverError INCORRECT_DATA }

-- Forged serialized state: valid payload parameters must still match the declared aggregate type.
-- groupBloomFilter(1000) expects size=960, hashes=5, seed=0, but the payload says size=8, hashes=1, seed=0.
SELECT CAST(unhex('08010000'), 'AggregateFunction(groupBloomFilter(1000), UInt64)'); -- { serverError INCORRECT_DATA }

-- Forged serialized state: seed must match the declared aggregate type.
-- groupBloomFilter(4096, 5, 0) expects seed=0, but the payload says seed=42.
SELECT CAST(unhex('8020052a00'), 'AggregateFunction(groupBloomFilter(4096, 5, 0), UInt64)'); -- { serverError INCORRECT_DATA }
