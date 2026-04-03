-- Tests for groupBloomFilter aggregate function -- empty/boundary data and merge

-- Aggregation over empty set: bloomFilterContains must return 0
SELECT bloomFilterContains(
    groupBloomFilterState(1000)(number),
    toUInt64(42)
) AS result
FROM numbers(0);

-- Empty string not in filter (filter built from toString(0..9))
SELECT bloomFilterContains(
    groupBloomFilterState(1000)(toString(number)),
    ''
) AS result
FROM numbers(10);

-- Empty string explicitly inserted into filter
SELECT bloomFilterContains(
    groupBloomFilterState(1000)(s),
    ''
) AS result
FROM (SELECT '' AS s);

-- Very long string (1000 chars)
SELECT bloomFilterContains(
    groupBloomFilterState(1000)(repeat('x', 1000)),
    repeat('x', 1000)
) AS result
FROM numbers(1);

-- Merge of incompatible filters (different size) must throw.
-- UNION ALL of states with different parameters produces a Variant type,
-- which is rejected at the type-checking level before merge is attempted.
SELECT groupBloomFilterMerge(state) FROM (
    SELECT groupBloomFilterState(100)(number) AS state FROM numbers(10)
    UNION ALL
    SELECT groupBloomFilterState(200)(number) AS state FROM numbers(10)
); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Merge of incompatible filters (different seed) must throw.
SELECT groupBloomFilterMerge(state) FROM (
    SELECT groupBloomFilterState(1000, 0.01, 0)(number) AS state FROM numbers(10)
    UNION ALL
    SELECT groupBloomFilterState(1000, 0.01, 42)(number) AS state FROM numbers(10)
); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Merge with empty rhs: result equals lhs
SELECT bloomFilterContains(
    groupBloomFilterMergeState(1000)(state),
    toUInt64(42)
) AS result
FROM (
    SELECT groupBloomFilterState(1000)(number) AS state FROM numbers(100)
    UNION ALL
    SELECT groupBloomFilterState(1000)(number) AS state FROM numbers(0)
);

-- Merge into empty lhs: result equals rhs
SELECT bloomFilterContains(
    groupBloomFilterMergeState(1000)(state),
    toUInt64(42)
) AS result
FROM (
    SELECT groupBloomFilterState(1000)(number) AS state FROM numbers(0)
    UNION ALL
    SELECT groupBloomFilterState(1000)(number) AS state FROM numbers(100)
);
