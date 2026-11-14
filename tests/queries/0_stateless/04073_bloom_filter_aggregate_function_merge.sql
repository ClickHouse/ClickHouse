-- Merge compatibility for `groupBloomFilter` aggregate states.

-- Different filter sizes are incompatible.
-- The analyzer produces a `Variant` for `UNION ALL`; the legacy analyzer fails during type unification.
SELECT groupBloomFilterMerge(state) FROM
(
    SELECT groupBloomFilterState(100)(number) AS state FROM numbers(10)
    UNION ALL
    SELECT groupBloomFilterState(200)(number) AS state FROM numbers(10)
); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT,NO_COMMON_TYPE }

-- Different seeds are incompatible.
SELECT groupBloomFilterMerge(state) FROM
(
    SELECT groupBloomFilterState(1000, 0.01, 0)(number) AS state FROM numbers(10)
    UNION ALL
    SELECT groupBloomFilterState(1000, 0.01, 42)(number) AS state FROM numbers(10)
); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT,NO_COMMON_TYPE }

-- Merge with an empty right- or left-hand state.
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
