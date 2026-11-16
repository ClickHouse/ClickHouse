-- Tests for groupBloomFilter aggregate function -- AggregatingMergeTree storage

-- Store and read bloom filter state in AggregatingMergeTree
CREATE TABLE bloom_filter_test
(
    key String,
    bf AggregateFunction(groupBloomFilter(1000), String)
)
ENGINE = AggregatingMergeTree
ORDER BY key;

INSERT INTO bloom_filter_test
SELECT
    'group1',
    groupBloomFilterState(1000)(toString(number))
FROM numbers(100);

INSERT INTO bloom_filter_test
SELECT
    'group2',
    groupBloomFilterState(1000)(toString(number + 1000))
FROM numbers(100);

SELECT
    key,
    bloomFilterContains(bf, '50') AS contains_50,
    bloomFilterContains(bf, '1050') AS contains_1050
FROM bloom_filter_test
ORDER BY key;

DROP TABLE bloom_filter_test;

-- AggregatingMergeTree with OPTIMIZE FINAL merges parts and ORs filter bits
CREATE TABLE bloom_filter_amt
(
    key String,
    bf AggregateFunction(groupBloomFilter(1000), UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY key;

INSERT INTO bloom_filter_amt
SELECT 'k', groupBloomFilterState(1000)(number)
FROM numbers(50);

INSERT INTO bloom_filter_amt
SELECT 'k', groupBloomFilterState(1000)(number + 50)
FROM numbers(50);

INSERT INTO bloom_filter_amt
SELECT 'k', groupBloomFilterState(1000, 0.025, 0)(number + 100)
FROM numbers(50);

OPTIMIZE TABLE bloom_filter_amt FINAL;

-- After merge, filter must contain values from all inserts (0..49, 50..99, and 100..149)
SELECT
    key,
    bloomFilterContains(groupBloomFilterMergeState(1000)(bf), toUInt64(10)) AS has_10,
    bloomFilterContains(groupBloomFilterMergeState(1000)(bf), toUInt64(75)) AS has_75,
    bloomFilterContains(groupBloomFilterMergeState(1000)(bf), toUInt64(125)) AS has_125,
    bloomFilterContains(groupBloomFilterMergeState(1000)(bf), toUInt64(200)) AS has_200
FROM bloom_filter_amt
GROUP BY key;

-- Equivalent parameter spellings must remain compatible through the automatic `Null` wrapper
-- used for Nullable value types.
CREATE TABLE bloom_filter_nullable_amt
(
    key String,
    bf AggregateFunction(groupBloomFilter, Nullable(UInt64))
)
ENGINE = AggregatingMergeTree
ORDER BY key;

INSERT INTO bloom_filter_nullable_amt
SELECT 'k', groupBloomFilterState(CAST(toUInt64(number), 'Nullable(UInt64)'))
FROM numbers(10);

INSERT INTO bloom_filter_nullable_amt
SELECT 'k', groupBloomFilterState(10000, 0.025, 0)(CAST(toUInt64(number + 100), 'Nullable(UInt64)'))
FROM numbers(10);

OPTIMIZE TABLE bloom_filter_nullable_amt FINAL;

SELECT
    key,
    bloomFilterContains(groupBloomFilterMergeState(bf), toUInt64(5)) AS has_5,
    bloomFilterContains(groupBloomFilterMergeState(bf), toUInt64(105)) AS has_105
FROM bloom_filter_nullable_amt
GROUP BY key;

DROP TABLE bloom_filter_nullable_amt;

-- Test type compatibility check on INSERT
-- Table expects AggregateFunction(groupBloomFilter(1000), UInt64)
-- But we try to insert AggregateFunction(groupBloomFilter(2000), UInt64)
-- This should fail with a type mismatch error
INSERT INTO bloom_filter_amt
SELECT 'k2', groupBloomFilterState(2000)(number)
FROM numbers(10); -- { serverError CANNOT_CONVERT_TYPE }

DROP TABLE bloom_filter_amt;

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
