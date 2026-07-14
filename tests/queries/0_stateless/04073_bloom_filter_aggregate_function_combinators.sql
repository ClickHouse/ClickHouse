-- Supported and rejected combinator chains for `groupBloomFilter`.

-- `bloomFilterContains` accepts `groupBloomFilterIfState` because `-If` preserves the Bloom filter state representation.
WITH
    (SELECT groupBloomFilterIfState(1000)(number, number = 42) FROM numbers(100)) AS bf
SELECT
    bloomFilterContains(bf, toUInt64(42)),
    bloomFilterContains(bf, toUInt64(41)),
    toTypeName(bf) LIKE 'AggregateFunction(1, groupBloomFilterIf%';

WITH
    (SELECT groupBloomFilterIfState(1000)(number, 0) FROM numbers(100)) AS bf
SELECT bloomFilterContains(bf, toUInt64(42));

-- `-Array` and `-If` preserve the Bloom filter state representation as well.
WITH
    (SELECT groupBloomFilterArrayIfState(100)([toUInt64(42), toUInt64(43)], toUInt8(1)) FROM numbers(1)) AS included_bf,
    (SELECT groupBloomFilterArrayIfState(100)([toUInt64(42)], toUInt8(0)) FROM numbers(1)) AS skipped_bf
SELECT
    bloomFilterContains(included_bf, toUInt64(42)),
    bloomFilterContains(included_bf, toUInt64(43)),
    bloomFilterContains(skipped_bf, toUInt64(42)),
    toTypeName(included_bf) LIKE 'AggregateFunction(1, groupBloomFilterArrayIf%';

-- Finalized forms have no meaningful scalar result.
SELECT groupBloomFilter(1000)(number) AS result
FROM numbers(100); -- { serverError BAD_ARGUMENTS }

SELECT groupBloomFilter(1000)(materialize(CAST(NULL, 'Nullable(UInt64)')))
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

SELECT groupBloomFilterDistinct(1000)(materialize(CAST(NULL, 'Nullable(UInt64)')))
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

SELECT groupBloomFilterOrNull(1000)(number)
FROM numbers(0); -- { serverError BAD_ARGUMENTS }

SELECT groupBloomFilterOrDefault(1000)(number)
FROM numbers(0); -- { serverError BAD_ARGUMENTS }

SELECT groupBloomFilterIfOrNull(1000)(number, 0)
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

SELECT groupBloomFilterIfOrDefault(1000)(number, 0)
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

SELECT groupBloomFilterMerge(1000)(state) FROM
(
    SELECT groupBloomFilterState(1000)(number) AS state FROM numbers(10)
    UNION ALL
    SELECT groupBloomFilterState(1000)(number + 10) AS state FROM numbers(10)
); -- { serverError BAD_ARGUMENTS }

-- Nullable arguments require a null adapter whose state is not interchangeable with
-- the state of `groupBloomFilterIf` or nested variants such as `groupBloomFilterArrayIf`.
SELECT groupBloomFilterIfState(CAST(number, 'Nullable(UInt64)'), toUInt8(1))
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

SELECT groupBloomFilterIfState(number, CAST(1, 'Nullable(UInt8)'))
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

SELECT groupBloomFilterArrayIfState([number], CAST(1, 'Nullable(UInt8)'))
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

-- `-OrNull` and `-OrDefault` require a meaningful finalized result, even in state-only chains.
SELECT groupBloomFilterOrNullState(1000)(number)
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

SELECT groupBloomFilterOrDefaultState(1000)(number)
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

SELECT groupBloomFilterIfOrDefaultState(1000)(number, toUInt8(1))
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

-- `-Distinct` keeps an additional set state that cannot be consumed by `bloomFilterContains`.
SELECT groupBloomFilterDistinctState(1000)(number)
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

SELECT groupBloomFilterDistinctMergeState(1000)(state)
FROM
(
    SELECT groupBloomFilterState(1000)(number) AS state
    FROM numbers(1)
); -- { serverError BAD_ARGUMENTS }

-- Reject `-Distinct` recursively when it wraps another supported combinator.
SELECT groupBloomFilterIfDistinctState(1000)(number, number = 0)
FROM numbers(1); -- { serverError BAD_ARGUMENTS }
