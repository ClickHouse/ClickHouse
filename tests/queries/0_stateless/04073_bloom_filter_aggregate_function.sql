-- Tests for groupBloomFilter aggregate function and bloomFilterContains scalar function
-- Basic functionality

-- Positive checks and state type name
WITH
    (SELECT groupBloomFilterState(1000)(number) FROM numbers(100)) AS bf,
    (SELECT groupBloomFilterState(1000)(number) AS state FROM numbers(100)) AS subquery_bf,
    (SELECT groupBloomFilterState(1000)(number) FROM numbers(10)) AS type_bf
SELECT
    bloomFilterContains(bf, toUInt64(42)),
    bloomFilterContains(bf, toUInt64(200)),
    bloomFilterContains(subquery_bf, toUInt64(42)),
    toTypeName(type_bf) LIKE 'AggregateFunction(groupBloomFilter%';

-- bloomFilterContains must accept groupBloomFilterIfState because the If combinator
-- keeps the same Bloom filter state representation as groupBloomFilterState.
WITH
    (SELECT groupBloomFilterIfState(1000)(number, number = 42) FROM numbers(100)) AS bf
SELECT
    bloomFilterContains(bf, toUInt64(42)),
    bloomFilterContains(bf, toUInt64(41)),
    toTypeName(bf) LIKE 'AggregateFunction(groupBloomFilterIf%';

WITH
    (SELECT groupBloomFilterIfState(1000)(number, 0) FROM numbers(100)) AS bf
SELECT bloomFilterContains(bf, toUInt64(42));

-- groupBloomFilter without -State combinator has no meaningful scalar result.
SELECT groupBloomFilter(1000)(number) AS result
FROM numbers(100); -- { serverError BAD_ARGUMENTS }

-- groupBloomFilter without -State combinator must still throw when all Nullable inputs are NULL.
SELECT groupBloomFilter(1000)(materialize(CAST(NULL, 'Nullable(UInt64)')))
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

-- groupBloomFilter without -State combinator must still throw through a wrapper when all Nullable inputs are NULL.
SELECT groupBloomFilterDistinct(1000)(materialize(CAST(NULL, 'Nullable(UInt64)')))
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

-- groupBloomFilterOrNull must not synthesize NULL for empty input.
SELECT groupBloomFilterOrNull(1000)(number)
FROM numbers(0); -- { serverError BAD_ARGUMENTS }

-- groupBloomFilterOrDefault must not synthesize a default scalar for empty input.
SELECT groupBloomFilterOrDefault(1000)(number)
FROM numbers(0); -- { serverError BAD_ARGUMENTS }

-- groupBloomFilterIfOrNull must not synthesize NULL when all rows are filtered out.
SELECT groupBloomFilterIfOrNull(1000)(number, 0)
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

-- groupBloomFilterIfOrDefault must not synthesize a default scalar when all rows are filtered out.
SELECT groupBloomFilterIfOrDefault(1000)(number, 0)
FROM numbers(1); -- { serverError BAD_ARGUMENTS }
