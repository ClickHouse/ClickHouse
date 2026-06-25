-- Tests for groupBloomFilter aggregate function and bloomFilterContains scalar function
-- Basic functionality

-- Value present in filter
SELECT bloomFilterContains(groupBloomFilterState(1000)(number), toUInt64(42)) AS result
FROM numbers(100);

-- Value absent from filter (definitely not present)
SELECT bloomFilterContains(groupBloomFilterState(1000)(number), toUInt64(200)) AS result
FROM numbers(100);

-- State from subquery used directly with bloomFilterContains
SELECT bloomFilterContains(state, toUInt64(42)) AS result
FROM (
    SELECT groupBloomFilterState(1000)(number) AS state
    FROM numbers(100)
);

-- groupBloomFilter without -State combinator has no meaningful scalar result.
SELECT groupBloomFilter(1000)(number) AS result
FROM numbers(100); -- { serverError BAD_ARGUMENTS }

-- groupBloomFilter without -State combinator must still throw when all Nullable inputs are NULL.
SELECT groupBloomFilter(1000)(materialize(CAST(NULL, 'Nullable(UInt64)')))
FROM numbers(1); -- { serverError BAD_ARGUMENTS }

-- Type name of state is AggregateFunction(groupBloomFilter...)
SELECT toTypeName(groupBloomFilterState(1000)(number)) LIKE 'AggregateFunction(groupBloomFilter%' AS is_correct_type
FROM numbers(10);
