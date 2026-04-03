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

-- groupBloomFilter without -State combinator returns 0 (placeholder)
SELECT groupBloomFilter(1000)(number) AS result
FROM numbers(100);

-- Type name of state is AggregateFunction(groupBloomFilter...)
SELECT toTypeName(groupBloomFilterState(1000)(number)) LIKE 'AggregateFunction(groupBloomFilter%' AS is_correct_type
FROM numbers(10);
