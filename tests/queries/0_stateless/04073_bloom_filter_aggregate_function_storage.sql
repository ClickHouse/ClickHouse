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

OPTIMIZE TABLE bloom_filter_amt FINAL;

-- After merge, filter must contain values from both inserts (0..49 and 50..99)
SELECT
    key,
    bloomFilterContains(groupBloomFilterMergeState(1000)(bf), toUInt64(10)) AS has_10,
    bloomFilterContains(groupBloomFilterMergeState(1000)(bf), toUInt64(75)) AS has_75,
    bloomFilterContains(groupBloomFilterMergeState(1000)(bf), toUInt64(200)) AS has_200
FROM bloom_filter_amt
GROUP BY key;

-- Test type compatibility check on INSERT
-- Table expects AggregateFunction(groupBloomFilter(1000), UInt64)
-- But we try to insert AggregateFunction(groupBloomFilter(2000), UInt64)
-- This should fail with a type mismatch error
INSERT INTO bloom_filter_amt
SELECT 'k2', groupBloomFilterState(2000)(number)
FROM numbers(10); -- { serverError CANNOT_CONVERT_TYPE }

DROP TABLE bloom_filter_amt;
