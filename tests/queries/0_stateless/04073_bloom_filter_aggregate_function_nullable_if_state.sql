-- Tests for groupBloomFilter aggregate function -- nullable If state compatibility

DROP TABLE IF EXISTS bloom_filter_nullable_if_amt;

CREATE TABLE bloom_filter_nullable_if_amt
(
    key String,
    bf AggregateFunction(groupBloomFilter, Nullable(UInt64))
)
ENGINE = AggregatingMergeTree
ORDER BY key;

INSERT INTO bloom_filter_nullable_if_amt
SELECT
    'k',
    groupBloomFilterIfState(CAST(toUInt64(number), 'Nullable(UInt64)'), number % 2 = 0)
FROM numbers(10);

INSERT INTO bloom_filter_nullable_if_amt
SELECT
    'k',
    groupBloomFilterIfState(10000, 0.025, 0)(CAST(toUInt64(number + 100), 'Nullable(UInt64)'), number % 2 = 0)
FROM numbers(10);

OPTIMIZE TABLE bloom_filter_nullable_if_amt FINAL;

SELECT
    key,
    bloomFilterContains(groupBloomFilterMergeState(bf), toUInt64(2)) AS has_2,
    bloomFilterContains(groupBloomFilterMergeState(bf), toUInt64(104)) AS has_104
FROM bloom_filter_nullable_if_amt
GROUP BY key;

DROP TABLE bloom_filter_nullable_if_amt;

DROP TABLE IF EXISTS bloom_filter_array_if_nullable_amt;

CREATE TABLE bloom_filter_array_if_nullable_amt
(
    key String,
    bf AggregateFunction(groupBloomFilter, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY key;

INSERT INTO bloom_filter_array_if_nullable_amt
SELECT
    'k',
    groupBloomFilterState(toUInt64(number))
FROM numbers(10);

INSERT INTO bloom_filter_array_if_nullable_amt
SELECT
    'k',
    groupBloomFilterArrayIfState([toUInt64(number + 100), toUInt64(number + 200)], CAST(number % 2 = 0, 'Nullable(UInt8)'))
FROM numbers(10);

OPTIMIZE TABLE bloom_filter_array_if_nullable_amt FINAL;

SELECT
    key,
    bloomFilterContains(groupBloomFilterMergeState(bf), toUInt64(2)) AS has_scalar_2,
    bloomFilterContains(groupBloomFilterMergeState(bf), toUInt64(104)) AS has_array_104,
    bloomFilterContains(groupBloomFilterMergeState(bf), toUInt64(204)) AS has_array_204
FROM bloom_filter_array_if_nullable_amt
GROUP BY key;

DROP TABLE bloom_filter_array_if_nullable_amt;
