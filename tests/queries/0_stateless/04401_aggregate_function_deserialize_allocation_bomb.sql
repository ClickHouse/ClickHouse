-- Crafted aggregate-function states that declare a huge element count in their
-- serialized form must be rejected before the deserializer tries to allocate
-- gigabytes of memory. Each blob below is only a handful of bytes but encodes a
-- size of ~4.29e9 elements (0xffffffff). Without the size guards these requested
-- 32-96 GiB before reading any data.

-- mannWhitneyUTest / rankCorr / largestTriangleThreeBuckets share StatCommon read().
-- The shared guard is bounded at 1<<30 to match the existing largestTriangleThreeBuckets
-- contract (its MAX_ARRAY_SIZE), so no legitimate pre-existing state is rejected; the
-- crafted size below is ~4.29e9 (0xffffffff), still well above the bound.
SELECT mannWhitneyUTestMerge(x) FROM (SELECT CAST(unhex('ffffffff0f00'), 'AggregateFunction(mannWhitneyUTest, Float64, UInt8)') AS x); -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT finalizeAggregation(CAST(unhex('ffffffff0f00'), 'AggregateFunction(largestTriangleThreeBuckets(3), Float64, Float64)')); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- quantileGK
SELECT finalizeAggregation(CAST(unhex('10270000000000007b14ae47e17a843f0000000000000000ffffffff00000000'), 'AggregateFunction(quantileGK(100), Float64)')); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- sequenceMatch
SELECT finalizeAggregation(CAST(unhex('00ffffffff00000000'), 'AggregateFunction(sequenceMatch(\'(?1)\'), DateTime, UInt8, UInt8, UInt8)')); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- groupArrayIntersect has two independent deserialize paths and both are guarded.
-- Numeric path: Array(UInt64) -> AggregateFunctionGroupArrayIntersect<T>.
SELECT finalizeAggregation(CAST(unhex('00ffffffff0f'), 'AggregateFunction(groupArrayIntersect, Array(UInt64))')); -- { serverError TOO_LARGE_ARRAY_SIZE }
-- Generic path: Array(String) -> AggregateFunctionGroupArrayIntersectGeneric.
SELECT finalizeAggregation(CAST(unhex('00ffffffff0f'), 'AggregateFunction(groupArrayIntersect, Array(String))')); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- A legitimate state must still round-trip after the guards.
SELECT mannWhitneyUTestMerge(s) FROM (SELECT mannWhitneyUTestState(x, y) AS s FROM (SELECT number::Float64 AS x, (number % 2)::UInt8 AS y FROM numbers(100)));
SELECT quantileGKMerge(100, 0.5)(s) FROM (SELECT quantileGKState(100, 0.5)(number) AS s FROM numbers(1000));
SELECT sequenceMatchMerge('(?1)(?2)')(s) FROM (SELECT sequenceMatchState('(?1)(?2)')(toDateTime(number), number = 1, number = 2) AS s FROM numbers(10));
SELECT arraySort(groupArrayIntersectMerge(s)) FROM (SELECT groupArrayIntersectState([1::UInt64, 2, 3]) AS s UNION ALL SELECT groupArrayIntersectState([2::UInt64, 3, 4]));
SELECT arraySort(groupArrayIntersectMerge(s)) FROM (SELECT groupArrayIntersectState(['a', 'b', 'c']) AS s UNION ALL SELECT groupArrayIntersectState(['b', 'c', 'd']));
SELECT length(largestTriangleThreeBucketsMerge(3)(s)) FROM (SELECT largestTriangleThreeBucketsState(3)(number::Float64, number::Float64) AS s FROM numbers(100));
