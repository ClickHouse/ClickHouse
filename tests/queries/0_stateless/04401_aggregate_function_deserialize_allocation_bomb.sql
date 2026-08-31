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

-- A size at or below a cap is still only a declaration, so it must not become an allocation
-- either. Every blob below declares a size the caps above accept and supplies no payload for
-- it. The memory limit is what discriminates: reading the declared size first needs 1-8 GiB and
-- fails with MEMORY_LIMIT_EXCEEDED, reading payload-first needs about 30 KiB and reports the
-- truncation. The window is wide on purpose - three orders of magnitude on either side.

-- StatCommon read(): all four consumers, and each of size_x / size_y on its own.
-- The blob is varint(1<<30), varint(0): exactly the cap, which the strict > admits.
SELECT mannWhitneyUTestMerge(x) FROM (SELECT CAST(unhex('808080800400'), 'AggregateFunction(mannWhitneyUTest, Float64, UInt8)') AS x) SETTINGS max_memory_usage = 100000000; -- { serverError CANNOT_READ_ALL_DATA }
SELECT kolmogorovSmirnovTestMerge(x) FROM (SELECT CAST(unhex('808080800400'), 'AggregateFunction(kolmogorovSmirnovTest, Float64, UInt8)') AS x) SETTINGS max_memory_usage = 100000000; -- { serverError CANNOT_READ_ALL_DATA }
SELECT rankCorrMerge(x) FROM (SELECT CAST(unhex('808080800400'), 'AggregateFunction(rankCorr, Float64, Float64)') AS x) SETTINGS max_memory_usage = 100000000; -- { serverError CANNOT_READ_ALL_DATA }
SELECT finalizeAggregation(CAST(unhex('808080800400'), 'AggregateFunction(largestTriangleThreeBuckets(3), Float64, Float64)')) SETTINGS max_memory_usage = 100000000; -- { serverError CANNOT_READ_ALL_DATA }
-- size_x = 0, size_y = 1<<30: the second limb is independently reachable.
SELECT mannWhitneyUTestMerge(x) FROM (SELECT CAST(unhex('008080808004'), 'AggregateFunction(mannWhitneyUTest, Float64, UInt8)') AS x) SETTINGS max_memory_usage = 100000000; -- { serverError CANNOT_READ_ALL_DATA }
-- One past the cap still belongs to the cap, not to the read.
SELECT mannWhitneyUTestMerge(x) FROM (SELECT CAST(unhex('818080800400'), 'AggregateFunction(mannWhitneyUTest, Float64, UInt8)') AS x); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- timeSeriesGroupArray: format version, then an 8-byte count with no cap at all.
SELECT finalizeAggregation(CAST(unhex('010000CA9A3B00000000'), 'AggregateFunction(timeSeriesGroupArray, DateTime, Float64)')) SETTINGS allow_experimental_time_series_aggregate_functions = 1, max_memory_usage = 100000000; -- { serverError CANNOT_READ_ALL_DATA }
-- count = 2^62+1, chosen so count * 12 wraps to 12: without the overflow check a 12-byte payload
-- passes the sufficiency test and the element loop then writes past an undersized allocation.
SELECT finalizeAggregation(CAST(unhex('0100' || '0100000000000040' || '000000000000000000000000'), 'AggregateFunction(timeSeriesGroupArray, DateTime, Float64)')) SETTINGS allow_experimental_time_series_aggregate_functions = 1, max_memory_usage = 100000000; -- { serverError TOO_LARGE_ARRAY_SIZE }

-- timeSeriesChangesToGrid reaches the same pattern through one bucket's sample count.
SELECT finalizeAggregation(CAST(unhex('0300' || '0100000000000000' || '0100000000000000' || '0000000000000000' || '0000008000000000'), 'AggregateFunction(timeSeriesChangesToGrid(0, 0, 1, 1), DateTime, Float64)')) SETTINGS allow_experimental_time_series_aggregate_functions = 1, max_memory_usage = 100000000; -- { serverError CANNOT_READ_ALL_DATA }

-- readStringBinaryInto is shared by -Distinct, groupUniqArray and generic groupArrayIntersect,
-- so one element declaring ~1 GiB is enough whatever the element count says.
SELECT finalizeAggregation(CAST(unhex('01FFFFFFFF03'), 'AggregateFunction(groupUniqArray, String)')) SETTINGS max_memory_usage = 100000000; -- { serverError CANNOT_READ_ALL_DATA }

-- ColumnString::deserializeAndInsertFromArena, reached by feeding attacker bytes back through a
-- non-plain column: the element is 8 bytes of size_t declaring 2 GiB and nothing after it.
SELECT finalizeAggregation(CAST(unhex('01080000008000000000'), 'AggregateFunction(groupUniqArray, Tuple(String))')) SETTINGS max_memory_usage = 100000000; -- { serverError CANNOT_READ_ALL_DATA }

-- serialize() walks a map, so a repeated key cannot come from a writer. Each repetition used to
-- allocate a nested state that emplace then dropped.
SELECT finalizeAggregation(CAST(unhex('020161000000000000000001610000000000000000'), 'AggregateFunction(sumMap, Map(String, UInt64))')); -- { serverError INCORRECT_DATA }

-- The caps added earlier still admit their own value, so the four reserves behind them are here too.
SELECT finalizeAggregation(CAST(unhex('00FFC1D72F'), 'AggregateFunction(groupArrayIntersect, Array(UInt64))')) SETTINGS max_memory_usage = 100000000; -- { serverError CANNOT_READ_ALL_DATA }
SELECT finalizeAggregation(CAST(unhex('00FFC1D72F'), 'AggregateFunction(groupArrayIntersect, Array(String))')) SETTINGS max_memory_usage = 100000000; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
SELECT finalizeAggregation(CAST(unhex('00FFE0F50500000000'), 'AggregateFunction(sequenceMatch(\'(?1)\'), DateTime, UInt8, UInt8, UInt8)')) SETTINGS max_memory_usage = 100000000; -- { serverError CANNOT_READ_ALL_DATA }
SELECT finalizeAggregation(CAST(unhex('10270000000000007b14ae47e17a843f0000000000000000FFE0F50500000000'), 'AggregateFunction(quantileGK(100), Float64)')) SETTINGS max_memory_usage = 100000000; -- { serverError CANNOT_READ_ALL_DATA }

-- Legitimate states for the newly touched functions.
SELECT kolmogorovSmirnovTestMerge(s) FROM (SELECT kolmogorovSmirnovTestState(x, y) AS s FROM (SELECT number::Float64 AS x, (number % 2)::UInt8 AS y FROM numbers(100)));
SELECT rankCorrMerge(s) FROM (SELECT rankCorrState(number::Float64, (number * 2)::Float64) AS s FROM numbers(100));
SELECT arraySort(groupUniqArrayMerge(s)) FROM (SELECT groupUniqArrayState(toString(number % 5)) AS s FROM numbers(50));
SELECT arraySort(groupUniqArrayMerge(s)) FROM (SELECT groupUniqArrayState(tuple(toString(number % 4))) AS s FROM numbers(40));
SELECT finalizeAggregation(sumMapState(map(toString(number % 3), number::UInt64))) FROM numbers(9);
SELECT bitmapCardinality(groupBitmapStateMerge(s)) FROM (SELECT groupBitmapState(number::UInt64) AS s FROM numbers(300000));
SELECT length(finalizeAggregation(timeSeriesGroupArrayState(t, v))) FROM (SELECT number::DateTime AS t, number::Float64 AS v FROM numbers(1000)) SETTINGS allow_experimental_time_series_aggregate_functions = 1;
SELECT finalizeAggregation(timeSeriesChangesToGridState(10, 50, 10, 10)(t, v)) FROM (SELECT number::DateTime AS t, number::Float64 AS v FROM numbers(60)) SETTINGS allow_experimental_time_series_aggregate_functions = 1;

-- A CAST(unhex(...)) blob is always fully buffered, so the assertions above only exercise the
-- one-shot path. A state read back from a table crosses compressed-block boundaries, which is
-- the path that stages the payload before committing it, and it must round-trip unchanged.
DROP TABLE IF EXISTS t_deserialize_allocation_bomb_refill;
CREATE TABLE t_deserialize_allocation_bomb_refill
(
    gua AggregateFunction(groupUniqArray, String),
    tsga AggregateFunction(timeSeriesGroupArray, DateTime, Float64)
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_experimental_time_series_aggregate_functions = 1;

INSERT INTO t_deserialize_allocation_bomb_refill
SELECT
    (SELECT groupUniqArrayState(s) FROM (SELECT repeat('x', 1000000) AS s UNION ALL SELECT repeat('y', 1000000) AS s)),
    (SELECT timeSeriesGroupArrayState(t, v) FROM (SELECT number::DateTime AS t, number::Float64 AS v FROM numbers(300000)))
SETTINGS allow_experimental_time_series_aggregate_functions = 1;

SELECT sum(length(x)) FROM (SELECT arrayJoin(groupUniqArrayMerge(gua)) AS x FROM t_deserialize_allocation_bomb_refill);
SELECT length(finalizeAggregation(tsga)) FROM t_deserialize_allocation_bomb_refill SETTINGS allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE t_deserialize_allocation_bomb_refill;
