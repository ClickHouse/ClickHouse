-- Tests the timeSeries{TopK,BottomK,LimitK}Masks aggregate functions: selection semantics, serialization, and argument validation.
-- The PromQL operators built on them are tested end to end in 04811_promql_topk_bottomk_limitk.

DROP TABLE IF EXISTS topk_input;
DROP TABLE IF EXISTS topk_states;

-- The functions are experimental and disabled by default.
SET allow_experimental_time_series_aggregate_functions = 0;
SET allow_experimental_time_series_table = 0;
SELECT timeSeriesTopKMasks(1, 1::UInt64, [1.]::Array(Nullable(Float64))); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

SET allow_experimental_time_series_aggregate_functions = 1;

SELECT '-- timeSeries*Masks: basic selection, ties, NULLs, k = 0, k > N';

CREATE TABLE topk_input (g UInt64, v Array(Nullable(Float64))) ENGINE = MergeTree ORDER BY g;
INSERT INTO topk_input VALUES (1, [10, 1, NULL, 5]), (2, [20, NULL, 2, 5]), (3, [30, 3, 1, NULL]);

SELECT timeSeriesTopKMasks(2, g, v) FROM topk_input;
-- The tie 5 vs 5 at the last time step is broken deterministically: the smaller key wins.
SELECT timeSeriesTopKMasks(1, g, v) FROM topk_input;
SELECT timeSeriesBottomKMasks(1, g, v) FROM topk_input;
SELECT timeSeriesBottomKMasks(2, g, v) FROM topk_input;
SELECT timeSeriesTopKMasks(0, g, v) FROM topk_input;
SELECT timeSeriesTopKMasks(10, g, v) FROM topk_input;
-- Per-step k (the form used when the PromQL `k` depends on the evaluation time).
SELECT timeSeriesTopKMasks([0, 1, 2, 3]::Array(UInt64), g, v) FROM topk_input;
-- Float32 values and values without Nullable.
SELECT timeSeriesTopKMasks(1, g, v::Array(Nullable(Float32))) FROM topk_input;
SELECT timeSeriesBottomKMasks(1, g, [g::Float64, 10 - g::Float64]::Array(Float64)) FROM topk_input;
-- Aggregation over no rows.
SELECT timeSeriesTopKMasks(2, g, v) FROM topk_input WHERE g > 100;

SELECT '-- timeSeries*Masks: NaN is chosen after any non-NaN value, for both topk and bottomk';

SELECT timeSeriesTopKMasks(1, t.1, t.2) FROM (SELECT arrayJoin([(1, [nan, nan]), (2, [1, nan]), (3, [2, NULL])]::Array(Tuple(UInt64, Array(Nullable(Float64))))) AS t);
SELECT timeSeriesTopKMasks(2, t.1, t.2) FROM (SELECT arrayJoin([(1, [nan, nan]), (2, [1, nan]), (3, [2, NULL])]::Array(Tuple(UInt64, Array(Nullable(Float64))))) AS t);
SELECT timeSeriesBottomKMasks(1, t.1, t.2) FROM (SELECT arrayJoin([(1, [nan, nan]), (2, [1, nan]), (3, [2, NULL])]::Array(Tuple(UInt64, Array(Nullable(Float64))))) AS t);

SELECT '-- timeSeriesLimitKMasks: selection by sampling key, candidacy still requires a value';

SELECT timeSeriesLimitKMasks(2, t.1, t.3, t.2) FROM (SELECT arrayJoin([(1, [1, NULL], 300), (2, [2, 2], 100), (3, [3, 3], 200)]::Array(Tuple(UInt64, Array(Nullable(Float64)), UInt64))) AS t);
SELECT timeSeriesLimitKMasks(1, t.1, t.3, t.2) FROM (SELECT arrayJoin([(1, [1, NULL], 300), (2, [2, 2], 100), (3, [3, 3], 200)]::Array(Tuple(UInt64, Array(Nullable(Float64)), UInt64))) AS t);
SELECT timeSeriesLimitKMasks(1, t.1, t.3, t.2) FROM (SELECT arrayJoin([(1, [1.], 100), (2, [2.], 100)]::Array(Tuple(UInt64, Array(Nullable(Float64)), UInt64))) AS t);

SELECT '-- timeSeries*Masks: states survive serialization to a MergeTree table and merging';

CREATE TABLE topk_states (part UInt8, st AggregateFunction(timeSeriesTopKMasks, UInt8, UInt64, Array(Nullable(Float64)))) ENGINE = MergeTree ORDER BY part;
INSERT INTO topk_states SELECT g % 2, timeSeriesTopKMasksState(2, g, v) FROM topk_input GROUP BY g % 2;
SELECT timeSeriesTopKMasksMerge(st) FROM topk_states;

-- There is no fixed cap on the number of time steps: a state with more than a million steps (a range of about 13 days at 1-second resolution) survives the same round trip.
TRUNCATE TABLE topk_states;
INSERT INTO topk_states SELECT 0, timeSeriesTopKMasksState(1, g, arrayWithConstant(1100000, toNullable(toFloat64(g)))) FROM (SELECT number + 1 AS g FROM numbers(2));
SELECT arrayMap(x -> (x.1, length(x.2), arraySum(x.2)), timeSeriesTopKMasksMerge(st)) FROM topk_states;

DROP TABLE topk_states;

SELECT '-- timeSeries*Masks: invalid arguments';

SELECT timeSeriesTopKMasks(1, g, arraySlice(v, 1, g)) FROM topk_input; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesTopKMasks(g, g, v) FROM topk_input; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesTopKMasks([1, 2]::Array(UInt64), g, v) FROM topk_input; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesTopKMasks(1, g::UInt32, v) FROM topk_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesTopKMasks(-1::Int64, g, v) FROM topk_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesTopKMasks(1, g, [1]::Array(UInt64)) FROM topk_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesLimitKMasks(1, g, v) FROM topk_input; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT timeSeriesTopKMasks(1)(2, g, v) FROM topk_input; -- { serverError AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS }
-- A corrupted serialized state claiming 2^56 - 1 time steps must fail once the actual payload runs out instead of attempting a huge allocation.
SELECT finalizeAggregation(CAST(unhex('0101FFFFFFFFFFFFFF7F'), 'AggregateFunction(timeSeriesTopKMasks, UInt64, UInt64, Array(Nullable(Float64)))')); -- { serverError CANNOT_READ_ALL_DATA }
-- A corrupted serialized state claiming more entries at a time step than k must be rejected.
SELECT finalizeAggregation(CAST(unhex('010101010000000000000002'), 'AggregateFunction(timeSeriesTopKMasks, UInt64, UInt64, Array(Nullable(Float64)))')); -- { serverError INCORRECT_DATA }

DROP TABLE topk_input;
