-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: `DatabaseReplicated::dropTable` does not drop `TimeSeries` inner tables synchronously, so the deferred inner DROPs are rejected with "ON CLUSTER is not allowed for Replicated database".

-- Tests the streaming evaluation of PromQL topk/bottomk/limitk: the timeSeriesSelect{TopK,BottomK,LimitK}Groups aggregate functions and the query plan built around them.

DROP TABLE IF EXISTS topk_input;
DROP TABLE IF EXISTS topk_states;
DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';

-- The functions are experimental and disabled by default.
SET allow_experimental_time_series_aggregate_functions = 0;
SET allow_experimental_time_series_table = 0;
SELECT timeSeriesSelectTopKGroups(1::UInt64, [1.]::Array(Nullable(Float64)), 1); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

SET allow_experimental_time_series_aggregate_functions = 1;

SELECT '-- timeSeriesSelect*Groups: basic selection, ties, NULLs, k = 0, k > N';

CREATE TABLE topk_input (g UInt64, v Array(Nullable(Float64))) ENGINE = MergeTree ORDER BY g;
INSERT INTO topk_input VALUES (1, [10, 1, NULL, 5]), (2, [20, NULL, 2, 5]), (3, [30, 3, 1, NULL]);

SELECT timeSeriesSelectTopKGroups(g, v, 2) FROM topk_input;
-- The tie 5 vs 5 at the last time step is broken deterministically: the smaller group wins.
SELECT timeSeriesSelectTopKGroups(g, v, 1) FROM topk_input;
SELECT timeSeriesSelectBottomKGroups(g, v, 1) FROM topk_input;
SELECT timeSeriesSelectBottomKGroups(g, v, 2) FROM topk_input;
SELECT timeSeriesSelectTopKGroups(g, v, 0) FROM topk_input;
SELECT timeSeriesSelectTopKGroups(g, v, 10) FROM topk_input;
-- Per-step k (the form used when the PromQL `k` depends on the evaluation time).
SELECT timeSeriesSelectTopKGroups(g, v, [0, 1, 2, 3]::Array(UInt64)) FROM topk_input;
-- Float32 values and values without Nullable.
SELECT timeSeriesSelectTopKGroups(g, v::Array(Nullable(Float32)), 1) FROM topk_input;
SELECT timeSeriesSelectBottomKGroups(g, [g::Float64, 10 - g::Float64]::Array(Float64), 1) FROM topk_input;
-- Aggregation over no rows.
SELECT timeSeriesSelectTopKGroups(g, v, 2) FROM topk_input WHERE g > 100;

SELECT '-- timeSeriesSelect*Groups: NaN is chosen after any non-NaN value, for both topk and bottomk';

SELECT timeSeriesSelectTopKGroups(t.1, t.2, 1) FROM (SELECT arrayJoin([(1, [nan, nan]), (2, [1, nan]), (3, [2, NULL])]::Array(Tuple(UInt64, Array(Nullable(Float64))))) AS t);
SELECT timeSeriesSelectTopKGroups(t.1, t.2, 2) FROM (SELECT arrayJoin([(1, [nan, nan]), (2, [1, nan]), (3, [2, NULL])]::Array(Tuple(UInt64, Array(Nullable(Float64))))) AS t);
SELECT timeSeriesSelectBottomKGroups(t.1, t.2, 1) FROM (SELECT arrayJoin([(1, [nan, nan]), (2, [1, nan]), (3, [2, NULL])]::Array(Tuple(UInt64, Array(Nullable(Float64))))) AS t);

SELECT '-- timeSeriesSelectLimitKGroups: selection by sampling key, candidacy still requires a value';

SELECT timeSeriesSelectLimitKGroups(t.1, t.2, 2, t.3) FROM (SELECT arrayJoin([(1, [1, NULL], 300), (2, [2, 2], 100), (3, [3, 3], 200)]::Array(Tuple(UInt64, Array(Nullable(Float64)), UInt64))) AS t);
SELECT timeSeriesSelectLimitKGroups(t.1, t.2, 1, t.3) FROM (SELECT arrayJoin([(1, [1, NULL], 300), (2, [2, 2], 100), (3, [3, 3], 200)]::Array(Tuple(UInt64, Array(Nullable(Float64)), UInt64))) AS t);
SELECT timeSeriesSelectLimitKGroups(t.1, t.2, 1, t.3) FROM (SELECT arrayJoin([(1, [1.], 100), (2, [2.], 100)]::Array(Tuple(UInt64, Array(Nullable(Float64)), UInt64))) AS t);

SELECT '-- timeSeriesSelect*Groups: states survive serialization to a MergeTree table and merging';

CREATE TABLE topk_states (part UInt8, st AggregateFunction(timeSeriesSelectTopKGroups, UInt64, Array(Nullable(Float64)), UInt8)) ENGINE = MergeTree ORDER BY part;
INSERT INTO topk_states SELECT g % 2, timeSeriesSelectTopKGroupsState(g, v, 2) FROM topk_input GROUP BY g % 2;
SELECT timeSeriesSelectTopKGroupsMerge(st) FROM topk_states;

-- There is no fixed cap on the number of time steps: a state with more than a million steps (a range of about 13 days at 1-second resolution) survives the same round trip.
TRUNCATE TABLE topk_states;
INSERT INTO topk_states SELECT 0, timeSeriesSelectTopKGroupsState(g, arrayWithConstant(1100000, toNullable(toFloat64(g))), 1) FROM (SELECT number + 1 AS g FROM numbers(2));
SELECT arrayMap(x -> (x.1, length(x.2), arraySum(x.2)), timeSeriesSelectTopKGroupsMerge(st)) FROM topk_states;

DROP TABLE topk_states;

SELECT '-- timeSeriesSelect*Groups: invalid arguments';

SELECT timeSeriesSelectTopKGroups(g, arraySlice(v, 1, g), 1) FROM topk_input; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesSelectTopKGroups(g, v, g) FROM topk_input; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesSelectTopKGroups(g, v, [1, 2]::Array(UInt64)) FROM topk_input; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesSelectTopKGroups(g::UInt32, v, 1) FROM topk_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesSelectTopKGroups(g, v, -1::Int64) FROM topk_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesSelectTopKGroups(g, [1]::Array(UInt64), 1) FROM topk_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesSelectLimitKGroups(g, v, 1) FROM topk_input; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT timeSeriesSelectTopKGroups(1)(g, v, 1) FROM topk_input; -- { serverError AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS }
-- A corrupted serialized state claiming 2^56 - 1 time steps must fail once the actual payload runs out instead of attempting a huge allocation.
SELECT finalizeAggregation(CAST(unhex('0101FFFFFFFFFFFFFF7F'), 'AggregateFunction(timeSeriesSelectTopKGroups, UInt64, Array(Nullable(Float64)), UInt64)')); -- { serverError CANNOT_READ_ALL_DATA }
-- A corrupted serialized state claiming more entries at a time step than k must be rejected.
SELECT finalizeAggregation(CAST(unhex('010101010000000000000002'), 'AggregateFunction(timeSeriesSelectTopKGroups, UInt64, Array(Nullable(Float64)), UInt64)')); -- { serverError INCORRECT_DATA }

DROP TABLE topk_input;

SELECT '-- PromQL topk/bottomk/limitk end to end';

SET allow_experimental_time_series_table = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

-- 4 series of the metric `m`: hosts h1, h2 in dc=a and hosts h3, h4 in dc=b.
-- Series h4 has a gap at timestamps 110 and 120, and series h1 ends with a NaN sample at timestamp 140.
INSERT INTO prometheus (metric_name, tags, time_series) VALUES
    ('m', map('host', 'h1', 'dc', 'a'), [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), 10), (toDateTime64(120, 3), 4), (toDateTime64(130, 3), 1), (toDateTime64(140, 3), nan)]),
    ('m', map('host', 'h2', 'dc', 'a'), [(toDateTime64(100, 3), 2), (toDateTime64(110, 3), 20), (toDateTime64(120, 3), 3), (toDateTime64(130, 3), 2)]),
    ('m', map('host', 'h3', 'dc', 'b'), [(toDateTime64(100, 3), 3), (toDateTime64(110, 3), 5), (toDateTime64(120, 3), 2), (toDateTime64(130, 3), 3)]),
    ('m', map('host', 'h4', 'dc', 'b'), [(toDateTime64(100, 3), 4), (toDateTime64(130, 3), 4)]);

SELECT '-- topk(2), range';
SELECT * FROM prometheusQueryRange('prometheus', 'topk(2, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags;
SELECT '-- bottomk(2), range';
SELECT * FROM prometheusQueryRange('prometheus', 'bottomk(2, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags;
SELECT '-- topk(1) by (dc), range';
SELECT * FROM prometheusQueryRange('prometheus', 'topk(1, last_over_time(m[10])) by (dc)', 100, 130, 10) ORDER BY tags;
SELECT '-- bottomk(1) without (host), range';
SELECT * FROM prometheusQueryRange('prometheus', 'bottomk(1, last_over_time(m[10])) without (host)', 100, 130, 10) ORDER BY tags;
SELECT '-- limitk(2), range';
SELECT * FROM prometheusQueryRange('prometheus', 'limitk(2, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags;
SELECT '-- limitk(10): k > number of series, range';
SELECT * FROM prometheusQueryRange('prometheus', 'limitk(10, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags;
SELECT '-- topk(0) and clamped negative k, range';
SELECT * FROM prometheusQueryRange('prometheus', 'topk(0, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags;
SELECT * FROM prometheusQueryRange('prometheus', 'topk(-3, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags;
SELECT '-- topk(10): k > number of series, range';
SELECT * FROM prometheusQueryRange('prometheus', 'topk(10, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags;
SELECT '-- topk(time() / 10 - 9): k depending on the evaluation time, range';
SELECT * FROM prometheusQueryRange('prometheus', 'topk(time() / 10 - 9, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags;
SELECT '-- topk(2), instant';
SELECT * FROM prometheusQuery('prometheus', 'topk(2, m)', 130) ORDER BY tags;
SELECT '-- topk(scalar(count(m)) - 2): k from a scalar subquery, instant';
SELECT * FROM prometheusQuery('prometheus', 'topk(scalar(count(m)) - 2, m)', 130) ORDER BY tags;
SELECT '-- bottomk(2), instant';
SELECT * FROM prometheusQuery('prometheus', 'bottomk(2, m)', 130) ORDER BY tags;
SELECT '-- limitk(2), instant';
SELECT * FROM prometheusQuery('prometheus', 'limitk(2, m)', 130) ORDER BY tags;
SELECT '-- NaN is chosen after any non-NaN value, instant at 140 where h1 is NaN';
SELECT * FROM prometheusQuery('prometheus', 'topk(3, m)', 140) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'topk(4, m)', 140) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'bottomk(1, m)', 140) ORDER BY tags;
SELECT '-- topk of a metric which matches no series';
SELECT * FROM prometheusQuery('prometheus', 'topk(2, nonexistent)', 130) ORDER BY tags;
SELECT '-- k = +Inf is an error';
SELECT * FROM prometheusQuery('prometheus', 'topk(+Inf, m)', 130); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'topk(1 / 0, m)', 130); -- { serverError CANNOT_CONVERT_TYPE }

DROP TABLE prometheus;

SELECT '-- topk over many series runs in bounded memory';

-- Regression test: with N = 3000 series and T = 200 steps the old plan needed T * N^2 * 8 bytes = 14.4 GB (per-step arrayTopK lambda capture replication), far over the 2 GB limit used here, while the streaming plan keeps about T * k * 16 bytes = 32 KB of selection state.

CREATE TABLE prometheus ENGINE = TimeSeries;

INSERT INTO prometheus (metric_name, tags, time_series)
SELECT
    'big',
    map('inst', toString(number)),
    arrayMap(step -> (toDateTime64(100 + step * 10, 3), toFloat64(number + 1)), range(200))
FROM numbers(3000);

SELECT count(), sum(length(time_series))
FROM prometheusQueryRange('prometheus', 'topk(10, last_over_time(big[10]))', 100, 2090, 10)
SETTINGS max_memory_usage = 2000000000;

DROP TABLE prometheus;
