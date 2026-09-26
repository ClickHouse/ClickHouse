-- Tests timeSeriesMadToGrid, the aggregate function behind the PromQL function `mad_over_time`: the median absolute
-- deviation `median(|x - median(x)|)` of the samples in the window. Both medians are R-7 (inclusive) quantiles taken the
-- way timeSeriesQuantileToGrid takes them. Like in Prometheus, a NaN sample in the window makes the result NaN.

DROP TABLE IF EXISTS mad_input;

-- The function is in private preview and disabled by default.
SET enable_time_series_aggregate_functions = 0;
SET enable_time_series_table = 0;
SELECT timeSeriesMadToGrid(100, 120, 10, 30)(toDateTime(100), 10::Float64); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

SET enable_time_series_aggregate_functions = 1;

-- Samples 1, 2, 3, 4, 100 at 100..140 on the grid [100, 110, 120, 130, 140] with a 50 second window, so the windows
-- hold {1}, {1, 2}, {1, 2, 3}, {1, 2, 3, 4} and {1, 2, 3, 4, 100}. `grp` splits the samples into two partial states.
CREATE TABLE mad_input (grp UInt8, timestamp DateTime, value Float64) ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO mad_input VALUES (0, 100, 1), (0, 110, 2), (1, 120, 3), (0, 130, 4), (1, 140, 100);

SELECT '-- an outlier barely changes the result: {1, 2, 3, 4} and {1, 2, 3, 4, 100} both give 1';
SELECT timeSeriesMadToGrid(100, 140, 10, 50)(timestamp, value) FROM mad_input;

SELECT '-- the samples can be passed as arrays or as an array of (timestamp, value) tuples, Float32 values are accepted';
WITH [100, 110, 120, 130, 140]::Array(DateTime) AS timestamps, [1, 2, 3, 4, 100]::Array(Float64) AS values
SELECT timeSeriesMadToGrid(100, 140, 10, 50)(timestamps, values);
SELECT timeSeriesMadToGrid(100, 140, 10, 50)(samples) FROM (SELECT groupArray((timestamp, value)) AS samples FROM mad_input);
WITH [100, 110, 120, 130, 140]::Array(DateTime) AS timestamps, [1, 2, 3, 4, 100]::Array(Float32) AS values
SELECT timeSeriesMadToGrid(100, 140, 10, 50)(timestamps, values) AS result, toTypeName(result);

SELECT '-- rows excluded by a condition or hidden by a NULL are ignored: without the sample 3 the windows are {1}, {1, 2}, {1, 2}, {1, 2, 4}, {1, 2, 4, 100}';
SELECT timeSeriesMadToGridIf(100, 140, 10, 50)(timestamp, value, value != 3) FROM mad_input;
SELECT timeSeriesMadToGrid(100, 140, 10, 50)(timestamp, if(value = 3, NULL, value)) FROM mad_input;

SELECT '-- partial states merge into the same result, also after a round trip through a table';
SELECT timeSeriesMadToGridMerge(100, 140, 10, 50)(st) FROM (SELECT timeSeriesMadToGridState(100, 140, 10, 50)(timestamp, value) AS st FROM mad_input GROUP BY grp);
DROP TABLE IF EXISTS mad_states;
CREATE TABLE mad_states (grp UInt8, st AggregateFunction(timeSeriesMadToGrid(100, 140, 10, 50), DateTime, Float64)) ENGINE = AggregatingMergeTree ORDER BY grp;
INSERT INTO mad_states SELECT grp, initializeAggregation('timeSeriesMadToGridState(100, 140, 10, 50)', timestamp, value) FROM mad_input;
DETACH TABLE mad_states;
ATTACH TABLE mad_states;
SELECT timeSeriesMadToGridMerge(100, 140, 10, 50)(st) FROM mad_states;
DROP TABLE mad_states;

DROP TABLE mad_input;

SELECT '-- sliding window: samples leave the window as the grid advances';
-- Samples 10, 0, 4, 1, 100, 6, 2, 3 at 100..170 with a 35 second window, so each window holds the last four samples.
WITH [100, 110, 120, 130, 140, 150, 160, 170]::Array(DateTime) AS timestamps, [10, 0, 4, 1, 100, 6, 2, 3]::Array(Float64) AS values
SELECT timeSeriesMadToGrid(100, 170, 10, 35)(timestamps, values);

SELECT '-- a window of 64 buckets: the values 1..110 equal the timestamps, so every window holds 64 consecutive integers';
SELECT timeSeriesMadToGrid(100, 110, 1, 64)(toUInt32(number + 1), toFloat64(number + 1)) FROM numbers(110);

SELECT '-- a step larger than the window: some samples are in no window';
WITH [95, 100, 112, 116, 125]::Array(DateTime) AS timestamps, [1, 2, 3, 4, 5]::Array(Float64) AS values
SELECT timeSeriesMadToGrid(100, 130, 15, 10)(timestamps, values);

SELECT '-- a window without samples stays NULL';
WITH [100]::Array(DateTime) AS timestamps, [1]::Array(Float64) AS values
SELECT timeSeriesMadToGrid(100, 200, 100, 30)(timestamps, values);

SELECT '-- samples with the same timestamp are collapsed into the greatest value, a NaN loses to any real value';
-- The samples at 100 collapse into 5 and the samples at 110 into 2, so the windows hold {5}, {5, 2} and {5, 2, 3}.
WITH [100, 100, 110, 110, 120]::Array(DateTime) AS timestamps, [1, 5, 2, nan, 3]::Array(Float64) AS values
SELECT timeSeriesMadToGrid(100, 120, 10, 30)(timestamps, values);

-- NaN samples follow Prometheus: a NaN sample is not dropped, it makes the result of every window containing it NaN.

SELECT '-- a NaN sample anywhere in the window gives NaN: [1, NaN, 3], [1, NaN, 3, 4] and [NaN, 1, 2, 3]';
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [1, nan, 3]::Array(Float64) AS values
SELECT timeSeriesMadToGrid(120, 120, 10, 30)(timestamps, values);
WITH [100, 110, 120, 130]::Array(DateTime) AS timestamps, [1, nan, 3, 4]::Array(Float64) AS values
SELECT timeSeriesMadToGrid(130, 130, 10, 40)(timestamps, values);
WITH [100, 110, 120, 130]::Array(DateTime) AS timestamps, [nan, 1, 2, 3]::Array(Float64) AS values
SELECT timeSeriesMadToGrid(130, 130, 10, 40)(timestamps, values);

SELECT '-- the windows without the NaN sample are not affected: [NaN], [NaN, 1], [1, 2] and [2] give NaN, NaN, 0.5 and 0';
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [nan, 1, 2]::Array(Float64) AS values
SELECT timeSeriesMadToGrid(100, 130, 10, 20)(timestamps, values);

SELECT '-- infinite samples with a finite median have infinite deviations, which sort last';
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [1, 2, inf]::Array(Float64) AS values
SELECT timeSeriesMadToGrid(120, 120, 10, 30)(timestamps, values);
WITH [100, 110, 120, 130]::Array(DateTime) AS timestamps, [1, 2, 3, inf]::Array(Float64) AS values
SELECT timeSeriesMadToGrid(130, 130, 10, 40)(timestamps, values);
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [-inf, 1, 2]::Array(Float64) AS values
SELECT timeSeriesMadToGrid(120, 120, 10, 30)(timestamps, values);
WITH [100, 110, 120, 130, 140, 150]::Array(DateTime) AS timestamps, [1, 2, 3, 4, inf, inf]::Array(Float64) AS values
SELECT timeSeriesMadToGrid(150, 150, 10, 60)(timestamps, values);

SELECT '-- the median of the deviations interpolated between a finite and an infinite deviation is +Inf, like in Prometheus';
WITH [100, 110, 120, 130]::Array(DateTime) AS timestamps, [-inf, 0, 1, inf]::Array(Float64) AS values
SELECT timeSeriesMadToGrid(130, 130, 10, 40)(timestamps, values);

SELECT '-- an infinite median gives NaN: the deviations of the samples equal to it are Inf - Inf';
WITH [100, 110, 120, 130]::Array(DateTime) AS timestamps, [1, 2, inf, inf]::Array(Float64) AS values
SELECT timeSeriesMadToGrid(130, 130, 10, 40)(timestamps, values);
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [1, inf, inf]::Array(Float64) AS values
SELECT timeSeriesMadToGrid(120, 120, 10, 30)(timestamps, values);
WITH [100]::Array(DateTime) AS timestamps, [inf]::Array(Float64) AS values
SELECT timeSeriesMadToGrid(100, 100, 10, 20)(timestamps, values);

SELECT '-- a median interpolated between -Inf and +Inf is NaN, and so is the result';
WITH [100, 110]::Array(DateTime) AS timestamps, [-inf, inf]::Array(Float64) AS values
SELECT timeSeriesMadToGrid(110, 110, 10, 20)(timestamps, values);

SELECT '-- invalid arguments';
SELECT timeSeriesMadToGrid(100, 120, 10)([100]::Array(DateTime), [1]::Array(Float64)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT timeSeriesMadToGrid(100, 120, 10, 30)([100]::Array(DateTime), [1]::Array(Float64), 0.5); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
