CREATE TABLE ts_raw_data(timestamp DateTime('UTC'), value Float64) ENGINE = MergeTree() ORDER BY timestamp;

INSERT INTO ts_raw_data VALUES
    (110, 1), (120, 1), (130, 3), (140, 4), (190, 5), (200, 5), (210, 8), (220, 12), (230, 13);

SELECT groupArraySorted(20)((toUnixTimestamp(timestamp), value)) FROM ts_raw_data;

SET allow_experimental_ts_to_grid_aggregate_function = 1;

-- Windows with 0, 1 (exactly 0.0, no NaN/negative-noise) or several samples.
WITH
    90 AS start, 210 AS end, 15 AS step, 45 AS window,
    range(start, end + 1, step) as grid
SELECT
    arrayZip(grid, timeSeriesStddevToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as stddev_45s,
    arrayZip(grid, timeSeriesStdvarToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as stdvar_45s
FROM ts_raw_data FORMAT Vertical;

-- Staleness window narrow enough that every grid point's window holds at most a single sample:
-- stddev/stdvar must be exactly 0 wherever a sample lands, NULL everywhere else.
WITH
    90 AS start, 230 AS end, 10 AS step, 5 AS window,
    range(start, end + 1, step) as grid
SELECT
    arrayZip(grid, timeSeriesStddevToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as stddev_5s,
    arrayZip(grid, timeSeriesStdvarToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as stdvar_5s
FROM ts_raw_data FORMAT Vertical;

-- AggregatingMergeTree table to test (de)serialization of the two new states
CREATE TABLE ts_data_agg(
    k UInt64,
    stddev_agg AggregateFunction(timeSeriesStddevToGrid(90, 210, 15, 45), DateTime('UTC'), Float64),
    stdvar_agg AggregateFunction(timeSeriesStdvarToGrid(90, 210, 15, 45), DateTime('UTC'), Float64)
) ENGINE AggregatingMergeTree() ORDER BY k;

-- Insert the data splitting it into several pieces, so the states must be merged
INSERT INTO ts_data_agg
SELECT
    toUnixTimestamp(timestamp) % 3,
    initializeAggregation('timeSeriesStddevToGridState(90, 210, 15, 45)', timestamp, value),
    initializeAggregation('timeSeriesStdvarToGridState(90, 210, 15, 45)', timestamp, value)
FROM ts_raw_data;

SELECT k, finalizeAggregation(stddev_agg), finalizeAggregation(stdvar_agg) FROM ts_data_agg FINAL ORDER BY k;

-- Reload the table and check that the data is the same (i.e. serialize-deserialize worked correctly)
DETACH TABLE ts_data_agg;
ATTACH TABLE ts_data_agg;
SELECT k, finalizeAggregation(stddev_agg), finalizeAggregation(stdvar_agg) FROM ts_data_agg FINAL ORDER BY k;

-- Check that -Merge returns the same result as computing directly on the original table
SELECT timeSeriesStddevToGrid(90, 210, 15, 45)(timestamp, value) FROM ts_raw_data;
SELECT timeSeriesStddevToGridMerge(90, 210, 15, 45)(stddev_agg) FROM ts_data_agg;

SELECT timeSeriesStdvarToGrid(90, 210, 15, 45)(timestamp, value) FROM ts_raw_data;
SELECT timeSeriesStdvarToGridMerge(90, 210, 15, 45)(stdvar_agg) FROM ts_data_agg;

DROP TABLE ts_data_agg;
DROP TABLE ts_raw_data;

-- Regression: two large-magnitude (~5.4e8) samples with a tiny (1-unit) spread must not collapse to
-- zero variance/stddev due to catastrophic cancellation in a naive `{count, sum, sum2}` accumulator -
-- `combined.sum2` and `combined.sum * combined.sum / count` would round to the same Float64 there.
-- Population variance/stddev for {540000000, 540000001} is exactly 0.25 / 0.5.
CREATE TABLE ts_large_magnitude(timestamp DateTime('UTC'), value Float64) ENGINE = MergeTree() ORDER BY timestamp;

INSERT INTO ts_large_magnitude VALUES (100, 540000000), (110, 540000001);

WITH
    100 AS start, 110 AS end, 10 AS step, 20 AS window,
    range(start, end + 1, step) as grid
SELECT
    arrayZip(grid, timeSeriesStddevToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as stddev_large,
    arrayZip(grid, timeSeriesStdvarToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as stdvar_large
FROM ts_large_magnitude FORMAT Vertical;

DROP TABLE ts_large_magnitude;

-- Regression: a NaN sample anywhere in the window must make stddev/stdvar propagate NaN, not silently
-- clamp to a valid-looking 0 (`std::max(0.0, NaN)` returns `0.0`, since any comparison against NaN is
-- false). The Prometheus storage path stores a genuine non-finite user sample raw and unfiltered, so
-- bad input like this must not be hidden as clean zero-variance data.
CREATE TABLE ts_non_finite(timestamp DateTime('UTC'), value Float64) ENGINE = MergeTree() ORDER BY timestamp;

INSERT INTO ts_non_finite VALUES (100, 1), (110, nan), (120, 3);

WITH
    100 AS start, 120 AS end, 10 AS step, 20 AS window,
    range(start, end + 1, step) as grid
SELECT
    arrayZip(grid, timeSeriesStddevToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as stddev_nan,
    arrayZip(grid, timeSeriesStdvarToGrid(start, end, step, window)(toUnixTimestamp(timestamp), value)) as stdvar_nan
FROM ts_non_finite FORMAT Vertical;

DROP TABLE ts_non_finite;

-- Regression: samples sharing a timestamp collapse into one keeping the largest real value, the rule the
-- whole timeSeries*ToGrid family follows (`timeseriesMaxValueForDuplicateTimestamp`). Accumulating the
-- Welford moments straight into the bucket would count each duplicate as a separate sample instead.
-- All queries below use a single-point grid at 100 with the window (90, 100].
SELECT 'Duplicate timestamps:';

-- Deduplicates to the single sample (95, 5), so the window has no spread at all.
SELECT timeSeriesStddevToGrid(100, 100, 1, 10)([95, 95]::Array(UInt32), [3, 5]::Array(Float64));
SELECT timeSeriesStdvarToGrid(100, 100, 1, 10)([95, 95]::Array(UInt32), [5, 3]::Array(Float64));

-- Deduplicates to (92, 1), (98, 5): population variance 4, standard deviation 2.
SELECT timeSeriesStddevToGrid(100, 100, 1, 10)([92, 98, 98]::Array(UInt32), [1, 3, 5]::Array(Float64));
SELECT timeSeriesStdvarToGrid(100, 100, 1, 10)([98, 92, 98]::Array(UInt32), [5, 1, 3]::Array(Float64));

SELECT 'NaN loses to a real value:';

SELECT timeSeriesStdvarToGrid(100, 100, 1, 10)([92, 98, 98]::Array(UInt32), [1, nan, 5]::Array(Float64));
SELECT timeSeriesStdvarToGrid(100, 100, 1, 10)([92, 98, 98]::Array(UInt32), [1, 5, nan]::Array(Float64));

SELECT 'NaN survives when all values at the timestamp are NaN:';

SELECT timeSeriesStdvarToGrid(100, 100, 1, 10)([92, 98, 98]::Array(UInt32), [1, nan, nan]::Array(Float64));

SELECT 'Partial states sharing a timestamp deduplicate on merge:';

SELECT timeSeriesStdvarToGridMerge(100, 100, 1, 10)(st)
FROM
(
    SELECT initializeAggregation('timeSeriesStdvarToGridState(100, 100, 1, 10)', ts_arr, val_arr) AS st
    FROM values('ts_arr Array(UInt32), val_arr Array(Float64)', ([95], [3.]), ([95], [5.]))
)
SETTINGS max_threads = 1;

SELECT 'Float32 input still yields a Float64 result:';

SELECT toTypeName(timeSeriesStddevToGrid(100, 100, 1, 10)([95, 95]::Array(UInt32), [3, 5]::Array(Float32)));
SELECT timeSeriesStdvarToGrid(100, 100, 1, 10)([92, 98, 98]::Array(UInt32), [1, 3, 5]::Array(Float32));
