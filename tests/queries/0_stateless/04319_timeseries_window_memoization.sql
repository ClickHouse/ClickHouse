-- Tests the memoization in the recompute path (`AggregateFunctionTimeseriesSlidingSum::getCurrentSum`, driven
-- from `AggregateFunctionTimeseriesBase::doInsertResultInto`): the merged window aggregate is reused across
-- consecutive grid points that cover the same set of populated buckets. Here `buckets_per_window` = 10, below
-- `BPW_TO_FORCE_TWO_STACKS` (20), so even `timeSeriesDerivToGrid` (the only function here that could use
-- two-stacks) stays on recompute; the other functions always recompute. A window (1000) much wider than the data
-- extent (300..700) makes a run of grid points share the identical full bucket set (memo hits), while the edges
-- (buckets entering/leaving) force recomputes.
SET allow_experimental_time_series_aggregate_functions = 1;
SET allow_experimental_ts_to_grid_aggregate_function = 1;

DROP TABLE IF EXISTS ts_memo;
CREATE TABLE ts_memo (timestamp DateTime, value Float64) ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO ts_memo VALUES
    (300, 1), (350, 2), (400, 3), (450, 2), (500, 5), (550, 5), (600, 8), (650, 7), (700, 9);

SELECT 'window=1000, step=100 (buckets_per_window=10 -> recompute + memoization):';
SELECT timeSeriesChangesToGrid(0, 2000, 100, 1000)(timestamp, value) FROM ts_memo;
SELECT timeSeriesResetsToGrid(0, 2000, 100, 1000)(timestamp, value) FROM ts_memo;
SELECT timeSeriesResampleToGridWithStaleness(0, 2000, 100, 1000)(timestamp, value) FROM ts_memo;
SELECT timeSeriesRateToGrid(0, 2000, 100, 1000)(timestamp, value) FROM ts_memo;
SELECT timeSeriesDerivToGrid(0, 2000, 100, 1000)(timestamp, value) FROM ts_memo;

-- Cross-check (each must be 1): in the run of grid points whose window covers all 9 samples, the memoized
-- value must equal a single-grid-point aggregate over the whole data, which does not use memoization.
-- Grid point index k (0-based) is at T = k*100, i.e. SQL array index k+1.
SELECT 'memoized values match a single full-window grid point (all 1):';
SELECT timeSeriesChangesToGrid(0, 2000, 100, 1000)(timestamp, value)[8]
     = timeSeriesChangesToGrid(700, 700, 100, 1000)(timestamp, value)[1] FROM ts_memo;
SELECT timeSeriesResetsToGrid(0, 2000, 100, 1000)(timestamp, value)[10]
     = timeSeriesResetsToGrid(900, 900, 100, 1000)(timestamp, value)[1] FROM ts_memo;
SELECT timeSeriesResampleToGridWithStaleness(0, 2000, 100, 1000)(timestamp, value)[12]
     = timeSeriesResampleToGridWithStaleness(1100, 1100, 100, 1000)(timestamp, value)[1] FROM ts_memo;
SELECT timeSeriesRateToGrid(0, 2000, 100, 1000)(timestamp, value)[9]
     = timeSeriesRateToGrid(800, 800, 100, 1000)(timestamp, value)[1] FROM ts_memo;

DROP TABLE ts_memo;
