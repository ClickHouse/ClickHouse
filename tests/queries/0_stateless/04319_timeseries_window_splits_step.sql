-- Tests the window-aligned sub-grid: when `window > step` and `window % step != 0`, each step cell is split
-- into two buckets at `T_g - (window % step)`. Exercises every timeSeries*ToGrid function across the
-- split / whole-multiple / window==step / window<step geometry regimes on small grids (recompute path).
SET allow_experimental_time_series_aggregate_functions = 1;
SET allow_experimental_ts_to_grid_aggregate_function = 1;

DROP TABLE IF EXISTS ts_split;
CREATE TABLE ts_split (timestamp DateTime, value Float64) ENGINE = MergeTree ORDER BY timestamp;
-- A counter-like series with two decreases (resets) at 25 and 55.
INSERT INTO ts_split VALUES
    (5, 10), (15, 12), (25, 11), (35, 20), (45, 25), (55, 5), (65, 8), (75, 9);

-- window=25, step=10: window % step = 5 (!= 0) and window > step -> each step cell is split into two buckets.
SELECT 'window splits step (window=25, step=10):';
SELECT timeSeriesResampleToGridWithStaleness(0, 80, 10, 25)(timestamp, value) FROM ts_split;
SELECT timeSeriesChangesToGrid(0, 80, 10, 25)(timestamp, value) FROM ts_split;
SELECT timeSeriesResetsToGrid(0, 80, 10, 25)(timestamp, value) FROM ts_split;
SELECT timeSeriesRateToGrid(0, 80, 10, 25)(timestamp, value) FROM ts_split;
SELECT timeSeriesDeltaToGrid(0, 80, 10, 25)(timestamp, value) FROM ts_split;
SELECT timeSeriesInstantRateToGrid(0, 80, 10, 25)(timestamp, value) FROM ts_split;
SELECT timeSeriesInstantDeltaToGrid(0, 80, 10, 25)(timestamp, value) FROM ts_split;
SELECT timeSeriesDerivToGrid(0, 80, 10, 25)(timestamp, value) FROM ts_split;
SELECT timeSeriesPredictLinearToGrid(0, 80, 10, 25, 30)(timestamp, value) FROM ts_split;

-- window=31, step=10: window % step = 1, a split with a small after-split sub-cell.
SELECT 'window splits step (window=31, step=10):';
SELECT timeSeriesChangesToGrid(0, 80, 10, 31)(timestamp, value) FROM ts_split;
SELECT timeSeriesResetsToGrid(0, 80, 10, 31)(timestamp, value) FROM ts_split;
SELECT timeSeriesDeltaToGrid(0, 80, 10, 31)(timestamp, value) FROM ts_split;
SELECT timeSeriesResampleToGridWithStaleness(0, 80, 10, 31)(timestamp, value) FROM ts_split;

-- Contrast regimes that do NOT split a step:
SELECT 'window multiple of step (window=20, step=10):';
SELECT timeSeriesChangesToGrid(0, 80, 10, 20)(timestamp, value) FROM ts_split;
SELECT timeSeriesDeltaToGrid(0, 80, 10, 20)(timestamp, value) FROM ts_split;

SELECT 'window == step (window=10, step=10):';
SELECT timeSeriesChangesToGrid(0, 80, 10, 10)(timestamp, value) FROM ts_split;
SELECT timeSeriesResampleToGridWithStaleness(0, 80, 10, 10)(timestamp, value) FROM ts_split;

SELECT 'window < step (window=7, step=10):';
SELECT timeSeriesChangesToGrid(0, 80, 10, 7)(timestamp, value) FROM ts_split;
SELECT timeSeriesResampleToGridWithStaleness(0, 80, 10, 7)(timestamp, value) FROM ts_split;

DROP TABLE ts_split;
