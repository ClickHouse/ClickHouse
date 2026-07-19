-- Drives the sliding two-stack aggregation path. Only the linear-regression functions (`timeSeriesDerivToGrid`,
-- `timeSeriesPredictLinearToGrid`) ever use two-stacks; they switch to it once a window holds enough populated
-- buckets (`AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS`) or can hold at least `BPW_TO_FORCE_TWO_STACKS`. The other
-- timeSeries*ToGrid functions always recompute. Both scenarios below span 50 and 51 buckets per window — above
-- `BPW_TO_FORCE_TWO_STACKS` — so the two regression functions run on two-stacks while the rest recompute, and the
-- window slides across the data so buckets both enter and leave.
-- Covers a whole-multiple window (`window % step == 0`) and a window that splits each step (`window % step != 0`).
SET allow_experimental_time_series_aggregate_functions = 1;
SET allow_experimental_ts_to_grid_aggregate_function = 1;

DROP TABLE IF EXISTS ts_two_stacks;
CREATE TABLE ts_two_stacks (timestamp DateTime, value Float64) ENGINE = MergeTree ORDER BY timestamp;
-- Dense-ish series spanning the windows' reach back to 60 (T_0 - window); includes resets at 88 and 108.
INSERT INTO ts_two_stacks VALUES
    (60, 1), (65, 3), (72, 6), (80, 10), (88, 9), (95, 14), (101, 20), (108, 5), (114, 8), (120, 13);

-- step=1 over [100,120] -> 21 grid points. window=50 -> 50 buckets/window (>= threshold -> two-stacks); window % step == 0.
SELECT 'two-stacks, window multiple of step (window=50, step=1):';
SELECT timeSeriesResampleToGridWithStaleness(100, 120, 1, 50)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesChangesToGrid(100, 120, 1, 50)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesResetsToGrid(100, 120, 1, 50)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesRateToGrid(100, 120, 1, 50)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesDeltaToGrid(100, 120, 1, 50)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesInstantRateToGrid(100, 120, 1, 50)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesInstantDeltaToGrid(100, 120, 1, 50)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesDerivToGrid(100, 120, 1, 50)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesPredictLinearToGrid(100, 120, 1, 50, 10)(timestamp, value) FROM ts_two_stacks;

-- step=2 over [100,120] -> 11 grid points. window=51 -> 51 buckets/window (>= threshold -> two-stacks); window % step == 1, so each step is split.
SELECT 'two-stacks, window splits step (window=51, step=2):';
SELECT timeSeriesResampleToGridWithStaleness(100, 120, 2, 51)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesChangesToGrid(100, 120, 2, 51)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesResetsToGrid(100, 120, 2, 51)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesRateToGrid(100, 120, 2, 51)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesDeltaToGrid(100, 120, 2, 51)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesInstantRateToGrid(100, 120, 2, 51)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesInstantDeltaToGrid(100, 120, 2, 51)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesDerivToGrid(100, 120, 2, 51)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesPredictLinearToGrid(100, 120, 2, 51, 10)(timestamp, value) FROM ts_two_stacks;

-- Cross-check the two-stack eviction logic: the sliding value at a grid point whose window has evicted buckets
-- that entered at an earlier grid point must equal a single-grid-point aggregate over the same window, which
-- builds the window from scratch with no eviction. Compared with a tolerance because the two summation orders of
-- the regression moments differ by ULPs. (Grid point T = 100 + k is at SQL array index k + 1.)
SELECT 'sliding two-stack values match a fresh single-grid-point aggregate (all 1):';
SELECT abs(timeSeriesDerivToGrid(100, 120, 1, 50)(timestamp, value)[11]
         - timeSeriesDerivToGrid(110, 110, 1, 50)(timestamp, value)[1]) < 1e-9 FROM ts_two_stacks;
SELECT abs(timeSeriesDerivToGrid(100, 120, 1, 50)(timestamp, value)[21]
         - timeSeriesDerivToGrid(120, 120, 1, 50)(timestamp, value)[1]) < 1e-9 FROM ts_two_stacks;
SELECT abs(timeSeriesPredictLinearToGrid(100, 120, 1, 50, 10)(timestamp, value)[21]
         - timeSeriesPredictLinearToGrid(120, 120, 1, 50, 10)(timestamp, value)[1]) < 1e-9 FROM ts_two_stacks;

-- Serialization round-trip over the new step-split bucket layout (window=51, step=2 -> window % step != 0):
-- merging two partial -State aggregates (built over disjoint row subsets) must reproduce the direct aggregate
-- exactly for the integer-valued functions, exercising (de)serialization of the split buckets.
SELECT 'merge of split partial states matches the direct aggregate (all 1):';
SELECT timeSeriesChangesToGrid(100, 120, 2, 51)(timestamp, value)
     = (SELECT timeSeriesChangesToGridMerge(100, 120, 2, 51)(s)
        FROM (SELECT timeSeriesChangesToGridState(100, 120, 2, 51)(timestamp, value) AS s
              FROM ts_two_stacks GROUP BY toUnixTimestamp(timestamp) % 2)) FROM ts_two_stacks;
SELECT timeSeriesResetsToGrid(100, 120, 2, 51)(timestamp, value)
     = (SELECT timeSeriesResetsToGridMerge(100, 120, 2, 51)(s)
        FROM (SELECT timeSeriesResetsToGridState(100, 120, 2, 51)(timestamp, value) AS s
              FROM ts_two_stacks GROUP BY toUnixTimestamp(timestamp) % 2)) FROM ts_two_stacks;

DROP TABLE ts_two_stacks;

-- Two-stacks selected via the AVERAGE-density path, not the hard cap: step=1, window=15 -> buckets_per_window=15
-- (below BPW_TO_FORCE_TWO_STACKS=20), but a sample in every bucket makes the average populated buckets per window
-- (~15) >= AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS=10, so the regression functions pick two-stacks through the
-- average condition. The full density (populated / bucket_count = 1.0 >= BUCKET_DENSITY_TO_ENABLE_RANGE_SCAN=0.4)
-- also drives the range-scan bucket iteration. Quadratic values make the per-window slope vary, so a faulty moment
-- merge would diverge from the fresh recompute.
DROP TABLE IF EXISTS ts_dense;
CREATE TABLE ts_dense (timestamp DateTime, value Float64) ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO ts_dense SELECT 186 + number, number * number FROM numbers(35);  -- one sample per bucket of [186, 220]

-- Eviction on the average-path two-stacks must still match a fresh single-grid-point aggregate over the same window.
SELECT 'dense average-path two-stack values match a fresh single-grid-point aggregate (all 1):';
SELECT abs(timeSeriesDerivToGrid(200, 220, 1, 15)(timestamp, value)[21]
         - timeSeriesDerivToGrid(220, 220, 1, 15)(timestamp, value)[1]) < 1e-9 FROM ts_dense;
SELECT abs(timeSeriesPredictLinearToGrid(200, 220, 1, 15, 10)(timestamp, value)[21]
         - timeSeriesPredictLinearToGrid(220, 220, 1, 15, 10)(timestamp, value)[1]) < 1e-9 FROM ts_dense;

DROP TABLE ts_dense;
