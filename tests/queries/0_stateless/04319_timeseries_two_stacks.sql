-- Drives the sliding two-stack aggregation path. The linear-regression functions (`timeSeriesDerivToGrid`,
-- `timeSeriesPredictLinearToGrid`) and the extremum functions (`timeSeriesMaxToGrid`, `timeSeriesMinToGrid`)
-- use two-stacks; they switch to it once a window holds enough populated buckets
-- (`AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS`) or can hold at least `BPW_TO_FORCE_TWO_STACKS`. The other
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
SELECT timeSeriesMaxToGrid(100, 120, 1, 50)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesMinToGrid(100, 120, 1, 50)(timestamp, value) FROM ts_two_stacks;

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
SELECT timeSeriesMaxToGrid(100, 120, 2, 51)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesMinToGrid(100, 120, 2, 51)(timestamp, value) FROM ts_two_stacks;

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
-- The extremum summary keeps exact values, so sliding two-stack results must equal the fresh aggregate exactly.
SELECT timeSeriesMaxToGrid(100, 120, 1, 50)(timestamp, value)[11]
     = timeSeriesMaxToGrid(110, 110, 1, 50)(timestamp, value)[1] FROM ts_two_stacks;
SELECT timeSeriesMaxToGrid(100, 120, 1, 50)(timestamp, value)[21]
     = timeSeriesMaxToGrid(120, 120, 1, 50)(timestamp, value)[1] FROM ts_two_stacks;
SELECT timeSeriesMinToGrid(100, 120, 1, 50)(timestamp, value)[21]
     = timeSeriesMinToGrid(120, 120, 1, 50)(timestamp, value)[1] FROM ts_two_stacks;

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
SELECT timeSeriesMaxToGrid(100, 120, 2, 51)(timestamp, value)
     = (SELECT timeSeriesMaxToGridMerge(100, 120, 2, 51)(s)
        FROM (SELECT timeSeriesMaxToGridState(100, 120, 2, 51)(timestamp, value) AS s
              FROM ts_two_stacks GROUP BY toUnixTimestamp(timestamp) % 2)) FROM ts_two_stacks;
SELECT timeSeriesMinToGrid(100, 120, 2, 51)(timestamp, value)
     = (SELECT timeSeriesMinToGridMerge(100, 120, 2, 51)(s)
        FROM (SELECT timeSeriesMinToGridState(100, 120, 2, 51)(timestamp, value) AS s
              FROM ts_two_stacks GROUP BY toUnixTimestamp(timestamp) % 2)) FROM ts_two_stacks;

DROP TABLE ts_two_stacks;

-- Two-stacks selected via the AVERAGE-density path, not the hard cap: step=1, window=15 -> buckets_per_window=15
-- (below BPW_TO_FORCE_TWO_STACKS=20), but a sample in every bucket makes the average populated buckets per window
-- (~15) >= AVG_POPULATED_BPW_TO_ENABLE_TWO_STACKS=10, so the regression functions pick two-stacks through the
-- average condition. The full density (populated / bucket_count = 1.0 >= BUCKET_DENSITY_TO_ENABLE_RANGE_SCAN=0.35)
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
SELECT timeSeriesMaxToGrid(200, 220, 1, 15)(timestamp, value)[21]
     = timeSeriesMaxToGrid(220, 220, 1, 15)(timestamp, value)[1] FROM ts_dense;
SELECT timeSeriesMinToGrid(200, 220, 1, 15)(timestamp, value)[21]
     = timeSeriesMinToGrid(220, 220, 1, 15)(timestamp, value)[1] FROM ts_dense;

DROP TABLE ts_dense;

-- IEEE-equal extrema (-0.0 vs +0.0) must resolve order-independently: the earliest sample wins on
-- the recompute path, the two-stack path, and through split -State merges alike.
SELECT 'signed-zero ties keep the earliest sample (1/max: -inf, -inf, -inf, inf):';
DROP TABLE IF EXISTS ts_zero;
CREATE TABLE ts_zero (timestamp DateTime, value Float64) ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO ts_zero VALUES (100, -0.), (101, 0.);
SELECT 1 / (timeSeriesMaxToGrid(101, 101, 1, 5)(timestamp, value))[1] FROM ts_zero;
SELECT 1 / (timeSeriesMaxToGrid(101, 121, 1, 50)(timestamp, value))[1] FROM ts_zero;
SELECT 1 / (timeSeriesMaxToGridMerge(101, 121, 1, 50)(s))[1]
  FROM (SELECT timeSeriesMaxToGridState(101, 121, 1, 50)(timestamp, value) AS s
        FROM ts_zero GROUP BY toUnixTimestamp(timestamp) % 2);
DROP TABLE ts_zero;
DROP TABLE IF EXISTS ts_zero2;
CREATE TABLE ts_zero2 (timestamp DateTime, value Float64) ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO ts_zero2 VALUES (100, 0.), (101, -0.);
SELECT 1 / (timeSeriesMaxToGrid(101, 121, 1, 50)(timestamp, value))[1] FROM ts_zero2;
DROP TABLE ts_zero2;

-- An all-NaN window must keep the latest sample's NaN payload on every path, observable via
-- reinterpretAsUInt64 (recompute, two-stacks, split -State merges, and min alike).
SELECT 'all-NaN windows keep the latest payload (FFF800000000000A x4):';
DROP TABLE IF EXISTS ts_nan;
CREATE TABLE ts_nan (timestamp DateTime, value Float64) ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO ts_nan VALUES (100, reinterpret(0x7FF8000000000001, 'Float64')), (101, reinterpret(0xFFF800000000000A, 'Float64'));
SELECT hex(reinterpretAsUInt64((timeSeriesMaxToGrid(101, 101, 1, 5)(timestamp, value))[1])) FROM ts_nan;
SELECT hex(reinterpretAsUInt64((timeSeriesMaxToGrid(101, 121, 1, 50)(timestamp, value))[1])) FROM ts_nan;
SELECT hex(reinterpretAsUInt64((timeSeriesMaxToGridMerge(101, 121, 1, 50)(s))[1]))
  FROM (SELECT timeSeriesMaxToGridState(101, 121, 1, 50)(timestamp, value) AS s
        FROM ts_nan GROUP BY toUnixTimestamp(timestamp) % 2);
SELECT hex(reinterpretAsUInt64((timeSeriesMinToGrid(101, 121, 1, 50)(timestamp, value))[1])) FROM ts_nan;
DROP TABLE ts_nan;

-- Duplicate timestamps leave neither `==` nor the timestamp able to separate the samples, so the
-- winner must come from a canonical raw-bit tie-break, not from the order the states were merged in.
SELECT 'same-timestamp ties are order-independent (FFF800000000000A x3, then -inf, -inf):';
DROP TABLE IF EXISTS ts_tie;
CREATE TABLE ts_tie (id UInt8, timestamp DateTime, value Float64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ts_tie VALUES (1, 100, reinterpret(0x7FF8000000000001, 'Float64')), (2, 100, reinterpret(0xFFF800000000000A, 'Float64'));
SELECT hex(reinterpretAsUInt64((timeSeriesMaxToGrid(101, 121, 1, 50)(timestamp, value))[1])) FROM ts_tie;
SELECT hex(reinterpretAsUInt64((timeSeriesMaxToGridMerge(101, 121, 1, 50)(s))[1]))
  FROM (SELECT timeSeriesMaxToGridState(101, 121, 1, 50)(timestamp, value) AS s
        FROM ts_tie GROUP BY id);
SELECT hex(reinterpretAsUInt64((timeSeriesMinToGrid(101, 121, 1, 50)(timestamp, value))[1])) FROM ts_tie;
DROP TABLE ts_tie;
DROP TABLE IF EXISTS ts_zero_tie;
CREATE TABLE ts_zero_tie (id UInt8, timestamp DateTime, value Float64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ts_zero_tie VALUES (1, 100, 0.), (2, 100, -0.);
SELECT 1 / (timeSeriesMaxToGrid(101, 121, 1, 50)(timestamp, value))[1] FROM ts_zero_tie;
SELECT 1 / (timeSeriesMaxToGridMerge(101, 121, 1, 50)(s))[1]
  FROM (SELECT timeSeriesMaxToGridState(101, 121, 1, 50)(timestamp, value) AS s
        FROM ts_zero_tie GROUP BY id);
DROP TABLE ts_zero_tie;
