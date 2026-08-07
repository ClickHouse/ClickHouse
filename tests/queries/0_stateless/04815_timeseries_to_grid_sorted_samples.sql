-- Tags: shard

-- Tests the sorted-append per-bucket sample buffer of the `timeSeries*ToGrid` aggregate functions: out-of-order and duplicate timestamps must produce the same results as sorted unique input on every path (plain adds, in-memory merges of partial states, serialized-state merges).

SET allow_experimental_time_series_aggregate_functions = 1;

-- The array-argument form feeds samples in exact array order, so a shuffled array deterministically exercises the out-of-order add path; the duplicated timestamps (125, 140, 170) must keep the larger of their two values to reproduce the sorted unique baseline.
SELECT 'sorted unique baseline (rate, increase, delta, changes, resets, deriv, predict, resample):';
WITH
    arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 125, 140, 155, 170, 185, 200]) AS ts,
    [1., 3, 2, 5, 4, 6, 8] AS vals
SELECT
    timeSeriesRateToGrid(100, 200, 20, 100)(ts, vals) AS rate,
    timeSeriesIncreaseToGrid(100, 200, 20, 100)(ts, vals) AS increase,
    timeSeriesDeltaToGrid(100, 200, 20, 100)(ts, vals) AS delta,
    timeSeriesChangesToGrid(100, 200, 20, 100)(ts, vals) AS changes,
    timeSeriesResetsToGrid(100, 200, 20, 100)(ts, vals) AS resets,
    timeSeriesDerivToGrid(100, 200, 20, 100)(ts, vals) AS deriv,
    timeSeriesPredictLinearToGrid(100, 200, 20, 100, 60)(ts, vals) AS predict,
    timeSeriesResampleToGridWithStaleness(100, 200, 20, 100)(ts, vals) AS resample
FORMAT Vertical;

SELECT 'shuffled input with duplicates equals the baseline (all 1):';
WITH
    arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 125, 140, 155, 170, 185, 200]) AS ts,
    [1., 3, 2, 5, 4, 6, 8] AS vals,
    arrayMap(x -> toDateTime64(x, 3, 'UTC'), [155, 110, 200, 125, 140, 185, 170, 170, 140, 125]) AS shuffled_ts,
    [5., 1, 8, 0.5, 0, 6, 1, 4, 2, 3] AS shuffled_vals
SELECT
    timeSeriesRateToGrid(100, 200, 20, 100)(shuffled_ts, shuffled_vals) = timeSeriesRateToGrid(100, 200, 20, 100)(ts, vals),
    timeSeriesIncreaseToGrid(100, 200, 20, 100)(shuffled_ts, shuffled_vals) = timeSeriesIncreaseToGrid(100, 200, 20, 100)(ts, vals),
    timeSeriesDeltaToGrid(100, 200, 20, 100)(shuffled_ts, shuffled_vals) = timeSeriesDeltaToGrid(100, 200, 20, 100)(ts, vals),
    timeSeriesChangesToGrid(100, 200, 20, 100)(shuffled_ts, shuffled_vals) = timeSeriesChangesToGrid(100, 200, 20, 100)(ts, vals),
    timeSeriesResetsToGrid(100, 200, 20, 100)(shuffled_ts, shuffled_vals) = timeSeriesResetsToGrid(100, 200, 20, 100)(ts, vals),
    timeSeriesDerivToGrid(100, 200, 20, 100)(shuffled_ts, shuffled_vals) = timeSeriesDerivToGrid(100, 200, 20, 100)(ts, vals),
    timeSeriesPredictLinearToGrid(100, 200, 20, 100, 60)(shuffled_ts, shuffled_vals) = timeSeriesPredictLinearToGrid(100, 200, 20, 100, 60)(ts, vals),
    timeSeriesResampleToGridWithStaleness(100, 200, 20, 100)(shuffled_ts, shuffled_vals) = timeSeriesResampleToGridWithStaleness(100, 200, 20, 100)(ts, vals);

-- Duplicate timestamps keep the larger value regardless of arrival order: after deduplication the sequence is (110, 5), (120, 5), so changes must be [0].
SELECT 'duplicate timestamps keep the larger value (changes = [0] twice):';
SELECT timeSeriesChangesToGrid(120, 120, 1, 60)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 110, 120]), [5., 2, 5]);
SELECT timeSeriesChangesToGrid(120, 120, 1, 60)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 110, 120]), [2., 5, 5]);

-- NaN samples (Prometheus stale markers) at a duplicated timestamp: the in-order add path resolves the duplicate as max(stored, incoming), which keeps the first arrival against NaN, same as the previous implementation.
SELECT 'NaN at a duplicate timestamp keeps the first arrival on the in-order path (delta [nan] [0], changes [1] [0]):';
SELECT timeSeriesDeltaToGrid(120, 120, 1, 60)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 110, 120]), [nan, 5., 5.]);
SELECT timeSeriesDeltaToGrid(120, 120, 1, 60)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 110, 120]), [5., nan, 5.]);
SELECT timeSeriesChangesToGrid(120, 120, 1, 60)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 110, 120]), [nan, 5., 5.]);
SELECT timeSeriesChangesToGrid(120, 120, 1, 60)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 110, 120]), [5., nan, 5.]);

-- The same NaN duplicates arriving out of order (the duplicate is no longer the last sample, so the bucket goes through lazy normalization): the stable sort keeps the equal-timestamp run in arrival order, so the first arrival must still win against NaN.
SELECT 'NaN at a duplicate timestamp keeps the first arrival on the out-of-order path (delta [nan] [0], changes [1] [0]):';
SELECT timeSeriesDeltaToGrid(120, 120, 1, 60)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 120, 110]), [nan, 5., 5.]);
SELECT timeSeriesDeltaToGrid(120, 120, 1, 60)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 120, 110]), [5., 5., nan]);
SELECT timeSeriesChangesToGrid(120, 120, 1, 60)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 120, 110]), [nan, 5., 5.]);
SELECT timeSeriesChangesToGrid(120, 120, 1, 60)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 120, 110]), [5., 5., nan]);

-- The same unsorted states with the duplicate NaN, consumed by the `-Merge` combinator in memory (merge into an empty state normalizes the argument) and serialized to disk through an `AggregatingMergeTree` roundtrip (`serialize` normalizes a copy): the first arrival must win on both paths too.
SELECT 'NaN at a duplicate timestamp keeps the first arrival on merge and serialization paths (delta [nan] [0] twice):';
SELECT timeSeriesDeltaToGridMerge(120, 120, 1, 60)(st) FROM (SELECT initializeAggregation('timeSeriesDeltaToGridState(120, 120, 1, 60)', arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 120, 110]), [nan, 5., 5.]) AS st);
SELECT timeSeriesDeltaToGridMerge(120, 120, 1, 60)(st) FROM (SELECT initializeAggregation('timeSeriesDeltaToGridState(120, 120, 1, 60)', arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 120, 110]), [5., 5., nan]) AS st);
DROP TABLE IF EXISTS ts_nan_states;
CREATE TABLE ts_nan_states (k UInt8, st AggregateFunction(timeSeriesDeltaToGrid(120, 120, 1, 60), DateTime64(3, 'UTC'), Float64)) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO ts_nan_states
SELECT 1, timeSeriesDeltaToGridState(120, 120, 1, 60)(toDateTime64(tv.1, 3, 'UTC'), tv.2)
FROM (SELECT arrayJoin(arrayZip([110, 120, 110], [nan, 5., 5.])) AS tv)
SETTINGS max_threads = 1;
INSERT INTO ts_nan_states
SELECT 2, timeSeriesDeltaToGridState(120, 120, 1, 60)(toDateTime64(tv.1, 3, 'UTC'), tv.2)
FROM (SELECT arrayJoin(arrayZip([110, 120, 110], [5., 5., nan])) AS tv)
SETTINGS max_threads = 1;
SELECT timeSeriesDeltaToGridMerge(120, 120, 1, 60)(st) FROM ts_nan_states GROUP BY k ORDER BY k;
DROP TABLE ts_nan_states;

-- `initializeAggregation` over a shuffled array builds a state that is still unsorted when the `-Merge` combinator consumes it, deterministically covering merge-into-empty and overlapping-range merge with an unsorted argument.
SELECT 'merging unsorted in-memory states equals the baseline (1):';
SELECT timeSeriesRateToGridMerge(100, 200, 20, 100)(st) = (SELECT timeSeriesRateToGrid(100, 200, 20, 100)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 125, 140, 155, 170, 185, 200]), [1., 3, 2, 5, 4, 6, 8]))
FROM
(
    SELECT initializeAggregation('timeSeriesRateToGridState(100, 200, 20, 100)', arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_arr), val_arr) AS st
    FROM values('ts_arr Array(UInt32), val_arr Array(Float64)', ([155, 110, 200], [5., 1, 8]), ([125, 185, 140, 170], [3., 6, 2, 4]))
)
SETTINGS max_threads = 1;

-- Scalar path: the same series stored as one sorted part and as two parts with interleaved timestamp ranges - a single-threaded scan concatenates the parts, so the per-bucket adds go out of order at every part boundary whichever part is read first.
DROP TABLE IF EXISTS ts_one_part;
DROP TABLE IF EXISTS ts_two_parts;
CREATE TABLE ts_one_part (id UInt64, timestamp DateTime64(3, 'UTC'), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE ts_two_parts (id UInt64, timestamp DateTime64(3, 'UTC'), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
SYSTEM STOP MERGES ts_two_parts;

-- 4 series, 40 samples each, 15s apart over [100, 685]; the sawtooth value gives rate/changes/resets transitions to count.
INSERT INTO ts_one_part SELECT number % 4, toDateTime64(100 + intDiv(number, 4) * 15, 3, 'UTC'), (intDiv(number, 4) % 7)::Float64 FROM numbers(160);
INSERT INTO ts_two_parts SELECT number % 4, toDateTime64(100 + intDiv(number, 4) * 15, 3, 'UTC'), (intDiv(number, 4) % 7)::Float64 FROM numbers(160) WHERE intDiv(number, 4) % 2 = 0;
INSERT INTO ts_two_parts SELECT number % 4, toDateTime64(100 + intDiv(number, 4) * 15, 3, 'UTC'), (intDiv(number, 4) % 7)::Float64 FROM numbers(160) WHERE intDiv(number, 4) % 2 = 1;

-- In-order aggregation would feed each state from a merged-sorted stream, hiding the disorder this test needs.
SET optimize_aggregation_in_order = 0;

SELECT 'single-threaded scan over interleaved parts equals the sorted part (all 1):';
SELECT a.id, a.rate = b.rate, a.changes = b.changes, a.deriv = b.deriv
FROM
(
    SELECT id, timeSeriesRateToGrid(100, 700, 60, 120)(timestamp, value) AS rate, timeSeriesChangesToGrid(100, 700, 60, 120)(timestamp, value) AS changes, timeSeriesDerivToGrid(100, 700, 60, 120)(timestamp, value) AS deriv
    FROM ts_two_parts GROUP BY id
    SETTINGS max_threads = 1
) AS a
INNER JOIN
(
    SELECT id, timeSeriesRateToGrid(100, 700, 60, 120)(timestamp, value) AS rate, timeSeriesChangesToGrid(100, 700, 60, 120)(timestamp, value) AS changes, timeSeriesDerivToGrid(100, 700, 60, 120)(timestamp, value) AS deriv
    FROM ts_one_part GROUP BY id
    SETTINGS max_threads = 1
) AS b USING (id)
ORDER BY a.id;

-- Two-level parallel aggregation merges partial states in memory (`merge` of two sorted runs with interleaved ranges).
SELECT 'two-level parallel aggregation equals the sorted part (all 1):';
SELECT a.id, a.rate = b.rate, a.changes = b.changes, a.deriv = b.deriv
FROM
(
    SELECT id, timeSeriesRateToGrid(100, 700, 60, 120)(timestamp, value) AS rate, timeSeriesChangesToGrid(100, 700, 60, 120)(timestamp, value) AS changes, timeSeriesDerivToGrid(100, 700, 60, 120)(timestamp, value) AS deriv
    FROM ts_two_parts GROUP BY id
    SETTINGS max_threads = 4, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1
) AS a
INNER JOIN
(
    SELECT id, timeSeriesRateToGrid(100, 700, 60, 120)(timestamp, value) AS rate, timeSeriesChangesToGrid(100, 700, 60, 120)(timestamp, value) AS changes, timeSeriesDerivToGrid(100, 700, 60, 120)(timestamp, value) AS deriv
    FROM ts_one_part GROUP BY id
    SETTINGS max_threads = 1
) AS b USING (id)
ORDER BY a.id;

-- `remote()` to two loopback shards serializes the partial states on each shard and deserializes and merges them on the initiator; both shards read the same full table, so every sample arrives twice and the merge's deduplication must collapse the copies.
SELECT 'serialized-state merge over remote() equals the sorted part (all 1):';
SELECT a.id, a.rate = b.rate, a.changes = b.changes, a.deriv = b.deriv
FROM
(
    SELECT id, timeSeriesRateToGrid(100, 700, 60, 120)(timestamp, value) AS rate, timeSeriesChangesToGrid(100, 700, 60, 120)(timestamp, value) AS changes, timeSeriesDerivToGrid(100, 700, 60, 120)(timestamp, value) AS deriv
    FROM remote('127.0.0.{1,2}', currentDatabase(), ts_two_parts) GROUP BY id
) AS a
INNER JOIN
(
    SELECT id, timeSeriesRateToGrid(100, 700, 60, 120)(timestamp, value) AS rate, timeSeriesChangesToGrid(100, 700, 60, 120)(timestamp, value) AS changes, timeSeriesDerivToGrid(100, 700, 60, 120)(timestamp, value) AS deriv
    FROM ts_one_part GROUP BY id
    SETTINGS max_threads = 1
) AS b USING (id)
ORDER BY a.id;

DROP TABLE ts_one_part;
DROP TABLE ts_two_parts;

-- An `AggregatingMergeTree` roundtrip of a state built from out-of-order scalar rows (single thread preserves the arrayJoin order) deterministically drives serialization of an unsorted state through the on-disk Native path.
SELECT 'AggregatingMergeTree roundtrip of an unsorted state equals the baseline (1):';
DROP TABLE IF EXISTS ts_agg_states;
CREATE TABLE ts_agg_states (k UInt8, st AggregateFunction(timeSeriesRateToGrid(100, 200, 20, 100), DateTime64(3, 'UTC'), Float64)) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO ts_agg_states
SELECT 1, timeSeriesRateToGridState(100, 200, 20, 100)(ts, val)
FROM
(
    SELECT toDateTime64(tv.1, 3, 'UTC') AS ts, tv.2 AS val
    FROM (SELECT arrayJoin(arrayZip([155, 110, 200, 125, 140, 185, 170], [5., 1, 8, 3, 2, 6, 4])) AS tv)
)
SETTINGS max_threads = 1;
SELECT timeSeriesRateToGridMerge(100, 200, 20, 100)(st) = (SELECT timeSeriesRateToGrid(100, 200, 20, 100)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 125, 140, 155, 170, 185, 200]), [1., 3, 2, 5, 4, 6, 8])) FROM ts_agg_states;
DROP TABLE ts_agg_states;

-- Old peers serialize hash-map iteration order: the second literal is the first with the two 16-byte (timestamp, value) pairs of its two-sample bucket swapped, a FORMAT_VERSION 3 timeSeriesRateToGridState(100, 200, 20, 100) over samples (110, 1), (125, 3), (140, 2) - regenerate both literals if the serialization format ever changes.
SELECT 'deserializing out-of-order wire pairs equals the sorted wire (grid, 1):';
WITH
    CAST(unhex('03000A00000000000000020000000000000005000000000000000100000000000000B0AD010000000000000000000000F03F0600000000000000020000000000000048E80100000000000000000000000840E0220200000000000000000000000040'), 'AggregateFunction(timeSeriesRateToGrid(100, 200, 20, 100), DateTime64(3, \'UTC\'), Float64)') AS sorted_state,
    CAST(unhex('03000A00000000000000020000000000000005000000000000000100000000000000B0AD010000000000000000000000F03F06000000000000000200000000000000E022020000000000000000000000004048E80100000000000000000000000840'), 'AggregateFunction(timeSeriesRateToGrid(100, 200, 20, 100), DateTime64(3, \'UTC\'), Float64)') AS swapped_state
SELECT finalizeAggregation(sorted_state), finalizeAggregation(sorted_state) = finalizeAggregation(swapped_state);
