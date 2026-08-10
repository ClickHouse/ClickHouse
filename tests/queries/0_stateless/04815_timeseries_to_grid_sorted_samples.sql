-- Tags: distributed

-- Tests the per-bucket sample storage of the `timeSeries*ToGrid` aggregate functions: input with out-of-order and
-- duplicate timestamps must give the same results as sorted unique input on every path (plain adds, in-memory merges
-- of partial states, serialized-state merges).

SET allow_experimental_time_series_aggregate_functions = 1;

-- The baseline: every function over sorted input with unique timestamps.
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

-- A shuffled array reliably tests the out-of-order add path.
-- The timestamps 125, 140, 170 appear twice with different values; each pair must keep the larger value to give
-- the same result as the baseline.
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

-- When several samples share a timestamp, the largest real value wins, and a NaN (a Prometheus stale marker) loses
-- to any real value, in whatever order the samples arrive. Each check must equal the aggregation of its explicit
-- deduplicated input.
SELECT 'duplicate timestamps keep the largest real value, a NaN loses to a real value (all 1):';
WITH
    arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 110, 120]) AS in_order,
    arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 120, 110]) AS out_of_order,
    arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 110, 110, 120]) AS in_order_triple,
    arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 120, 110, 110]) AS out_of_order_triple,
    arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 120]) AS deduped,
    timeSeriesDeltaToGrid(120, 120, 1, 60)(deduped, [5., 5.]) AS expected_delta,
    timeSeriesChangesToGrid(120, 120, 1, 60)(deduped, [5., 5.]) AS expected_changes,
    timeSeriesDeltaToGrid(120, 120, 1, 60)(deduped, [7., 5.]) AS expected_delta_larger
SELECT
    timeSeriesChangesToGrid(120, 120, 1, 60)(in_order, [5., 2, 5]) = expected_changes,
    timeSeriesChangesToGrid(120, 120, 1, 60)(in_order, [2., 5, 5]) = expected_changes,
    timeSeriesDeltaToGrid(120, 120, 1, 60)(in_order, [nan, 5., 5.]) = expected_delta,
    timeSeriesDeltaToGrid(120, 120, 1, 60)(in_order, [5., nan, 5.]) = expected_delta,
    timeSeriesChangesToGrid(120, 120, 1, 60)(in_order, [nan, 5., 5.]) = expected_changes,
    timeSeriesChangesToGrid(120, 120, 1, 60)(in_order, [5., nan, 5.]) = expected_changes,
    timeSeriesDeltaToGrid(120, 120, 1, 60)(out_of_order, [nan, 5., 5.]) = expected_delta,
    timeSeriesDeltaToGrid(120, 120, 1, 60)(out_of_order, [5., 5., nan]) = expected_delta,
    timeSeriesChangesToGrid(120, 120, 1, 60)(out_of_order, [nan, 5., 5.]) = expected_changes,
    timeSeriesChangesToGrid(120, 120, 1, 60)(out_of_order, [5., 5., nan]) = expected_changes,
    timeSeriesDeltaToGrid(120, 120, 1, 60)(in_order_triple, [5., nan, 7., 5.]) = expected_delta_larger,
    timeSeriesDeltaToGrid(120, 120, 1, 60)(out_of_order_triple, [5., 5., nan, 7.]) = expected_delta_larger;

-- A NaN survives deduplication only when every sample at the timestamp is NaN.
SELECT 'a timestamp with only NaN samples stays NaN (delta [nan]):';
SELECT timeSeriesDeltaToGrid(120, 120, 1, 60)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 110, 120]), [nan, nan, 5.]);

-- The `-Merge` combinator over hand-made states, covering the merge branches: the first state has an unsorted
-- bucket (200 arrives before 185), so merging it into the empty accumulator sorts it; the second state has
-- another unsorted bucket (140 before 125), so merging it into the non-empty accumulator sorts a copy and
-- joins the overlapping runs. Duplicates across the states must keep the largest real value (0.5 vs 2 at
-- timestamp 140) and drop the NaN (nan vs 4 at timestamp 170).
SELECT 'merging unsorted in-memory states equals the baseline (1):';
SELECT timeSeriesRateToGridMerge(100, 200, 20, 100)(st) = (SELECT timeSeriesRateToGrid(100, 200, 20, 100)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 125, 140, 155, 170, 185, 200]), [1., 3, 2, 5, 4, 6, 8]))
FROM
(
    SELECT initializeAggregation('timeSeriesRateToGridState(100, 200, 20, 100)', arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_arr), val_arr) AS st
    FROM values('ts_arr Array(UInt32), val_arr Array(Float64)', ([110, 200, 185, 140, 170], [1., 8, 6, 0.5, nan]), ([155, 140, 125, 170], [5., 2, 3, 4]))
)
SETTINGS max_threads = 1;

-- Two states put samples into the same bucket (140, 160] with non-overlapping ranges inside it (145 vs 155). With one
-- thread the merge order follows the row order, so one order takes the append fast path and the other the prepend
-- fast path; both must equal the baseline.
SELECT 'merging states with non-overlapping ranges in either order equals the baseline (1 1):';
WITH (SELECT timeSeriesRateToGrid(100, 200, 20, 100)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 125, 145, 155, 170, 185, 200]), [1., 3, 2, 5, 4, 6, 8])) AS baseline
SELECT
    (SELECT timeSeriesRateToGridMerge(100, 200, 20, 100)(st) FROM (SELECT initializeAggregation('timeSeriesRateToGridState(100, 200, 20, 100)', arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_arr), val_arr) AS st FROM values('ts_arr Array(UInt32), val_arr Array(Float64)', ([110, 125, 145], [1., 3, 2]), ([155, 170, 185, 200], [5., 4, 6, 8])))) = baseline,
    (SELECT timeSeriesRateToGridMerge(100, 200, 20, 100)(st) FROM (SELECT initializeAggregation('timeSeriesRateToGridState(100, 200, 20, 100)', arrayMap(x -> toDateTime64(x, 3, 'UTC'), ts_arr), val_arr) AS st FROM values('ts_arr Array(UInt32), val_arr Array(Float64)', ([155, 170, 185, 200], [5., 4, 6, 8]), ([110, 125, 145], [1., 3, 2])))) = baseline
SETTINGS max_threads = 1;

-- An `AggregatingMergeTree` roundtrip of a state with an unsorted bucket (185 arrives after 200) makes `serialize`
-- run on a state that is still unsorted in memory, so it must sort a copy and write that; the readback must equal
-- the baseline.
SELECT 'AggregatingMergeTree roundtrip of an unsorted state equals the baseline (1):';
DROP TABLE IF EXISTS ts_agg_states;
CREATE TABLE ts_agg_states (k UInt8, st AggregateFunction(timeSeriesRateToGrid(100, 200, 20, 100), Array(DateTime64(3, 'UTC')), Array(Float64))) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO ts_agg_states SELECT 1, initializeAggregation('timeSeriesRateToGridState(100, 200, 20, 100)', arrayMap(x -> toDateTime64(x, 3, 'UTC'), [155, 110, 200, 125, 140, 185, 170]), [5., 1, 8, 3, 2, 6, 4]);
SELECT timeSeriesRateToGridMerge(100, 200, 20, 100)(st) = (SELECT timeSeriesRateToGrid(100, 200, 20, 100)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), [110, 125, 140, 155, 170, 185, 200]), [1., 3, 2, 5, 4, 6, 8])) FROM ts_agg_states;
DROP TABLE ts_agg_states;

-- Servers of older versions serialize samples in hash-map order, so any wire order must be accepted. Both literals
-- are a FORMAT_VERSION 3 timeSeriesRateToGridState(100, 200, 20, 100) over samples (110, 1), (125, 3), (140, 2);
-- in the second one the two 16-byte (timestamp, value) pairs of its two-sample bucket are swapped.
-- Regenerate both literals if the serialization format ever changes.
SELECT 'deserializing samples in a different wire order gives the same result (grid, 1):';
WITH
    CAST(unhex('03000A00000000000000020000000000000005000000000000000100000000000000B0AD010000000000000000000000F03F0600000000000000020000000000000048E80100000000000000000000000840E0220200000000000000000000000040'), 'AggregateFunction(timeSeriesRateToGrid(100, 200, 20, 100), DateTime64(3, \'UTC\'), Float64)') AS sorted_state,
    CAST(unhex('03000A00000000000000020000000000000005000000000000000100000000000000B0AD010000000000000000000000F03F06000000000000000200000000000000E022020000000000000000000000004048E80100000000000000000000000840'), 'AggregateFunction(timeSeriesRateToGrid(100, 200, 20, 100), DateTime64(3, \'UTC\'), Float64)') AS swapped_state
SELECT finalizeAggregation(sorted_state), finalizeAggregation(sorted_state) = finalizeAggregation(swapped_state);

-- Scalar path: the same series stored as one sorted part and as two parts with interleaved timestamps.
-- A single-threaded scan reads one part after the other, so the adds go out of order at every part boundary,
-- whichever part is read first.
DROP TABLE IF EXISTS ts_one_part;
DROP TABLE IF EXISTS ts_two_parts;
DROP VIEW IF EXISTS ts_ref;
CREATE TABLE ts_one_part (id UInt64, timestamp DateTime64(3, 'UTC'), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE ts_two_parts (id UInt64, timestamp DateTime64(3, 'UTC'), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
SYSTEM STOP MERGES ts_two_parts;

-- 4 series, 40 samples each, 15s apart over [100, 685]; the sawtooth value gives rate/changes/resets transitions to count.
INSERT INTO ts_one_part SELECT number % 4, toDateTime64(100 + intDiv(number, 4) * 15, 3, 'UTC'), (intDiv(number, 4) % 7)::Float64 FROM numbers(160);
INSERT INTO ts_two_parts SELECT number % 4, toDateTime64(100 + intDiv(number, 4) * 15, 3, 'UTC'), (intDiv(number, 4) % 7)::Float64 FROM numbers(160) WHERE intDiv(number, 4) % 2 = 0;
INSERT INTO ts_two_parts SELECT number % 4, toDateTime64(100 + intDiv(number, 4) * 15, 3, 'UTC'), (intDiv(number, 4) % 7)::Float64 FROM numbers(160) WHERE intDiv(number, 4) % 2 = 1;

-- In-order aggregation would feed each state from an already sorted stream and hide the disorder this test needs.
SET optimize_aggregation_in_order = 0;

-- The reference: the sorted single part, read by one thread, so every state is built from in-order adds.
CREATE VIEW ts_ref AS
SELECT
    id,
    timeSeriesRateToGrid(100, 700, 60, 120)(timestamp, value) AS rate,
    timeSeriesChangesToGrid(100, 700, 60, 120)(timestamp, value) AS changes,
    timeSeriesDerivToGrid(100, 700, 60, 120)(timestamp, value) AS deriv
FROM ts_one_part
GROUP BY id
SETTINGS max_threads = 1;

SELECT 'single-threaded scan over interleaved parts equals the sorted part (all 1):';
SELECT a.id, (a.rate, a.changes, a.deriv) = (b.rate, b.changes, b.deriv)
FROM
(
    SELECT id, timeSeriesRateToGrid(100, 700, 60, 120)(timestamp, value) AS rate, timeSeriesChangesToGrid(100, 700, 60, 120)(timestamp, value) AS changes, timeSeriesDerivToGrid(100, 700, 60, 120)(timestamp, value) AS deriv
    FROM ts_two_parts GROUP BY id
    SETTINGS max_threads = 1
) AS a
INNER JOIN ts_ref AS b USING (id)
ORDER BY a.id;

-- Two-level parallel aggregation merges partial states in memory (`merge` of two sorted runs with interleaved ranges).
SELECT 'two-level parallel aggregation equals the sorted part (all 1):';
SELECT a.id, (a.rate, a.changes, a.deriv) = (b.rate, b.changes, b.deriv)
FROM
(
    SELECT id, timeSeriesRateToGrid(100, 700, 60, 120)(timestamp, value) AS rate, timeSeriesChangesToGrid(100, 700, 60, 120)(timestamp, value) AS changes, timeSeriesDerivToGrid(100, 700, 60, 120)(timestamp, value) AS deriv
    FROM ts_two_parts GROUP BY id
    SETTINGS max_threads = 4, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1
) AS a
INNER JOIN ts_ref AS b USING (id)
ORDER BY a.id;

-- `remote()` with two loopback shards makes each shard serialize its partial states and the initiator deserialize
-- and merge them; both shards read the same full table, so every sample arrives twice and the deduplication
-- in `merge` must remove the copies.
SELECT 'serialized-state merge over remote() equals the sorted part (all 1):';
SELECT a.id, (a.rate, a.changes, a.deriv) = (b.rate, b.changes, b.deriv)
FROM
(
    SELECT id, timeSeriesRateToGrid(100, 700, 60, 120)(timestamp, value) AS rate, timeSeriesChangesToGrid(100, 700, 60, 120)(timestamp, value) AS changes, timeSeriesDerivToGrid(100, 700, 60, 120)(timestamp, value) AS deriv
    FROM remote('127.0.0.{1,2}', currentDatabase(), ts_two_parts) GROUP BY id
) AS a
INNER JOIN ts_ref AS b USING (id)
ORDER BY a.id;

DROP VIEW ts_ref;
DROP TABLE ts_one_part;
DROP TABLE ts_two_parts;
