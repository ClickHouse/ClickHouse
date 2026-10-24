-- timeSeriesRateToGrid, timeSeriesIncreaseToGrid and timeSeriesDeltaToGrid keep all samples of a state in one sorted buffer.
-- Out-of-order adds, duplicate timestamps, merges and serialized states must give the same results as sorted unique input.

SET enable_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts_flat;
CREATE TABLE ts_flat (k UInt32, ts DateTime64(3, 'UTC'), v Float64) ENGINE = MergeTree ORDER BY k;
-- 47 samples 7 seconds apart over [75, 397], a counter with resets.
INSERT INTO ts_flat SELECT number, toDateTime64(75 + 7 * number, 3, 'UTC'), ((number * 3) % 50)::Float64 FROM numbers(47);

-- Window 60 has one bucket per step, window 50 splits every step, window 10 leaves gaps between the windows.
SELECT 'baseline:';
SELECT
    timeSeriesRateToGrid(100, 400, 20, 60)(ts, v),
    timeSeriesIncreaseToGrid(100, 400, 20, 50)(ts, v),
    timeSeriesDeltaToGrid(100, 400, 20, 10)(ts, v)
FROM (SELECT * FROM ts_flat ORDER BY k)
SETTINGS max_threads = 1
FORMAT Vertical;

SELECT 'reversed rows (1 1 1):';
SELECT
    timeSeriesRateToGrid(100, 400, 20, 60)(ts, v) = (SELECT timeSeriesRateToGrid(100, 400, 20, 60)(ts, v) FROM ts_flat),
    timeSeriesIncreaseToGrid(100, 400, 20, 50)(ts, v) = (SELECT timeSeriesIncreaseToGrid(100, 400, 20, 50)(ts, v) FROM ts_flat),
    timeSeriesDeltaToGrid(100, 400, 20, 10)(ts, v) = (SELECT timeSeriesDeltaToGrid(100, 400, 20, 10)(ts, v) FROM ts_flat)
FROM (SELECT * FROM ts_flat ORDER BY k DESC)
SETTINGS max_threads = 1;

SELECT 'reversed arrays (1 1 1):';
WITH
    arrayReverse(range(47)) AS ks,
    arrayMap(n -> toDateTime64(75 + 7 * n, 3, 'UTC'), ks) AS rts,
    arrayMap(n -> ((n * 3) % 50)::Float64, ks) AS rv
SELECT
    timeSeriesRateToGrid(100, 400, 20, 60)(rts, rv) = (SELECT timeSeriesRateToGrid(100, 400, 20, 60)(ts, v) FROM ts_flat),
    timeSeriesIncreaseToGrid(100, 400, 20, 50)(rts, rv) = (SELECT timeSeriesIncreaseToGrid(100, 400, 20, 50)(ts, v) FROM ts_flat),
    timeSeriesDeltaToGrid(100, 400, 20, 10)(rts, rv) = (SELECT timeSeriesDeltaToGrid(100, 400, 20, 10)(ts, v) FROM ts_flat);

-- Every fifth sample arrives again later with a smaller value and with NaN; the larger real value must win.
SELECT 'duplicate timestamps (1 1 1):';
SELECT
    timeSeriesRateToGrid(100, 400, 20, 60)(ts, v) = (SELECT timeSeriesRateToGrid(100, 400, 20, 60)(ts, v) FROM ts_flat),
    timeSeriesIncreaseToGrid(100, 400, 20, 50)(ts, v) = (SELECT timeSeriesIncreaseToGrid(100, 400, 20, 50)(ts, v) FROM ts_flat),
    timeSeriesDeltaToGrid(100, 400, 20, 10)(ts, v) = (SELECT timeSeriesDeltaToGrid(100, 400, 20, 10)(ts, v) FROM ts_flat)
FROM
(
    SELECT * FROM
    (
        SELECT k, ts, v FROM ts_flat
        UNION ALL SELECT k, ts, v - 1 FROM ts_flat WHERE k % 5 = 0
        UNION ALL SELECT k, ts, nan FROM ts_flat WHERE k % 5 = 0
    )
    ORDER BY k % 3, k DESC
)
SETTINGS max_threads = 1;

-- Interleaved partial states need a real merge; halves in reverse order arrive out of order.
SELECT 'merge of states (1 1 1 1):';
SELECT
    (SELECT timeSeriesRateToGridMerge(100, 400, 20, 60)(s) FROM (SELECT timeSeriesRateToGridState(100, 400, 20, 60)(ts, v) AS s FROM ts_flat GROUP BY k % 3))
        = (SELECT timeSeriesRateToGrid(100, 400, 20, 60)(ts, v) FROM ts_flat),
    (SELECT timeSeriesIncreaseToGridMerge(100, 400, 20, 50)(s) FROM (SELECT timeSeriesIncreaseToGridState(100, 400, 20, 50)(ts, v) AS s FROM ts_flat GROUP BY k % 3))
        = (SELECT timeSeriesIncreaseToGrid(100, 400, 20, 50)(ts, v) FROM ts_flat),
    (SELECT timeSeriesDeltaToGridMerge(100, 400, 20, 10)(s) FROM (SELECT timeSeriesDeltaToGridState(100, 400, 20, 10)(ts, v) AS s FROM ts_flat GROUP BY k % 3))
        = (SELECT timeSeriesDeltaToGrid(100, 400, 20, 10)(ts, v) FROM ts_flat),
    (SELECT timeSeriesRateToGridMerge(100, 400, 20, 60)(s) FROM (SELECT timeSeriesRateToGridState(100, 400, 20, 60)(ts, v) AS s FROM ts_flat GROUP BY k < 20 ORDER BY k < 20))
        = (SELECT timeSeriesRateToGrid(100, 400, 20, 60)(ts, v) FROM ts_flat)
SETTINGS max_threads = 1;

-- 2000 partial states merged newest first, in a shuffled order and interleaved must match one pass over the samples.
SELECT 'many merged states (1 1 1):';
WITH (SELECT timeSeriesRateToGrid(0, 20000, 100, 300)(toDateTime64(number, 3, 'UTC'), (number % 1000)::Float64) FROM numbers(20000)) AS expected
SELECT
    (SELECT timeSeriesRateToGridMerge(0, 20000, 100, 300)(s) FROM (SELECT intDiv(number, 10) AS c, timeSeriesRateToGridState(0, 20000, 100, 300)(toDateTime64(number, 3, 'UTC'), (number % 1000)::Float64) AS s FROM numbers(20000) GROUP BY c ORDER BY c DESC)) = expected,
    (SELECT timeSeriesRateToGridMerge(0, 20000, 100, 300)(s) FROM (SELECT intDiv(number, 10) AS c, timeSeriesRateToGridState(0, 20000, 100, 300)(toDateTime64(number, 3, 'UTC'), (number % 1000)::Float64) AS s FROM numbers(20000) GROUP BY c ORDER BY cityHash64(c))) = expected,
    (SELECT timeSeriesRateToGridMerge(0, 20000, 100, 300)(s) FROM (SELECT number % 2000 AS c, timeSeriesRateToGridState(0, 20000, 100, 300)(toDateTime64(number, 3, 'UTC'), (number % 1000)::Float64) AS s FROM numbers(20000) GROUP BY c ORDER BY c DESC)) = expected
SETTINGS max_threads = 1;

-- States that interleave in stretches, states that share every timestamp and states with an unsorted tail must merge like one pass.
SELECT 'merged runs (1 1 1):';
WITH
    (SELECT timeSeriesRateToGrid(0, 20000, 100, 300)(toDateTime64(number, 3, 'UTC'), (number % 1000)::Float64) FROM numbers(20000)) AS expected,
    (SELECT timeSeriesRateToGrid(0, 20000, 100, 300)(t, m) FROM (SELECT toDateTime64(number % 20000, 3, 'UTC') AS t, max(((number * 7) % 1000)::Float64) AS m FROM numbers(8 * 20000) GROUP BY t ORDER BY t)) AS expected_shared
SELECT
    (SELECT timeSeriesRateToGridMerge(0, 20000, 100, 300)(s) FROM (SELECT intDiv(number, 26) % 6 AS c, timeSeriesRateToGridState(0, 20000, 100, 300)(toDateTime64(number, 3, 'UTC'), (number % 1000)::Float64) AS s FROM numbers(20000) GROUP BY c ORDER BY c)) = expected,
    (SELECT timeSeriesRateToGridMerge(0, 20000, 100, 300)(s) FROM (SELECT intDiv(number, 20000) AS c, timeSeriesRateToGridState(0, 20000, 100, 300)(toDateTime64(number % 20000, 3, 'UTC'), ((number * 7) % 1000)::Float64) AS s FROM numbers(8 * 20000) GROUP BY c ORDER BY c)) = expected_shared,
    (SELECT timeSeriesRateToGridMerge(0, 20000, 100, 300)(s) FROM (SELECT c, timeSeriesRateToGridState(0, 20000, 100, 300)(t, v) AS s FROM (SELECT number % 5 AS c, toDateTime64(number, 3, 'UTC') AS t, (number % 1000)::Float64 AS v FROM numbers(20000) ORDER BY intDiv(number, 3000), cityHash64(number)) GROUP BY c ORDER BY c DESC)) = expected
SETTINGS max_threads = 1;

-- One state over 300 series with the same 310 timestamps: every timestamp arrives 300 times and the largest value must win.
SELECT 'many series in one state (1 1):';
WITH
    (SELECT timeSeriesRateToGrid(1000, 4000, 30, 120)(t, m) FROM (SELECT toDateTime64(900 + 10 * (number % 310), 3, 'UTC') AS t, max(((number * 7) % 1000)::Float64) AS m FROM numbers(300 * 310) GROUP BY t ORDER BY t)) AS expected
SELECT
    (SELECT timeSeriesRateToGrid(1000, 4000, 30, 120)(t, v) FROM (SELECT toDateTime64(900 + 10 * (number % 310), 3, 'UTC') AS t, ((number * 7) % 1000)::Float64 AS v FROM numbers(300 * 310) ORDER BY number)) = expected,
    (SELECT timeSeriesRateToGrid(1000, 4000, 30, 120)(ts, vs) FROM (SELECT groupArray(toDateTime64(900 + 10 * (number % 310), 3, 'UTC')) AS ts, groupArray(((number * 7) % 1000)::Float64) AS vs FROM (SELECT number FROM numbers(300 * 310) ORDER BY number))) = expected
SETTINGS max_threads = 1;

-- An unsorted state is written sorted and reads back to the same result.
SELECT 'serialized unsorted state (1):';
SELECT finalizeAggregation(CAST(CAST(s AS String) AS AggregateFunction(timeSeriesRateToGrid(100, 400, 20, 60), DateTime64(3, 'UTC'), Float64)))
    = (SELECT timeSeriesRateToGrid(100, 400, 20, 60)(ts, v) FROM ts_flat)
FROM (SELECT timeSeriesRateToGridState(100, 400, 20, 60)(ts, v) AS s FROM (SELECT * FROM ts_flat ORDER BY k DESC) SETTINGS max_threads = 1);

-- A state with more samples than are reserved before reading grows while it is read.
SELECT 'serialized big state (1):';
SELECT finalizeAggregation(CAST(CAST(timeSeriesRateToGridState(0, 70000, 1000, 1000)(toDateTime64(number, 3, 'UTC'), number::Float64) AS String) AS AggregateFunction(timeSeriesRateToGrid(0, 70000, 1000, 1000), DateTime64(3, 'UTC'), Float64)))
    = timeSeriesRateToGrid(0, 70000, 1000, 1000)(toDateTime64(number, 3, 'UTC'), number::Float64)
FROM numbers(70000);

-- The same series sent again and again, with as many distinct timestamps as just below, at and just above a buffer capacity.
SELECT 'repeated series (1 1 1):';
SELECT
    (SELECT timeSeriesRateToGrid(1000, 5100, 1, 60)(toDateTime(1000 + number % 4095), toFloat64(number % 4095)) FROM numbers(4095 * 20))
        = (SELECT timeSeriesRateToGrid(1000, 5100, 1, 60)(toDateTime(1000 + number), toFloat64(number)) FROM numbers(4095)),
    (SELECT timeSeriesRateToGrid(1000, 5100, 1, 60)(toDateTime(1000 + number % 4096), toFloat64(number % 4096)) FROM numbers(4096 * 20))
        = (SELECT timeSeriesRateToGrid(1000, 5100, 1, 60)(toDateTime(1000 + number), toFloat64(number)) FROM numbers(4096)),
    (SELECT timeSeriesRateToGrid(1000, 5100, 1, 60)(toDateTime(1000 + number % 4097), toFloat64(number % 4097)) FROM numbers(4097 * 20))
        = (SELECT timeSeriesRateToGrid(1000, 5100, 1, 60)(toDateTime(1000 + number), toFloat64(number)) FROM numbers(4097))
SETTINGS max_threads = 1;

-- The same repeated series through the per-row paths (-If and a Nullable value) keeps only the distinct timestamps in memory.
SELECT 'repeated series, per-row adds (1 1):';
SELECT
    (SELECT timeSeriesRateToGridIf(1000, 5100, 1, 60)(toDateTime(1000 + number % 4095), toFloat64(number % 4095), 1) FROM numbers(4095 * 1000))
        = (SELECT timeSeriesRateToGrid(1000, 5100, 1, 60)(toDateTime(1000 + number), toFloat64(number)) FROM numbers(4095)),
    (SELECT timeSeriesRateToGrid(1000, 5100, 1, 60)(toDateTime(1000 + number % 4095), toNullable(toFloat64(number % 4095))) FROM numbers(4095 * 1000))
        = (SELECT timeSeriesRateToGrid(1000, 5100, 1, 60)(toDateTime(1000 + number), toFloat64(number)) FROM numbers(4095))
SETTINGS max_threads = 1, max_memory_usage = 20000000;

-- Format version 6: the version, the bucket count, the number of samples and the samples in timestamp order.
SELECT 'state layout:';
SELECT hex(CAST(timeSeriesRateToGridState(100, 200, 20, 100)(arrayMap(x -> toDateTime64(x, 3, 'UTC'), [140, 110]), [2., 1]) AS String));

-- A format version 5 state (samples (110, 1), (125, 3), (140, 2) in a map of buckets) is rejected like any other version.
SELECT 'corrupted states:';
SELECT finalizeAggregation(CAST(unhex('05000A00000000000000020000000000000005000000000000000100000000000000B0AD010000000000000000000000F03F0600000000000000020000000000000048E80100000000000000000000000840E0220200000000000000000000000040'), 'AggregateFunction(timeSeriesRateToGrid(100, 200, 20, 100), DateTime64(3, \'UTC\'), Float64)')); -- { serverError INCORRECT_DATA }
-- A sample moved into the gap between two windows (105 with step 20 and window 10) belongs to no bucket.
SELECT finalizeAggregation(CAST(substring(s, 1, 18) || unhex('289A010000000000') || substring(s, 27) AS AggregateFunction(timeSeriesRateToGrid(100, 400, 20, 10), DateTime64(3, 'UTC'), Float64)))
FROM (SELECT CAST(timeSeriesRateToGridState(100, 400, 20, 10)(toDateTime64(100, 3, 'UTC'), 1.) AS String) AS s); -- { serverError INCORRECT_DATA }
-- A huge number of samples fails at the end of the data instead of allocating memory for it.
SELECT finalizeAggregation(CAST(substring(s, 1, 10) || unhex('FFFFFFFFFFFFFFFF') || substring(s, 19) AS AggregateFunction(timeSeriesRateToGrid(100, 400, 20, 10), DateTime64(3, 'UTC'), Float64)))
FROM (SELECT CAST(timeSeriesRateToGridState(100, 400, 20, 10)(toDateTime64(100, 3, 'UTC'), 1.) AS String) AS s); -- { serverError CANNOT_READ_ALL_DATA }

DROP TABLE ts_flat;
