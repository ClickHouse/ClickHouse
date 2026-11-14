-- The buckets of a `timeSeries*ToGrid` state live in a hash map while sparse and in a plain array once they fill half
-- of their index range. The results must not depend on that, whatever order the samples come in.

SET allow_experimental_time_series_aggregate_functions = 1;

-- A sample every 4 s added in a scrambled order, so the state moves between the map and the array.
SELECT 'all functions, scrambled order:';
WITH
    arraySort(i -> cityHash64(i), range(83)) AS perm,
    arrayMap(i -> toDateTime(72 + 4 * i, 'UTC'), perm) AS ts,
    arrayMap(i -> toFloat64((i * 37) % 23 + 1), perm) AS vals
SELECT
    timeSeriesRateToGrid(100, 400, 10, 30)(ts, vals) AS rate,
    timeSeriesIncreaseToGrid(100, 400, 10, 30)(ts, vals) AS increase,
    timeSeriesDeltaToGrid(100, 400, 10, 30)(ts, vals) AS delta,
    timeSeriesInstantRateToGrid(100, 400, 10, 30)(ts, vals) AS instant_rate,
    timeSeriesInstantDeltaToGrid(100, 400, 10, 30)(ts, vals) AS instant_delta,
    timeSeriesChangesToGrid(100, 400, 10, 30)(ts, vals) AS changes,
    timeSeriesResetsToGrid(100, 400, 10, 30)(ts, vals) AS resets,
    timeSeriesDerivToGrid(100, 400, 10, 30)(ts, vals) AS deriv,
    timeSeriesPredictLinearToGrid(100, 400, 10, 30, 60)(ts, vals) AS predict_linear,
    timeSeriesLinearRegressionToGrid(100, 400, 10, 30)(ts, vals) AS linear_regression,
    timeSeriesQuantileToGrid(100, 400, 10, 30)(ts, vals, 0.25) AS quantile,
    timeSeriesMadToGrid(100, 400, 10, 30)(ts, vals) AS mad,
    timeSeriesCountToGrid(100, 400, 10, 30)(ts, vals) AS count,
    timeSeriesSumToGrid(100, 400, 10, 30)(ts, vals) AS sum,
    timeSeriesAvgToGrid(100, 400, 10, 30)(ts, vals) AS avg,
    timeSeriesMinToGrid(100, 400, 10, 30)(ts, vals) AS min,
    timeSeriesMaxToGrid(100, 400, 10, 30)(ts, vals) AS max,
    timeSeriesStddevToGrid(100, 400, 10, 30)(ts, vals) AS stddev,
    timeSeriesStdvarToGrid(100, 400, 10, 30)(ts, vals) AS stdvar,
    timeSeriesLastToGrid(100, 400, 10, 30)(ts, vals) AS last,
    timeSeriesFirstToGrid(100, 400, 10, 30)(ts, vals) AS first,
    timeSeriesPresentToGrid(100, 400, 10, 30)(ts, vals) AS present,
    timeSeriesTimestampOfMinToGrid(100, 400, 10, 30)(ts, vals) AS ts_of_min,
    timeSeriesTimestampOfMaxToGrid(100, 400, 10, 30)(ts, vals) AS ts_of_max,
    timeSeriesTimestampOfFirstToGrid(100, 400, 10, 30)(ts, vals) AS ts_of_first,
    timeSeriesTimestampOfLastToGrid(100, 400, 10, 30)(ts, vals) AS ts_of_last
FORMAT Vertical;

-- One series over 4003 buckets, added row by row in five orders: every order must give the same results.
DROP TABLE IF EXISTS ordered_samples;
CREATE TABLE ordered_samples (name String, pos UInt32, ts DateTime('UTC'), value Float64) ENGINE = MergeTree ORDER BY (name, pos);
INSERT INTO ordered_samples
SELECT name, pos, toDateTime(940 + 7 * i, 'UTC'), toFloat64((i * 37) % 1000 + 1)
FROM
(
    SELECT 'ascending' AS name, range(5723) AS perm
    UNION ALL SELECT 'descending', arrayReverse(range(5723))
    UNION ALL SELECT 'scrambled', arraySort(i -> cityHash64(i), range(5723))
    UNION ALL SELECT 'sparse first', arrayConcat(arrayFilter(i -> i % 400 = 0, range(5723)), arrayFilter(i -> i % 400 != 0, range(5723)))
    UNION ALL SELECT 'second half first', arrayConcat(range(2861, 5723), range(2861))
)
ARRAY JOIN perm AS i, arrayEnumerate(perm) AS pos;

SELECT 'orders, distinct results (5 1 1 1 1 1):';
SELECT count(), uniqExact(rate), uniqExact(sum), uniqExact(quantile), uniqExact(max), uniqExact(last)
FROM
(
    SELECT
        name,
        timeSeriesRateToGrid(1000, 41000, 10, 30)(ts, value) AS rate,
        timeSeriesSumToGrid(1000, 41000, 10, 30)(ts, value) AS sum,
        timeSeriesQuantileToGrid(1000, 41000, 10, 30)(ts, value, 0.9) AS quantile,
        timeSeriesMaxToGrid(1000, 41000, 10, 30)(ts, value) AS max,
        timeSeriesLastToGrid(1000, 41000, 10, 30)(ts, value) AS last
    FROM ordered_samples
    GROUP BY name
)
SETTINGS max_threads = 1, max_block_size = 1000;

SELECT 'ascending, points and extremes of rate and max:';
SELECT arrayCount(x -> x IS NOT NULL, rate), arrayMin(arrayFilter(x -> x IS NOT NULL, rate)), arrayMax(arrayFilter(x -> x IS NOT NULL, rate)),
    arrayCount(x -> x IS NOT NULL, max), arrayMin(arrayFilter(x -> x IS NOT NULL, max)), arrayMax(arrayFilter(x -> x IS NOT NULL, max))
FROM
(
    SELECT
        timeSeriesRateToGrid(1000, 41000, 10, 30)(ts, value) AS rate,
        timeSeriesMaxToGrid(1000, 41000, 10, 30)(ts, value) AS max
    FROM ordered_samples
    WHERE name = 'ascending'
);

-- Partial states that end up dense or sparse, stored in a table and merged back.
DROP TABLE IF EXISTS states;
CREATE TABLE states
(
    part String,
    rate AggregateFunction(timeSeriesRateToGrid(1000, 41000, 10, 30), DateTime('UTC'), Float64),
    max AggregateFunction(timeSeriesMaxToGrid(1000, 41000, 10, 30), DateTime('UTC'), Float64),
    quantile AggregateFunction(timeSeriesQuantileToGrid(1000, 41000, 10, 30), DateTime('UTC'), Float64, Float64)
)
ENGINE = MergeTree ORDER BY part;

INSERT INTO states
SELECT
    part,
    timeSeriesRateToGridState(1000, 41000, 10, 30)(ts, value),
    timeSeriesMaxToGridState(1000, 41000, 10, 30)(ts, value),
    timeSeriesQuantileToGridState(1000, 41000, 10, 30)(ts, value, 0.9)
FROM
(
    SELECT multiIf(ts BETWEEN 15000 AND 22000, 'dense middle', intDiv(toUnixTimestamp(ts) - 940, 7) % 97 = 0, 'sparse', 'rest') AS part, ts, value
    FROM ordered_samples
    WHERE name = if(part = 'rest', 'descending', 'ascending')
    ORDER BY name, pos
)
GROUP BY part
SETTINGS max_threads = 1;

SELECT 'merged states equal one pass (1 1 1):';
SELECT
    timeSeriesRateToGridMerge(1000, 41000, 10, 30)(rate)
        = (SELECT timeSeriesRateToGrid(1000, 41000, 10, 30)(ts, value) FROM ordered_samples WHERE name = 'ascending'),
    timeSeriesMaxToGridMerge(1000, 41000, 10, 30)(max)
        = (SELECT timeSeriesMaxToGrid(1000, 41000, 10, 30)(ts, value) FROM ordered_samples WHERE name = 'ascending'),
    timeSeriesQuantileToGridMerge(1000, 41000, 10, 30)(quantile)
        = (SELECT timeSeriesQuantileToGrid(1000, 41000, 10, 30)(ts, value, 0.9) FROM ordered_samples WHERE name = 'ascending')
FROM states;

-- Six series of different density, read by many threads in small blocks, so partial states of every kind are merged.
-- Each result is compared with the same function over the series as one sorted array.
DROP VIEW IF EXISTS shapes;
CREATE VIEW shapes AS
SELECT toUInt64(number % 6) AS series, intDiv(number, 6) AS i, toDateTime(940 + 7 * i, 'UTC') AS ts, toFloat64((i * 37 + series) % 1000 + 1) AS value
FROM numbers_mt(34338)
WHERE multiIf(series = 0, true, series = 1, i % 300 = 0, series = 2, i >= 2861 OR i % 150 = 0, series = 3, i < 1500 OR i >= 4000, series = 4, i % 2 = 0, i % 3 = 0);

SELECT 'threads, per series: points of rate and max, runs, distinct results (2 1 1 1 1):';
SELECT series, arrayCount(x -> x IS NOT NULL, any(rate)), arrayCount(x -> x IS NOT NULL, any(max)), count(), uniqExact(rate), uniqExact(sum), uniqExact(quantile), uniqExact(max)
FROM
(
    SELECT
        series,
        timeSeriesRateToGrid(1000, 41000, 10, 30)(ts, value) AS rate,
        timeSeriesSumToGrid(1000, 41000, 10, 30)(ts, value) AS sum,
        timeSeriesQuantileToGrid(1000, 41000, 10, 30)(ts, value, 0.9) AS quantile,
        timeSeriesMaxToGrid(1000, 41000, 10, 30)(ts, value) AS max
    FROM shapes
    GROUP BY series
    UNION ALL
    SELECT
        series,
        timeSeriesRateToGrid(1000, 41000, 10, 30)(samples),
        timeSeriesSumToGrid(1000, 41000, 10, 30)(samples),
        timeSeriesQuantileToGrid(1000, 41000, 10, 30)(samples, 0.9),
        timeSeriesMaxToGrid(1000, 41000, 10, 30)(samples)
    FROM (SELECT series, arraySort(groupArray((ts, value))) AS samples FROM shapes GROUP BY series)
    GROUP BY series
)
GROUP BY series
ORDER BY series
SETTINGS max_threads = 8, max_block_size = 100;

SELECT 'threads with external aggregation, distinct results per series (1 1):';
SELECT series, uniqExact(rate), uniqExact(max)
FROM
(
    SELECT series, timeSeriesRateToGrid(1000, 41000, 10, 30)(ts, value) AS rate, timeSeriesMaxToGrid(1000, 41000, 10, 30)(ts, value) AS max
    FROM shapes
    GROUP BY series
    UNION ALL
    SELECT series, timeSeriesRateToGrid(1000, 41000, 10, 30)(samples), timeSeriesMaxToGrid(1000, 41000, 10, 30)(samples)
    FROM (SELECT series, arraySort(groupArray((ts, value))) AS samples FROM shapes GROUP BY series)
    GROUP BY series
)
GROUP BY series
ORDER BY series
SETTINGS max_threads = 8, max_block_size = 1000, group_by_two_level_threshold = 1, max_bytes_before_external_group_by = 1;

-- The peak memory of 200 states shows the memory of their buckets. A gappy series must not take more than the map,
-- and a dense series with one late bucket below it no more than the same buckets added in order.
SET max_threads = 1, max_block_size = 65536, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0,
    max_untracked_memory = 0, log_queries = 1;

SELECT series, timeSeriesSumToGridState(10, 200000, 10, 10)(toDateTime(10 + 10 * (i + intDiv(i, 2)), 'UTC'), toFloat64(i))
FROM (SELECT number % 200 AS series, intDiv(number, 200) AS i FROM numbers(1200000))
GROUP BY series FORMAT Null SETTINGS log_comment = '05340_gappy';

SELECT series, timeSeriesSumToGridState(10, 200000, 10, 10)(toDateTime(10 + 10 * if(i = 0, 19999, i - 1 + intDiv(i - 1, 2)), 'UTC'), toFloat64(i))
FROM (SELECT number % 200 AS series, intDiv(number, 200) AS i FROM numbers(1200000))
GROUP BY series FORMAT Null SETTINGS log_comment = '05340_map';

SELECT series, timeSeriesSumToGridState(10, 200000, 10, 10)(toDateTime(10 + 10 * (i + 100), 'UTC'), toFloat64(i))
FROM (SELECT number % 200 AS series, intDiv(number, 200) AS i FROM numbers(620200))
GROUP BY series FORMAT Null SETTINGS log_comment = '05340_dense';

SELECT series, timeSeriesSumToGridState(10, 200000, 10, 10)(toDateTime(10 + 10 * if(i = 3100, 100, i + 101), 'UTC'), toFloat64(i))
FROM (SELECT number % 200 AS series, intDiv(number, 200) AS i FROM numbers(620200))
GROUP BY series FORMAT Null SETTINGS log_comment = '05340_backfill';

SYSTEM FLUSH LOGS query_log;

SELECT 'memory: gappy within the map, backfill within dense (1 1):';
SELECT
    maxIf(memory_usage, log_comment = '05340_gappy') < 1.25 * maxIf(memory_usage, log_comment = '05340_map'),
    maxIf(memory_usage, log_comment = '05340_backfill') < 1.25 * maxIf(memory_usage, log_comment = '05340_dense')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment IN ('05340_gappy', '05340_map', '05340_dense', '05340_backfill');

DROP VIEW shapes;
DROP TABLE states;
DROP TABLE ordered_samples;
