-- timeSeriesRateToGrid, timeSeriesIncreaseToGrid and timeSeriesDeltaToGrid sort a long out-of-order tail of samples
-- by splitting it into groups of consecutive timestamps. Shuffled input with duplicate timestamps must give the same
-- results as the same samples in sorted order, for any timestamp scale and for spans wider than 32 bits.

SET enable_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts_part_sort_3;
DROP TABLE IF EXISTS ts_part_sort_9;
-- Rows come back in the order of the hash, i.e. shuffled.
CREATE TABLE ts_part_sort_3 (h UInt64, ts DateTime64(3, 'UTC'), v Float64) ENGINE = MergeTree ORDER BY h;
CREATE TABLE ts_part_sort_9 (h UInt64, ts DateTime64(9, 'UTC'), v Float64) ENGINE = MergeTree ORDER BY h;

-- 20000 seconds with two samples at every second (the larger value must win), a counter with resets.
-- At scale 9 the span of 2e13 ticks does not fit 32 bits.
INSERT INTO ts_part_sort_3 SELECT intHash64(number), toDateTime64(1000000 + intDiv(number, 2), 3, 'UTC'), toFloat64((intDiv(number, 2) * 7) % 500 + number % 2) FROM numbers(40000);
INSERT INTO ts_part_sort_9 SELECT intHash64(number), toDateTime64(1000000 + intDiv(number, 2), 9, 'UTC'), toFloat64((intDiv(number, 2) * 7) % 500 + number % 2) FROM numbers(40000);

SELECT 'scale 3, shuffled = sorted (1 1 1), grid is not empty (1):';
SELECT
    timeSeriesRateToGrid(1000000, 1020000, 60, 300)(ts, v) = (SELECT timeSeriesRateToGrid(1000000, 1020000, 60, 300)(ts, v) FROM (SELECT ts, v FROM ts_part_sort_3 ORDER BY ts, v)),
    timeSeriesIncreaseToGrid(1000000, 1020000, 60, 300)(ts, v) = (SELECT timeSeriesIncreaseToGrid(1000000, 1020000, 60, 300)(ts, v) FROM (SELECT ts, v FROM ts_part_sort_3 ORDER BY ts, v)),
    timeSeriesDeltaToGrid(1000000, 1020000, 60, 300)(ts, v) = (SELECT timeSeriesDeltaToGrid(1000000, 1020000, 60, 300)(ts, v) FROM (SELECT ts, v FROM ts_part_sort_3 ORDER BY ts, v))
FROM ts_part_sort_3
SETTINGS max_threads = 1;
SELECT countIf(x IS NOT NULL) > 300 FROM (SELECT arrayJoin(timeSeriesRateToGrid(1000000, 1020000, 60, 300)(ts, v)) AS x FROM ts_part_sort_3) SETTINGS max_threads = 1;

SELECT 'scale 9, shuffled = sorted (1 1 1), grid is not empty (1):';
SELECT
    timeSeriesRateToGrid(1000000, 1020000, 60, 300)(ts, v) = (SELECT timeSeriesRateToGrid(1000000, 1020000, 60, 300)(ts, v) FROM (SELECT ts, v FROM ts_part_sort_9 ORDER BY ts, v)),
    timeSeriesIncreaseToGrid(1000000, 1020000, 60, 300)(ts, v) = (SELECT timeSeriesIncreaseToGrid(1000000, 1020000, 60, 300)(ts, v) FROM (SELECT ts, v FROM ts_part_sort_9 ORDER BY ts, v)),
    timeSeriesDeltaToGrid(1000000, 1020000, 60, 300)(ts, v) = (SELECT timeSeriesDeltaToGrid(1000000, 1020000, 60, 300)(ts, v) FROM (SELECT ts, v FROM ts_part_sort_9 ORDER BY ts, v))
FROM ts_part_sort_9
SETTINGS max_threads = 1;
SELECT countIf(x IS NOT NULL) > 300 FROM (SELECT arrayJoin(timeSeriesRateToGrid(1000000, 1020000, 60, 300)(ts, v)) AS x FROM ts_part_sort_9) SETTINGS max_threads = 1;

DROP TABLE ts_part_sort_3;
DROP TABLE ts_part_sort_9;
