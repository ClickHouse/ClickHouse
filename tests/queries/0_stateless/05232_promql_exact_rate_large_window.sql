-- Tags: no-random-settings

-- Exact mode with a window overlapping many samples: the window must slide without summing it again per grid point.
SET enable_time_series_aggregate_functions = 1;

SELECT 'rate';
SELECT length(r), round(r[-1], 6) FROM
(
    SELECT timeSeriesRateToGrid(0, 100000, 10, 30000, 1)(toUInt32(number), toFloat64(number)) AS r
    FROM numbers(100001)
);

SELECT 'rate with a reset every other sample';
SELECT length(r), round(r[-1], 6) FROM
(
    SELECT timeSeriesRateToGrid(0, 100000, 10, 30000, 1)(toUInt32(number), toFloat64(number % 2)) AS r
    FROM numbers(100001)
);

SELECT 'delta';
SELECT length(r), round(r[-1], 6) FROM
(
    SELECT timeSeriesDeltaToGrid(0, 100000, 10, 30000, 1)(toUInt32(number), toFloat64(number)) AS r
    FROM numbers(100001)
);
