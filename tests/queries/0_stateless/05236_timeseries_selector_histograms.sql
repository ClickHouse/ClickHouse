-- `timeSeriesSelector` over a TimeSeries table with histograms (version 6 and later) returns the float samples and the histogram
-- samples together; the column `histogram` holds a histogram sample as it's stored. Older tables return three columns.

SET allow_experimental_time_series_table = 1;
SET print_pretty_type_names = 0;

DROP TABLE IF EXISTS ts_hist;
DROP TABLE IF EXISTS ts_v5;

CREATE TABLE ts_hist ENGINE = TimeSeries;

SELECT '--- the columns ---';
DESCRIBE TABLE timeSeriesSelector(ts_hist, 'm', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3));

-- A float-only time series.
INSERT INTO ts_hist (metric_name, tags, samples)
    VALUES ('m_float', {'job': 'a'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.5), (toDateTime64('2026-01-01 00:01:00', 3), 2.5)]);

-- A histogram-only time series, an integer histogram then a float histogram. Every field has a distinct value.
INSERT INTO ts_hist (metric_name, tags, histograms.timestamp, histograms.schema, histograms.zero_threshold, histograms.count_int, histograms.zero_count_int, histograms.sum,
                     histograms.positive_spans, histograms.positive_values_int, histograms.negative_spans, histograms.negative_values_int)
    VALUES ('m_hist', {'job': 'a'}, [toDateTime64('2026-01-01 00:00:00', 3)], [3], [0.001], [50], [2], [12.5], [[(-2, 2), (1, 2)]], [[3, 10, 20, 9]], [[(0, 1)]], [[6]]);
INSERT INTO ts_hist (metric_name, tags, histograms.timestamp, histograms.is_float, histograms.counter_reset_hint, histograms.schema, histograms.zero_threshold,
                     histograms.count_float, histograms.zero_count_float, histograms.sum, histograms.positive_spans, histograms.positive_values_float)
    VALUES ('m_hist', {'job': 'a'}, [toDateTime64('2026-01-01 00:01:00', 3)], [true], [3], [2], [0.002], [7.5], [0.5], [3.25], [[(0, 3)]], [[1.5, 2.5, 3]]);

-- A histogram with custom buckets.
INSERT INTO ts_hist (metric_name, tags, histograms.timestamp, histograms.schema, histograms.count_int, histograms.sum,
                     histograms.positive_spans, histograms.positive_values_int, histograms.custom_values)
    VALUES ('m_nhcb', {'job': 'a'}, [toDateTime64('2026-01-01 00:00:00', 3)], [-53], [15], [4.2], [[(0, 4)]], [[1, 2, 4, 8]], [[0.1, 0.5, 1]]);

-- A time series with a float sample and a histogram sample, written in one row.
INSERT INTO ts_hist (metric_name, tags, samples, histograms.timestamp, histograms.schema, histograms.count_int, histograms.sum,
                     histograms.positive_spans, histograms.positive_values_int)
    VALUES ('m_mixed', {'job': 'a'}, [(toDateTime64('2026-01-01 00:00:00', 3), 7)], [toDateTime64('2026-01-01 00:01:00', 3)], [0], [5], [2.5], [[(0, 1)]], [[5]]);

-- An integer count which doesn't fit Float64 exactly, and a stale marker (the NaN payload 0x7FF0000000000002 in `sum`).
INSERT INTO ts_hist (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('m_big', {'job': 'a'}, [toDateTime64('2026-01-01 00:00:00', 3)], [9007199254740993], [1], [[(0, 1)]], [[9007199254740993]]);
INSERT INTO ts_hist (metric_name, tags, histograms.timestamp, histograms.sum)
    VALUES ('m_stale', {'job': 'a'}, [toDateTime64('2026-01-01 00:00:00', 3)], [reinterpretAsFloat64(toUInt64(9218868437227405314))]);

SELECT '--- a float-only time series: empty histogram ---';
SELECT timestamp, value, histogram
FROM timeSeriesSelector(ts_hist, 'm_float', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3))
ORDER BY timestamp;

SELECT '--- a histogram-only time series: both flavours as stored, value 0 ---';
SELECT timestamp, value, histogram
FROM timeSeriesSelector(ts_hist, 'm_hist', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3))
ORDER BY timestamp;

SELECT '--- custom buckets ---';
SELECT timestamp, histogram
FROM timeSeriesSelector(ts_hist, 'm_nhcb', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3));

SELECT '--- a float sample and a histogram sample of one time series ---';
SELECT timestamp, value, length(histogram)
FROM timeSeriesSelector(ts_hist, 'm_mixed', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3))
ORDER BY timestamp;

SELECT '--- integer counts are exact, the stale marker is bit-exact ---';
SELECT histogram[1].count_int, histogram[1].positive_values_int
FROM timeSeriesSelector(ts_hist, 'm_big', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3));
SELECT hex(reinterpretAsUInt64(histogram[1].sum))
FROM timeSeriesSelector(ts_hist, 'm_stale', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3));

SELECT '--- the time range applies to both kinds of samples ---';
SELECT countIf(empty(histogram)), countIf(notEmpty(histogram))
FROM timeSeriesSelector(ts_hist, '{job="a"}', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3));
SELECT countIf(empty(histogram)), countIf(notEmpty(histogram))
FROM timeSeriesSelector(ts_hist, '{job="a"}', toDateTime64('2026-01-01 00:00:30', 3), toDateTime64('2026-01-01 00:10:00', 3));

SELECT '--- prometheus queries fail on histogram samples until they support them ---';
-- A float-only time series of a table with histograms works.
SELECT value FROM prometheusQuery(ts_hist, 'm_float', toDateTime64('2026-01-01 00:01:30', 3));
-- The histogram sample of `m_mixed` is in the lookback window.
SELECT value FROM prometheusQuery(ts_hist, 'm_mixed', toDateTime64('2026-01-01 00:01:30', 3)); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
-- Only the float sample of `m_mixed` is in the lookback window.
SELECT value FROM prometheusQuery(ts_hist, 'm_mixed', toDateTime64('2026-01-01 00:00:30', 3));

SELECT '--- a table of version 5: three columns ---';
CREATE TABLE ts_v5 ENGINE = TimeSeries SETTINGS version = 5;
INSERT INTO ts_v5 (metric_name, tags, samples) VALUES ('m_float', {'job': 'a'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.5)]);
DESCRIBE TABLE timeSeriesSelector(ts_v5, 'm_float', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3));
SELECT timestamp, value
FROM timeSeriesSelector(ts_v5, 'm_float', toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-01 00:10:00', 3));

DROP TABLE ts_hist;
DROP TABLE ts_v5;
