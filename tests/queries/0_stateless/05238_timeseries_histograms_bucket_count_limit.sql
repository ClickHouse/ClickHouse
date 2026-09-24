-- A bucket count of an integer histogram must fit Int64: Prometheus keeps bucket counts as int64 deltas, so a larger count
-- couldn't be returned by remote read. The total count and the zero count are uint64 in Prometheus and aren't limited.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_hist;
CREATE TABLE ts_hist ENGINE = TimeSeries;

SELECT '--- a bucket count above the maximum Int64 is rejected ---';
INSERT INTO ts_hist (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('m', {'a': 'b'}, [toDateTime64('2026-01-01 00:00:00', 3)], [9223372036854775808], [1], [[(0, 1)]], [[9223372036854775808]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.negative_spans, histograms.negative_values_int)
    VALUES ('m', {'a': 'b'}, [toDateTime64('2026-01-01 00:00:00', 3)], [9223372036854775808], [1], [[(0, 1)]], [[9223372036854775808]]); -- { serverError INCORRECT_DATA }

SELECT '--- the maximum Int64 is accepted ---';
INSERT INTO ts_hist (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('m', {'a': 'b'}, [toDateTime64('2026-01-01 00:00:00', 3)], [9223372036854775807], [1], [[(0, 1)]], [[9223372036854775807]]);

SELECT '--- the total count may exceed the maximum Int64 when every bucket fits ---';
INSERT INTO ts_hist (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_spans, histograms.positive_values_int)
    VALUES ('m', {'a': 'b'}, [toDateTime64('2026-01-01 00:01:00', 3)], [18446744073709551614], [1], [[(0, 2)]], [[9223372036854775807, 9223372036854775807]]);

SELECT count_int, positive_values_int FROM timeSeriesHistograms(ts_hist) ORDER BY timestamp;

DROP TABLE ts_hist;
