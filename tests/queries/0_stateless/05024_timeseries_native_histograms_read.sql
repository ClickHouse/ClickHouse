-- Test: reading the outer `histograms` column of a TimeSeries table with a "histograms" target
-- (the data and the expected values mirror 05024_timeseries_native_histograms.sql).

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_src;
DROP TABLE IF EXISTS ts_dst;
DROP TABLE IF EXISTS ts_plain;

CREATE TABLE ts_src ENGINE = TimeSeries SETTINGS store_native_histograms = 1;
CREATE TABLE ts_dst ENGINE = TimeSeries SETTINGS store_native_histograms = 1;

INSERT INTO ts_src (metric_name, tags, histograms) VALUES
    ('test_histogram_seconds', map('job', 'test'), [('2024-01-01 00:00:01.000', 0, 3, 0.001, 10, 25.5, 2, [(0, 2), (1, 1)], [3, 2, 3], [], [], [], 10, 2, [3, 2, 3], [2]), ('2024-01-01 00:00:02.000', 1, -53, 0, 6.5, 12.25, 0, [(0, 2)], [4.5, 2], [], [], [0.1, 0.5], 0, 0, [], [])]);
INSERT INTO ts_src (metric_name, tags, time_series) VALUES
    ('test_gauge', map('job', 'test'), [('2024-01-01 00:00:01.000', 1.5)]);

-- The order of the elements within an array is not guaranteed, so the arrays are normalized with arraySort().

SELECT '-- SELECT histograms returns the ingested histograms';
SELECT arraySort(histograms) AS h FROM ts_src ORDER BY h;

SELECT '-- a mixed read joins the histograms with metric_name and tags by the series id';
SELECT metric_name, tags, arraySort(histograms) FROM ts_src ORDER BY metric_name;
SELECT metric_name, tags['job'], length(histograms) FROM ts_src ORDER BY metric_name;

SELECT '-- time_series and histograms together: a series gets an empty array for the kind of samples it does not have';
SELECT metric_name, arraySort(time_series), arraySort(histograms) FROM ts_src ORDER BY metric_name;

SELECT '-- SELECT * includes the histograms column';
SELECT metric_name, tags, arraySort(time_series), arraySort(histograms), (metric_family, type, unit, help)
    FROM (SELECT * FROM ts_src) ORDER BY metric_name;

SELECT '-- INSERT ... SELECT * round-trips the histograms into another histogram-enabled table';
INSERT INTO ts_dst SELECT * FROM ts_src;
SELECT metric_name, tags, arraySort(time_series), arraySort(histograms) FROM ts_dst ORDER BY metric_name;
SELECT timestamp, flags, schema, zero_threshold, count, sum, zero_count, positive_spans, positive_values, negative_spans, negative_values, custom_values,
    count_int, zero_count_int, positive_values_int, negative_values_int
    FROM timeSeriesHistograms(ts_dst) ORDER BY timestamp;
SELECT count() FROM timeSeriesSamples(ts_dst);
SELECT min(min_time), max(max_time) FROM timeSeriesTags(ts_dst) WHERE metric_name = 'test_histogram_seconds';

SELECT '-- a table without the histograms target has no histograms column';
CREATE TABLE ts_plain ENGINE = TimeSeries;
SELECT histograms FROM ts_plain; -- { serverError UNKNOWN_IDENTIFIER }
SELECT count() FROM ts_plain;

DROP TABLE ts_src;
DROP TABLE ts_dst;
DROP TABLE ts_plain;
