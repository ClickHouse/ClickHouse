-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A histogram returned by a query is a float histogram, as in Prometheus, so it can be inserted back into a TimeSeries table.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_src;
DROP TABLE IF EXISTS ts_dst_selector;

CREATE TABLE ts_src ENGINE = TimeSeries SETTINGS store_native_histograms = 1;

-- Two integer histograms with exact integer counts; flags = 4 is the counter reset hint NO.
INSERT INTO ts_src (metric_name, tags, histograms) VALUES
    ('h', map('job', 'a'), [
        (toDateTime64(100, 3), 0, 0, 0.001, 5, 7.5, 1, [(0, 2), (1, 1)], [2, 1, 1], [], [], [], 5, 1, [2, 1, 1], []),
        (toDateTime64(110, 3), 4, 0, 0.001, 10, 25.5, 2, [(0, 2), (1, 1)], [3, 2, 3], [], [], [], 10, 2, [3, 2, 3], [])]);

SELECT '-- instant selector: the result keeps its counts and reset hint, and is marked as a float histogram';
CREATE TABLE ts_dst_selector ENGINE = TimeSeries SETTINGS store_native_histograms = 1;
INSERT INTO ts_dst_selector (metric_name, tags, histograms)
    SELECT 'h', map('job', 'a'), [(timestamp, h.1, h.2, h.3, h.4, h.5, h.6, h.7, h.8, h.9, h.10, h.11, h.12, h.13, h.14, h.15)]
    FROM (SELECT timestamp, assumeNotNull(histogram) AS h FROM prometheusQuery(ts_src, 'h', 120));
SELECT timestamp, flags, `schema`, zero_threshold, count, sum, zero_count, positive_spans, positive_values, negative_spans, negative_values,
    custom_values, count_int, zero_count_int, positive_values_int, negative_values_int
FROM timeSeriesHistograms(ts_dst_selector);
DROP TABLE ts_dst_selector;

DROP TABLE ts_src;
