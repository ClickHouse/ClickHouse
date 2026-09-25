-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A Prometheus stale marker (the NaN 0x7ff0000000000002) and a quiet NaN share a timestamp.
-- Of two NaNs the greater bit pattern wins, so the quiet NaN is kept whatever the order of the samples.

SET enable_time_series_table = 1;

SELECT 'PromQL instant selector: the series is present with NaN';
DROP TABLE IF EXISTS ts_marker_first;
DROP TABLE IF EXISTS ts_nan_first;
CREATE TABLE ts_marker_first ENGINE = TimeSeries;
CREATE TABLE ts_nan_first ENGINE = TimeSeries;
INSERT INTO ts_marker_first (metric_name, tags, samples) VALUES
    ('m', map(), [(toDateTime64(95, 3), reinterpretAsFloat64(0x7ff0000000000002)), (toDateTime64(95, 3), nan)]);
INSERT INTO ts_nan_first (metric_name, tags, samples) VALUES
    ('m', map(), [(toDateTime64(95, 3), nan), (toDateTime64(95, 3), reinterpretAsFloat64(0x7ff0000000000002))]);
SELECT count(), countIf(isNaN(value)) FROM prometheusQuery(ts_marker_first, 'm', 100);
SELECT count(), countIf(isNaN(value)) FROM prometheusQuery(ts_nan_first, 'm', 100);
DROP TABLE ts_marker_first;
DROP TABLE ts_nan_first;
