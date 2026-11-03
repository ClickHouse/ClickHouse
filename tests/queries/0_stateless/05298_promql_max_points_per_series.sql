-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- `promql_max_points_per_series` limits only the Prometheus HTTP API, so the prometheusQueryRange table function is not limited.

SET enable_time_series_table = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

SELECT length(samples) FROM prometheusQueryRange(ts, 'vector(1)', 0, 11001, 1);
SELECT length(samples) FROM prometheusQueryRange(ts, 'vector(1)', 0, 20000, 1) SETTINGS promql_max_points_per_series = 10;

DROP TABLE ts;
