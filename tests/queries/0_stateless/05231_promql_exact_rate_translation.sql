-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- The setting `promql_exact_rate` makes the PromQL translation pass the parameter `exact_rate` to the aggregate functions
-- implementing `rate`, `increase` and `delta` (tested in 05231_promql_exact_rate).

SET enable_time_series_table = 1;
SET enable_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('up', map('instance', 'host1'), [(toDateTime64(1699999940, 3), 10.0), (toDateTime64(1699999960, 3), 20.0), (toDateTime64(1700000000, 3), 30.0)]);

SELECT '--- instant query, default extrapolation ---';
SELECT value FROM prometheusQuery(ts, 'rate(up[80s])', 1700000000);
SELECT value FROM prometheusQuery(ts, 'increase(up[80s])', 1700000000);
SELECT value FROM prometheusQuery(ts, 'delta(up[80s])', 1700000000);

SELECT '--- instant query, promql_exact_rate = 1 ---';
SET promql_exact_rate = 1;
SELECT value FROM prometheusQuery(ts, 'rate(up[80s])', 1700000000);
SELECT value FROM prometheusQuery(ts, 'increase(up[80s])', 1700000000);
SELECT value FROM prometheusQuery(ts, 'delta(up[80s])', 1700000000);

SELECT '--- range query, promql_exact_rate = 1: the previous sample is used inside the range ---';
SELECT arrayMap(x -> (toUnixTimestamp64Second(x.1), x.2), samples) FROM prometheusQueryRange(ts, 'increase(up[40s])', 1699999960, 1700000000, 20);

SELECT '--- range query, promql_exact_rate = 0 ---';
SET promql_exact_rate = 0;
SELECT arrayMap(x -> (toUnixTimestamp64Second(x.1), x.2), samples) FROM prometheusQueryRange(ts, 'increase(up[40s])', 1699999960, 1700000000, 20);

SELECT '--- promql dialect ---';
SET promql_table = 'ts', promql_evaluation_time = 1700000000, session_timezone = 'UTC';
SET dialect = 'promql';
increase(up[80s]);
SET promql_exact_rate = 1;
increase(up[80s]);
SET dialect = 'clickhouse';

DROP TABLE ts;
