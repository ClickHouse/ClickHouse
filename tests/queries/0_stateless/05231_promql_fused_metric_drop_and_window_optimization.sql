-- Tags: no-fasttest, no-parallel-replicas
-- Tests fused metric drop optimization and subquery elimination in PromQL range queries.

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;
SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS prometheus;

CREATE TABLE prometheus ENGINE = TimeSeries;

INSERT INTO prometheus (metric_name, tags, time_series) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(100, 3), 1.0), (toDateTime64(110, 3), 2.0), (toDateTime64(120, 3), 4.0), (toDateTime64(130, 3), 8.0)]),
    ('m', map('host', 'h2'), [(toDateTime64(100, 3), 10.0), (toDateTime64(110, 3), 20.0), (toDateTime64(120, 3), 40.0), (toDateTime64(130, 3), 80.0)]),
    ('n', map('host', 'h1'), [(toDateTime64(100, 3), 5.0), (toDateTime64(110, 3), 15.0), (toDateTime64(120, 3), 25.0), (toDateTime64(130, 3), 35.0)]);

SELECT '-- exact metric name rate range query';
SELECT tags, time_series FROM prometheusQueryRange('prometheus', 'rate(m[20])', 100, 130, 10) ORDER BY ALL;

SELECT '-- exact metric name increase range query';
SELECT tags, time_series FROM prometheusQueryRange('prometheus', 'increase(m[20])', 100, 130, 10) ORDER BY ALL;

SELECT '-- exact metric name rate with offset';
SELECT tags, time_series FROM prometheusQueryRange('prometheus', 'rate(m[20] offset 10)', 110, 130, 10) ORDER BY ALL;

SELECT '-- rate with downstream arithmetic';
SELECT tags, time_series FROM prometheusQueryRange('prometheus', 'rate(m[20]) + 1', 100, 130, 10) ORDER BY ALL;

SELECT '-- instant query';
SELECT tags, timestamp, value FROM prometheusQuery('prometheus', 'rate(m[20])', 130) ORDER BY ALL;

SET prefer_column_name_to_alias = 1;
SELECT '-- under prefer_column_name_to_alias = 1';
SELECT tags, time_series FROM prometheusQueryRange('prometheus', 'rate(m[20])', 100, 130, 10) ORDER BY ALL;
SELECT tags, time_series FROM prometheusQueryRange('prometheus', 'predict_linear(m[20], 10)', 100, 130, 10) ORDER BY ALL;
SELECT tags, time_series FROM prometheusQueryRange('prometheus', 'quantile_over_time(0.5, m[20])', 100, 130, 10) ORDER BY ALL;
SET prefer_column_name_to_alias = 0;

SELECT '-- non-exact metric selector with duplicate series collision throws exception';
SELECT count() FROM prometheusQueryRange('prometheus', 'rate({host="h1"}[20])', 100, 130, 10); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- non-exact metric selector under prefer_column_name_to_alias = 1 throws exception';
SET prefer_column_name_to_alias = 1;
SELECT count() FROM prometheusQueryRange('prometheus', 'rate({host="h1"}[20])', 100, 130, 10); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SET prefer_column_name_to_alias = 0;

SELECT '-- explain plan verifies single aggregation step when metric name is exact';
SELECT countIf(explain LIKE '%Aggregating%') AS aggregating_steps,
       countIf(explain LIKE '%any(timeSeriesRateToGrid%') AS has_redundant_array_aggregation
FROM (EXPLAIN SELECT * FROM prometheusQueryRange('prometheus', 'rate(m[20])', 100, 130, 10));

SELECT countIf(explain LIKE '%Aggregating%') AS aggregating_steps,
       countIf(explain LIKE '%any(timeSeriesLinearRegressionToGrid%') AS has_redundant_array_aggregation
FROM (EXPLAIN SELECT * FROM prometheusQueryRange('prometheus', 'predict_linear(m[20], 10)', 100, 130, 10));

SELECT countIf(explain LIKE '%Aggregating%') AS aggregating_steps,
       countIf(explain LIKE '%any(timeSeriesQuantileToGrid%') AS has_redundant_array_aggregation
FROM (EXPLAIN SELECT * FROM prometheusQueryRange('prometheus', 'quantile_over_time(0.5, m[20])', 100, 130, 10));

DROP TABLE prometheus;
