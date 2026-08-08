-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Tests the PromQL aggregation operators topk/bottomk/limitk end to end: the streaming plan built around the
-- timeSeriesSelect{TopK,BottomK,LimitK}Groups aggregate functions (the functions themselves are tested
-- in 04811_aggregate_functions_timeseries_topk_bottomk_limitk).

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

-- 4 series of the metric `m`: hosts h1, h2 in dc=a and hosts h3, h4 in dc=b.
-- Series h4 has a gap at timestamps 110 and 120, and series h1 ends with a NaN sample at timestamp 140.
INSERT INTO prometheus (metric_name, tags, time_series) VALUES
    ('m', map('host', 'h1', 'dc', 'a'), [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), 10), (toDateTime64(120, 3), 4), (toDateTime64(130, 3), 1), (toDateTime64(140, 3), nan)]),
    ('m', map('host', 'h2', 'dc', 'a'), [(toDateTime64(100, 3), 2), (toDateTime64(110, 3), 20), (toDateTime64(120, 3), 3), (toDateTime64(130, 3), 2)]),
    ('m', map('host', 'h3', 'dc', 'b'), [(toDateTime64(100, 3), 3), (toDateTime64(110, 3), 5), (toDateTime64(120, 3), 2), (toDateTime64(130, 3), 3)]),
    ('m', map('host', 'h4', 'dc', 'b'), [(toDateTime64(100, 3), 4), (toDateTime64(130, 3), 4)]);

SELECT '-- topk(2), range';
SELECT * FROM prometheusQueryRange('prometheus', 'topk(2, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags;
SELECT '-- bottomk(2), range';
SELECT * FROM prometheusQueryRange('prometheus', 'bottomk(2, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags;
SELECT '-- topk(1) by (dc), range';
SELECT * FROM prometheusQueryRange('prometheus', 'topk(1, last_over_time(m[10])) by (dc)', 100, 130, 10) ORDER BY tags;
SELECT '-- bottomk(1) without (host), range';
SELECT * FROM prometheusQueryRange('prometheus', 'bottomk(1, last_over_time(m[10])) without (host)', 100, 130, 10) ORDER BY tags;
SELECT '-- limitk(2), range';
SELECT * FROM prometheusQueryRange('prometheus', 'limitk(2, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags;
SELECT '-- limitk(10): k > number of series, range';
SELECT * FROM prometheusQueryRange('prometheus', 'limitk(10, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags;
SELECT '-- topk(0) and clamped negative k, range';
SELECT * FROM prometheusQueryRange('prometheus', 'topk(0, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags;
SELECT * FROM prometheusQueryRange('prometheus', 'topk(-3, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags;
SELECT '-- topk(10): k > number of series, range';
SELECT * FROM prometheusQueryRange('prometheus', 'topk(10, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags;
SELECT '-- topk(time() / 10 - 9): k depending on the evaluation time, range';
SELECT * FROM prometheusQueryRange('prometheus', 'topk(time() / 10 - 9, last_over_time(m[10]))', 100, 130, 10) ORDER BY tags;
SELECT '-- topk(2), instant';
SELECT * FROM prometheusQuery('prometheus', 'topk(2, m)', 130) ORDER BY tags;
SELECT '-- topk(scalar(count(m)) - 2): k from a scalar subquery, instant';
SELECT * FROM prometheusQuery('prometheus', 'topk(scalar(count(m)) - 2, m)', 130) ORDER BY tags;
SELECT '-- bottomk(2), instant';
SELECT * FROM prometheusQuery('prometheus', 'bottomk(2, m)', 130) ORDER BY tags;
SELECT '-- limitk(2), instant';
SELECT * FROM prometheusQuery('prometheus', 'limitk(2, m)', 130) ORDER BY tags;
SELECT '-- NaN is chosen after any non-NaN value, instant at 140 where h1 is NaN';
SELECT * FROM prometheusQuery('prometheus', 'topk(3, m)', 140) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'topk(4, m)', 140) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'bottomk(1, m)', 140) ORDER BY tags;
SELECT '-- topk of a metric which matches no series';
SELECT * FROM prometheusQuery('prometheus', 'topk(2, nonexistent)', 130) ORDER BY tags;
SELECT '-- k = +Inf is an error';
SELECT * FROM prometheusQuery('prometheus', 'topk(+Inf, m)', 130); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'topk(1 / 0, m)', 130); -- { serverError CANNOT_CONVERT_TYPE }

DROP TABLE prometheus;
