-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Tests binary operators applied to two aggregations of the same expression, e.g. `sum(m) - max(m)`,
-- which are calculated in a single aggregation, and the cases which must keep using a join.

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
    ('m', map('host', 'h4', 'dc', 'b'), [(toDateTime64(100, 3), 4), (toDateTime64(130, 3), 4)]),
    ('n', map('host', 'h1', 'dc', 'a'), [(toDateTime64(100, 3), 7), (toDateTime64(110, 3), 8), (toDateTime64(120, 3), 9), (toDateTime64(130, 3), 10)]);

SELECT '-- sum(m) - max(m), instant';
SELECT * FROM prometheusQuery('prometheus', 'sum(m) - max(m)', 130) ORDER BY tags;
SELECT '-- sum(m) / count(m), instant';
SELECT * FROM prometheusQuery('prometheus', 'sum(m) / count(m)', 130) ORDER BY tags;
SELECT '-- sum(m) % max(m), instant';
SELECT * FROM prometheusQuery('prometheus', 'sum(m) % max(m)', 130) ORDER BY tags;
SELECT '-- sum(m) * group(m), instant';
SELECT * FROM prometheusQuery('prometheus', 'sum(m) * group(m)', 130) ORDER BY tags;
SELECT '-- max(m) ^ min(m), instant';
SELECT * FROM prometheusQuery('prometheus', 'max(m) ^ min(m)', 130) ORDER BY tags;
SELECT '-- avg(m) + stddev(m), instant';
SELECT * FROM prometheusQuery('prometheus', 'avg(m) + stddev(m)', 130) ORDER BY tags;
SELECT '-- atan2(sum(m), max(m)), instant';
SELECT * FROM prometheusQuery('prometheus', 'sum(m) atan2 max(m)', 130) ORDER BY tags;

SELECT '-- sum by (dc) (m) - min by (dc) (m), range';
SELECT * FROM prometheusQueryRange('prometheus', 'sum by (dc) (m) - min by (dc) (m)', 100, 140, 10) ORDER BY tags;
SELECT '-- max without (host) (m) - min without (host) (m), range';
SELECT * FROM prometheusQueryRange('prometheus', 'max without (host) (m) - min without (host) (m)', 100, 140, 10) ORDER BY tags;
SELECT '-- stddev by (dc) (m) + stdvar by (dc) (m), range';
SELECT * FROM prometheusQueryRange('prometheus', 'stddev by (dc) (m) + stdvar by (dc) (m)', 100, 140, 10) ORDER BY tags;
SELECT '-- labels are dropped the same way with and without a shared aggregation argument, instant';
SELECT * FROM prometheusQuery('prometheus', 'sum by (dc, host) (m) - max by (dc, host) (m)', 130) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'sum by (dc, host) (m) - max by (dc, host) (n)', 130) ORDER BY tags;

SELECT '-- the shared argument is a range function, range';
SELECT * FROM prometheusQueryRange('prometheus', 'sum(rate(m[20])) - max(rate(m[20]))', 110, 140, 10) ORDER BY tags;
SELECT '-- nested aggregation pairs, instant';
SELECT * FROM prometheusQuery('prometheus', '(sum(m) - max(m)) / sum(m)', 130) ORDER BY tags;
SELECT '-- the shared argument matches no series, instant';
SELECT * FROM prometheusQuery('prometheus', 'sum(nonexistent) - max(nonexistent)', 130) ORDER BY tags;

SELECT '-- not shared: different arguments, instant';
SELECT * FROM prometheusQuery('prometheus', 'sum(m) - max(n)', 130) ORDER BY tags;
SELECT '-- not shared: different grouping, instant';
SELECT * FROM prometheusQuery('prometheus', 'sum by (dc) (m) - max by (host) (m)', 130) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'sum without (host) (m) - max by (dc) (m)', 130) ORDER BY tags;
SELECT '-- not shared: the arguments differ by an offset, instant';
SELECT * FROM prometheusQuery('prometheus', 'sum(m) - max(m offset 10)', 130) ORDER BY tags;
SELECT '-- not shared: comparison operator, instant';
SELECT * FROM prometheusQuery('prometheus', 'sum(m) > max(m)', 130) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'sum(m) > bool max(m)', 130) ORDER BY tags;
SELECT '-- not shared: logical operator, instant';
SELECT * FROM prometheusQuery('prometheus', 'sum(m) unless max(m)', 130) ORDER BY tags;
SELECT '-- not shared: group_left, instant';
SELECT * FROM prometheusQuery('prometheus', 'sum by (dc) (m) - on (dc) group_left () max by (dc) (m)', 130) ORDER BY tags;
SELECT '-- not shared: by (__name__) keeps the metric name, instant';
SELECT * FROM prometheusQuery('prometheus', 'sum by (__name__) (m) - max by (__name__) (m)', 130) ORDER BY tags;
SELECT '-- not shared: one side is not an aggregation, instant';
SELECT * FROM prometheusQuery('prometheus', 'sum(m) - m', 130) ORDER BY tags;
SELECT '-- not shared: quantile is not a one-argument aggregation, instant';
SELECT * FROM prometheusQuery('prometheus', 'quantile(0.5, m) - max(m)', 130) ORDER BY tags;

DROP TABLE prometheus;
