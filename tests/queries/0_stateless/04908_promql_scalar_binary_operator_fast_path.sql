-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Tests binary operators between two vectors aggregated to a single series with no tags
-- (e.g. `sum(a) / sum(b)`): such shapes are transpiled to a simplified SQL query without
-- the vector-matching machinery, and this test pins down that the results (including
-- PromQL emptiness semantics and NaN/Inf propagation) stay the same.

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

-- `m1` has 3 series, `m2` has 2 series, `mz` sums to zero, `mn` contains a NaN sample.
-- `mlate` has samples only around timestamp 1000, i.e. it is empty at timestamp 130 at query time
-- (while still known to the transpiler, unlike a completely nonexistent metric).
INSERT INTO prometheus (metric_name, tags, time_series) VALUES
    ('m1', map('host', 'h1', 'dc', 'a'), [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), 10), (toDateTime64(120, 3), 4), (toDateTime64(130, 3), 1)]),
    ('m1', map('host', 'h2', 'dc', 'a'), [(toDateTime64(100, 3), 2), (toDateTime64(110, 3), 20), (toDateTime64(120, 3), 3), (toDateTime64(130, 3), 2)]),
    ('m1', map('host', 'h3', 'dc', 'b'), [(toDateTime64(100, 3), 3), (toDateTime64(110, 3), 5), (toDateTime64(120, 3), 2), (toDateTime64(130, 3), 3)]),
    ('m2', map('host', 'h1', 'dc', 'a'), [(toDateTime64(100, 3), 4), (toDateTime64(110, 3), 8), (toDateTime64(120, 3), 1), (toDateTime64(130, 3), 4)]),
    ('m2', map('host', 'h3', 'dc', 'b'), [(toDateTime64(100, 3), 8), (toDateTime64(110, 3), 4), (toDateTime64(120, 3), 5), (toDateTime64(130, 3), 8)]),
    ('mz', map('host', 'h1'), [(toDateTime64(100, 3), 5), (toDateTime64(130, 3), 5)]),
    ('mz', map('host', 'h2'), [(toDateTime64(100, 3), -5), (toDateTime64(130, 3), -5)]),
    ('mn', map('host', 'h1'), [(toDateTime64(100, 3), nan), (toDateTime64(130, 3), nan)]),
    ('mlate', map('host', 'h1'), [(toDateTime64(1000, 3), 7)]);

SELECT '-- sum(m1) / sum(m2), instant';
SELECT * FROM prometheusQuery('prometheus', 'sum(m1) / sum(m2)', 130) ORDER BY tags;
SELECT '-- 1 - sum(m1) / sum(m2), instant';
SELECT * FROM prometheusQuery('prometheus', '1 - sum(m1) / sum(m2)', 130) ORDER BY tags;
SELECT '-- sum(m1) / sum(m2) * 100, instant';
SELECT * FROM prometheusQuery('prometheus', 'sum(m1) / sum(m2) * 100', 130) ORDER BY tags;
SELECT '-- avg(m1) - max(m2), min(m1) + count(m2), instant';
SELECT * FROM prometheusQuery('prometheus', 'avg(m1) - max(m2)', 130) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'min(m1) + count(m2)', 130) ORDER BY tags;
SELECT '-- nested: sum(m1) / sum(m2) / sum(m2), instant';
SELECT * FROM prometheusQuery('prometheus', 'sum(m1) / sum(m2) / sum(m2)', 130) ORDER BY tags;
SELECT '-- sum(m1) % sum(m2), sum(m1) ^ sum(m2), instant';
SELECT * FROM prometheusQuery('prometheus', 'sum(m1) % sum(m2)', 130) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'sum(m1) ^ sum(m2)', 130) ORDER BY tags;
SELECT '-- sum(m1) / sum(m2), range';
SELECT * FROM prometheusQueryRange('prometheus', 'sum(m1) / sum(m2)', 100, 130, 10) ORDER BY tags;

SELECT '-- division by zero: +Inf, -Inf and NaN';
SELECT * FROM prometheusQuery('prometheus', 'sum(m1) / sum(mz)', 130) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', '(0 - sum(m1)) / sum(mz)', 130) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'sum(mz) / sum(mz)', 130) ORDER BY tags;
SELECT '-- NaN operand propagates';
SELECT * FROM prometheusQuery('prometheus', 'sum(mn) / sum(m1)', 130) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'sum(m1) / sum(mn)', 130) ORDER BY tags;

SELECT '-- one side is a metric with no such name: result is empty';
SELECT * FROM prometheusQuery('prometheus', 'sum(no_such_metric) / sum(m1)', 130) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'sum(m1) / sum(no_such_metric)', 130) ORDER BY tags;
SELECT '-- both sides have no such name: result is empty';
SELECT * FROM prometheusQuery('prometheus', 'sum(no_such_metric) / sum(no_such_metric_2)', 130) ORDER BY tags;
SELECT '-- one side has no samples at the evaluation time: result is empty';
SELECT * FROM prometheusQuery('prometheus', 'sum(mlate) / sum(m1)', 130) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'sum(m1) / sum(mlate)', 130) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', '1 - sum(m1) / sum(mlate)', 130) ORDER BY tags;

SELECT '-- comparison operators: filter and bool';
SELECT * FROM prometheusQuery('prometheus', 'sum(m1) > sum(m2)', 130) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'sum(m1) < sum(m2)', 130) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'sum(m1) > bool sum(m2)', 130) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'sum(m1) < bool sum(m2)', 130) ORDER BY tags;

SELECT '-- vector matching by labels keeps the general path';
SELECT * FROM prometheusQuery('prometheus', 'sum by (host) (m1) / sum by (host) (m2)', 130) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'sum by (host, dc) (m1) / on(host) sum by (host) (m2)', 130) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'sum by (host, dc) (m1) / ignoring(dc) sum by (host) (m2)', 130) ORDER BY tags;
SELECT '-- group_left keeps the general path';
SELECT * FROM prometheusQuery('prometheus', 'm1 / on(host) group_left sum by (host) (m2)', 130) ORDER BY tags;
SELECT '-- on() matching between two aggregated vectors';
SELECT * FROM prometheusQuery('prometheus', 'sum(m1) / on() sum(m2)', 130) ORDER BY tags;

DROP TABLE prometheus;
