-- Tags: no-fasttest, no-replicated-database
-- ^^ PromQL needs ANTLR4, which is disabled in the fast-test build. The experimental TimeSeries table engine does not
-- round-trip through DatabaseReplicated.

-- A selector with an equality matcher for `__name__` has one metric name across all its series. Dropping that tag is
-- injective, so `dropMetricName` can project the groups directly instead of aggregating and checking duplicates.

SET allow_experimental_time_series_table = 1;
SET allow_experimental_time_series_aggregate_functions = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS prometheus;
CREATE TABLE prometheus ENGINE = TimeSeries;

INSERT INTO prometheus (metric_name, tags, time_series) VALUES
    ('m', map('job', 'api'), [(toDateTime64(100, 3), 1.0), (toDateTime64(110, 3), 2.0), (toDateTime64(120, 3), 3.0)]),
    ('m', map('job', 'worker'), [(toDateTime64(100, 3), 2.0), (toDateTime64(110, 3), 4.0), (toDateTime64(120, 3), 8.0)]),
    ('n', map('job', 'backend'), [(toDateTime64(100, 3), 3.0), (toDateTime64(110, 3), 6.0), (toDateTime64(120, 3), 9.0)]);

SELECT '-- fixed metric name: projection-only drop';
SELECT countIf(explain LIKE '%timeSeriesThrowDuplicateSeriesIf%') = 0
FROM (EXPLAIN SELECT * FROM prometheusQuery('prometheus', 'rate(m[20])', 120));

SELECT count() FROM prometheusQuery('prometheus', 'rate(m[20])', 120);

SELECT '-- fixed metric name survives offset';
SELECT countIf(explain LIKE '%timeSeriesThrowDuplicateSeriesIf%') = 0
FROM (EXPLAIN SELECT * FROM prometheusQuery('prometheus', 'rate(m[20] offset 10)', 130));

SELECT '-- a non-equality metric matcher keeps the duplicate check';
SELECT countIf(explain LIKE '%timeSeriesThrowDuplicateSeriesIf%') > 0
FROM (EXPLAIN SELECT * FROM prometheusQuery('prometheus', 'rate({__name__=~"m|n"}[20])', 120));

SELECT '-- rewriting __name__ keeps the duplicate check conservative';
SELECT countIf(explain LIKE '%timeSeriesThrowDuplicateSeriesIf%') > 0
FROM (EXPLAIN SELECT * FROM prometheusQuery('prometheus', 'abs(label_replace(m, "__name__", "renamed", "", ""))', 120));

DROP TABLE prometheus;
