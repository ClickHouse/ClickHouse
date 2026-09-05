-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

INSERT INTO prometheus (metric_name, tags, time_series) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(100, 3), 1)]),
    ('m', map('host', 'h2'), [(toDateTime64(100, 3), 2)]);

SELECT 'sum by() aggregates all series into one group';
SELECT count() AS series_count, sum(value) AS value
FROM prometheusQuery('prometheus', 'sum by() (m)', 100);

SELECT 'sum by() uses the constant empty group';
SELECT countIf(explain LIKE '%timeSeriesRemoveAllTagsExcept%') = 0
FROM (EXPLAIN SELECT * FROM prometheusQuery('prometheus', 'sum by() (m)', 100));

DROP TABLE prometheus;
