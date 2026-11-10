-- Tags: no-fasttest
-- PromQL needs ANTLR4, which is disabled in the fast-test build.

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;
SET empty_result_for_aggregation_by_empty_set = 1;

DROP TABLE IF EXISTS prometheus;
CREATE TABLE prometheus ENGINE = TimeSeries;

SELECT *
FROM prometheusQuery('prometheus', 'absent(nonexistent{job="api"})', 130)
ORDER BY tags;

SELECT *
FROM prometheusQuery('prometheus', 'scalar(nonexistent)', 130)
ORDER BY value;

SELECT getSetting('empty_result_for_aggregation_by_empty_set');

DROP TABLE prometheus;
