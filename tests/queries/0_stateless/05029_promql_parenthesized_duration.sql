-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

SELECT '--- parenthesized range and subquery durations ---';
SELECT count() FROM prometheusQuery('ts', 'rate(up[(5m)])', 1000);
SELECT count() FROM prometheusQuery('ts', 'rate(up[(5m):(1m)])', 1000);

SELECT '--- parenthesized offsets ---';
SELECT count() FROM prometheusQuery('ts', 'up offset (5m)', 1000);
SELECT count() FROM prometheusQuery('ts', 'up offset (-5m)', 1000);

DROP TABLE ts;
