-- Tags: no-fasttest
-- no-fasttest: the PromQL grammar requires ANTLR4 which is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

-- One sample a minute from 1000020 to 1000620.
INSERT INTO ts (metric_name, tags, samples)
SELECT 'm', map('l', 'a'), arrayMap(i -> (toDateTime64(1000020 + 60 * i, 3), 1.0), range(11));

SELECT 'subquery without step, default step 15s:';
SELECT value FROM prometheusQuery(ts, 'count_over_time(m[5m:])', 1000620);
SELECT value FROM prometheusQuery(ts, 'sum_over_time(m[5m:])', 1000620);
SELECT tags, arrayMap(x -> x.2, samples) FROM prometheusQueryRange(ts, 'count_over_time(m[5m:])', 1000320, 1000620, 60) ORDER BY ALL;

SELECT 'subquery without step, promql_default_subquery_step = 60:';
SELECT value FROM prometheusQuery(ts, 'count_over_time(m[5m:])', 1000620) SETTINGS promql_default_subquery_step = 60;
SELECT value FROM prometheusQuery(ts, 'sum_over_time(m[5m:])', 1000620) SETTINGS promql_default_subquery_step = 60;
SELECT tags, arrayMap(x -> x.2, samples) FROM prometheusQueryRange(ts, 'count_over_time(m[5m:])', 1000320, 1000620, 60) ORDER BY ALL SETTINGS promql_default_subquery_step = 60;

SELECT 'an explicit subquery step wins:';
SELECT value FROM prometheusQuery(ts, 'count_over_time(m[5m:30s])', 1000620) SETTINGS promql_default_subquery_step = 60;

SELECT 'instant selector two minutes after the last sample, default lookback 5m:';
SELECT tags, value FROM prometheusQuery(ts, 'm', 1000740);
SELECT tags, samples FROM prometheusQueryRange(ts, 'm', 1000620, 1001020, 60) ORDER BY ALL;

SELECT 'promql_lookback_delta = 60:';
SELECT count() FROM prometheusQuery(ts, 'm', 1000740) SETTINGS promql_lookback_delta = 60;
SELECT tags, value FROM prometheusQuery(ts, 'm', 1000660) SETTINGS promql_lookback_delta = 60;
SELECT tags, samples FROM prometheusQueryRange(ts, 'm', 1000620, 1001020, 60) ORDER BY ALL SETTINGS promql_lookback_delta = 60;

SELECT 'fractional values are rounded up to milliseconds:';
SELECT count() FROM prometheusQuery(ts, 'm', fromUnixTimestamp64Milli(1000624099)) SETTINGS promql_lookback_delta = 4.1;
SELECT count() FROM prometheusQuery(ts, 'm', fromUnixTimestamp64Milli(1000624099)) SETTINGS promql_lookback_delta = 4.099;
SELECT count() FROM prometheusQuery(ts, 'm', 1000620) SETTINGS promql_lookback_delta = 0.0005;
SELECT count() FROM prometheusQuery(ts, 'm', fromUnixTimestamp64Milli(1000620001)) SETTINGS promql_lookback_delta = 0.0005;
SELECT value FROM prometheusQuery(ts, 'count_over_time(m[5m:])', fromUnixTimestamp64Milli(1000620500)) SETTINGS promql_default_subquery_step = 1.001;
SELECT value FROM prometheusQuery(ts, 'count_over_time(m[5m:])', fromUnixTimestamp64Milli(1000620500)) SETTINGS promql_default_subquery_step = 1;

SELECT 'zero means the default:';
SELECT count() FROM prometheusQuery(ts, 'm', 1000740) SETTINGS promql_lookback_delta = 0;
SELECT count() FROM prometheusQuery(ts, 'm', 1000920) SETTINGS promql_lookback_delta = 0;
SELECT value FROM prometheusQuery(ts, 'count_over_time(m[5m:])', 1000620) SETTINGS promql_default_subquery_step = 0;

SELECT 'negative values are rejected:';
SELECT * FROM prometheusQuery(ts, 'm', 1000740) SETTINGS promql_lookback_delta = -60; -- { serverError BAD_ARGUMENTS }
SELECT * FROM prometheusQuery(ts, 'count_over_time(m[5m:])', 1000620) SETTINGS promql_default_subquery_step = -15; -- { serverError BAD_ARGUMENTS }

SELECT 'the promql dialect:';
SET promql_table = 'ts';
SET dialect = 'promql';
SET promql_evaluation_time = 1000740;
m;
SET promql_lookback_delta = 60;
m;
SET promql_evaluation_time = 1000620;
SET promql_default_subquery_step = 60;
count_over_time(m[5m:]);
SET dialect = 'clickhouse';

DROP TABLE ts;
