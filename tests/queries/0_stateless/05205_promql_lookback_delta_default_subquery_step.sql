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

SELECT 'exact microseconds on a DateTime64(6) table:';
DROP TABLE IF EXISTS ts6;
CREATE TABLE ts6 (samples Array(Tuple(DateTime64(6, 'UTC'), Float64))) ENGINE = TimeSeries;
INSERT INTO ts6 (metric_name, tags, samples) VALUES ('m', {'l': 'a'}, [(toDateTime64(1000620, 6, 'UTC'), 1)]);
SELECT count() FROM prometheusQuery(ts6, 'm', fromUnixTimestamp64Micro(1000624099999)) SETTINGS promql_lookback_delta = 4.1;
SELECT count() FROM prometheusQuery(ts6, 'm', fromUnixTimestamp64Micro(1000624099999)) SETTINGS promql_lookback_delta = 4.099999;
SELECT samples FROM prometheusQuery(ts6, 'm[3s:]', fromUnixTimestamp64Micro(1000623500000)) SETTINGS promql_default_subquery_step = 1.001;
DROP TABLE ts6;

SELECT 'zero means the default:';
SELECT count() FROM prometheusQuery(ts, 'm', 1000740) SETTINGS promql_lookback_delta = 0;
SELECT count() FROM prometheusQuery(ts, 'm', 1000920) SETTINGS promql_lookback_delta = 0;
SELECT value FROM prometheusQuery(ts, 'count_over_time(m[5m:])', 1000620) SETTINGS promql_default_subquery_step = 0;

SELECT 'below 1 microsecond means the default:';
SELECT count() FROM prometheusQuery(ts, 'm', 1000740) SETTINGS promql_lookback_delta = 0.0000009;
SELECT value FROM prometheusQuery(ts, 'count_over_time(m[5m:])', 1000620) SETTINGS promql_default_subquery_step = 0.0000009;

SELECT 'negative values are rejected:';
SELECT * FROM prometheusQuery(ts, 'm', 1000740) SETTINGS promql_lookback_delta = -60; -- { serverError BAD_ARGUMENTS }
SELECT * FROM prometheusQuery(ts, 'count_over_time(m[5m:])', 1000620) SETTINGS promql_default_subquery_step = -15; -- { serverError BAD_ARGUMENTS }
SELECT * FROM prometheusQuery(ts, 'm', 1000740) SETTINGS promql_lookback_delta = -0.0000009; -- { serverError BAD_ARGUMENTS }
SELECT * FROM prometheusQuery(ts, 'count_over_time(m[5m:])', 1000620) SETTINGS promql_default_subquery_step = -0.0000009; -- { serverError BAD_ARGUMENTS }

SELECT 'a table created from prometheusQuery uses the settings of the query reading it:';
CREATE TABLE pq AS prometheusQuery(ts, 'm', 1000740);
CREATE TABLE pq_subquery AS prometheusQuery(ts, 'count_over_time(m[5m:])', 1000620);
SET promql_lookback_delta = 60;
CREATE TABLE pq_created_with_60 AS prometheusQuery(ts, 'm', 1000740);
SET promql_lookback_delta = 300;
SELECT count() FROM pq;
SELECT count() FROM pq SETTINGS promql_lookback_delta = 60;
SELECT count() FROM pq_created_with_60;
SELECT value FROM pq_subquery;
SELECT value FROM pq_subquery SETTINGS promql_default_subquery_step = 60;
DROP TABLE pq;
DROP TABLE pq_subquery;
DROP TABLE pq_created_with_60;

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
