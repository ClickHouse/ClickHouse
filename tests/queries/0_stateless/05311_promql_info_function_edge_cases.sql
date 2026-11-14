-- Tags: no-fasttest, no-tsan, long
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag long: the flaky check runs a changed test in many parallel copies, which makes the many `info` queries exceed 180 seconds.
-- Tag no-tsan: an `info` query needs more stack than the 5% that TSan allows.

-- PromQL info(): edge cases with the results of Prometheus v3.5.0 on the same samples (a part of 05308_promql_info_function).

SET enable_time_series_table = 1;
SET session_timezone = 'UTC';

SELECT '-- two info series with the same identifying labels at the same time';
DROP TABLE IF EXISTS ts_e;
CREATE TABLE ts_e ENGINE = TimeSeries;
INSERT INTO ts_e (metric_name, tags, samples) VALUES
    ('metric', map('instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 1), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 1)]),
    ('metric2', map('instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 7)]),
    ('target_info', map('instance', 'a', 'job', '1', 'v', 'x'), [(toDateTime64(0, 3), 1), (toDateTime64(600, 3), 1)]),
    ('target_info', map('instance', 'a', 'job', '1', 'v', 'y'), [(toDateTime64(600, 3), 1)]),
    ('other', map('instance', 'c', 'job', '3'), [(toDateTime64(0, 3), 1), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 1)]);
SELECT 'info(metric) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_e, 'info(metric)', 0, 600, 300) ORDER BY tags; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT 'info(metric2) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_e, 'info(metric2)', 0, 600, 300) ORDER BY tags;
SELECT 'info(other) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_e, 'info(other)', 0, 600, 300) ORDER BY tags;
DROP TABLE ts_e;

SELECT '-- a label selector whose every matcher matches the empty string';
DROP TABLE IF EXISTS ts_f;
CREATE TABLE ts_f ENGINE = TimeSeries;
INSERT INTO ts_f (metric_name, tags, samples) VALUES
    ('metric', map('instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 1), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 1)]),
    ('target_info', map('data', 'info', 'instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 1), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 1)]);
SELECT 'info(metric, {__name__!="metric"}) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_f, 'info(metric, {__name__!="metric"})', 0, 600, 300) ORDER BY tags;
SELECT 'info(metric, {__name__!="metric", data=~".*"}) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_f, 'info(metric, {__name__!="metric", data=~".*"})', 0, 600, 300) ORDER BY tags;
SELECT 'info(metric, {}) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_f, 'info(metric, {})', 0, 600, 300) ORDER BY tags;
SELECT * FROM prometheusQuery(ts_f, '{}', 0); -- { serverError CANNOT_PARSE_PROMQL_QUERY }
DROP TABLE ts_f;

SELECT '-- an info series appearing and disappearing within the range, a base series with only one identifying label';
DROP TABLE IF EXISTS ts_g;
CREATE TABLE ts_g ENGINE = TimeSeries;
INSERT INTO ts_g (metric_name, tags, samples) VALUES
    ('metric', map('instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 0), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 2)]),
    ('target_info', map('data', 'mid', 'instance', 'a', 'job', '1'), [(toDateTime64(300, 3), 1)]),
    ('job_only', map('job', '1'), [(toDateTime64(0, 3), 3), (toDateTime64(300, 3), 3), (toDateTime64(600, 3), 3)]),
    ('target_info', map('data', 'job only', 'job', '1'), [(toDateTime64(0, 3), 1), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 1)]);
SELECT 'info(metric) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_g, 'info(metric)', 0, 600, 300) ORDER BY tags;
SELECT 'info(job_only) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_g, 'info(job_only)', 0, 600, 300) ORDER BY tags;
SELECT 'info({__name__=~"metric|job_only"}) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_g, 'info({__name__=~"metric|job_only"})', 0, 600, 300) ORDER BY tags;
DROP TABLE ts_g;
SELECT 'label values with special characters are matched literally';
CREATE TABLE ts_s ENGINE = TimeSeries;
INSERT INTO ts_s (metric_name, tags, samples) VALUES
    ('metric', map('instance', 'a.b', 'job', 'j"1'), [(toDateTime64(0, 3), 1)]),
    ('target_info', map('data', 'x', 'instance', 'a.b', 'job', 'j"1'), [(toDateTime64(0, 3), 1)]),
    ('target_info', map('data', 'wrong', 'instance', 'aXb', 'job', 'j"1'), [(toDateTime64(0, 3), 1)]);
SELECT tags FROM prometheusQuery(ts_s, 'info(metric)', 0);
DROP TABLE ts_s;

