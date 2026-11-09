-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- PromQL info(): the results match Prometheus 3.5 with --enable-feature=promql-experimental-functions.

SET enable_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('metric', map('instance', 'a', 'job', '1', 'label', 'value'), [(toDateTime64(0, 3), 0), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 2)]),
    ('metric_not_matching_target_info', map('instance', 'a', 'job', '2', 'label', 'value'), [(toDateTime64(0, 3), 0), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 2)]),
    ('metric_with_overlapping_label', map('instance', 'a', 'job', '1', 'label', 'value', 'data', 'base'), [(toDateTime64(0, 3), 0), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 2)]),
    ('target_info', map('instance', 'a', 'job', '1', 'data', 'info', 'another_data', 'another info'), [(toDateTime64(0, 3), 1), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 1)]),
    ('build_info', map('instance', 'a', 'job', '1', 'build_data', 'build'), [(toDateTime64(0, 3), 1), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 1)]),
    ('info', map('a', 'b'), [(toDateTime64(600, 3), 5)]);

SELECT '-- all data labels of target_info';
SELECT * FROM prometheusQueryRange(ts, 'info(metric)', 0, 600, 300) ORDER BY ALL;
SELECT '-- one data label';
SELECT * FROM prometheusQueryRange(ts, 'info(metric, {data=~".+"})', 0, 600, 300) ORDER BY ALL;
SELECT '-- no info series with the same identifying labels';
SELECT * FROM prometheusQueryRange(ts, 'info(metric_not_matching_target_info)', 0, 600, 300) ORDER BY ALL;
SELECT '-- a data label matcher not matching the empty string drops series without that label';
SELECT * FROM prometheusQueryRange(ts, 'info(metric, {non_existent=~".+"})', 0, 600, 300) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'info(metric, {non_existent=~".*"})', 0, 600, 300) ORDER BY ALL;
SELECT '-- labels of the series are kept';
SELECT * FROM prometheusQueryRange(ts, 'info(metric_with_overlapping_label)', 0, 600, 300) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'info(metric_with_overlapping_label, {data="info"})', 0, 600, 300) ORDER BY ALL;
SELECT '-- other info metrics';
SELECT * FROM prometheusQueryRange(ts, 'info(metric, {__name__="non_existent"})', 0, 600, 300) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'info(metric, {__name__="non_existent", data=~".+"})', 0, 600, 300) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'info(metric, {__name__="build_info"})', 0, 600, 300) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'info(metric, {__name__=~".+_info"})', 0, 600, 300) ORDER BY ALL;
SELECT '-- every matcher of the label selector matches the empty string';
SELECT * FROM prometheusQueryRange(ts, 'info(metric, {__name__=~".*"})', 0, 600, 300) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'info(metric, {__name__!="foo"})', 0, 600, 300) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'info(metric, {__name__=~"(|foo_info)"})', 0, 600, 300) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'info(metric, {__name__=~"(|build_info)"})', 0, 600, 300) ORDER BY ALL;
SELECT '-- info series are not enriched';
SELECT * FROM prometheusQueryRange(ts, 'info(build_info, {__name__=~".+_info", build_data=~".+"})', 0, 600, 300) ORDER BY ALL;
SELECT '-- the metric name dropped before info() stays dropped';
SELECT * FROM prometheusQueryRange(ts, 'info(floor(metric))', 0, 600, 300) ORDER BY ALL;
SELECT '-- a metric named info';
SELECT * FROM prometheusQuery(ts, 'info', 600);

DROP TABLE ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('metric', map('instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 0), (toDateTime64(120, 3), 2), (toDateTime64(180, 3), 3), (toDateTime64(240, 3), 4)]),
    ('target_info', map('instance', 'a', 'job', '1', 'version', 'old'), [(toDateTime64(0, 3), 1), (toDateTime64(120, 3), 1)]),
    ('target_info', map('instance', 'a', 'job', '1', 'version', 'new'), [(toDateTime64(180, 3), 1), (toDateTime64(240, 3), 1)]);

SELECT '-- the newest info series is used, with the lookback';
SELECT * FROM prometheusQueryRange(ts, 'info(metric)', 60, 240, 60) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'info(metric @ 60)', 60, 240, 60) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'info(metric offset 1m)', 60, 240, 60) ORDER BY ALL;
SELECT '-- the first selector of an aggregation is in its expression';
SELECT * FROM prometheusQueryRange(ts, 'info(topk(scalar(metric @ 120), metric))', 60, 240, 60) ORDER BY ALL;
SELECT '-- subqueries';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time(info(metric)[2m:1m])', 60, 240, 60) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'info(max_over_time(metric[2m:1m]))', 60, 240, 60) ORDER BY ALL;

DROP TABLE ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('metric', map('job', 'api'), [(toDateTime64(0, 3), 1)]),
    ('metric', map('instance', 'standalone'), [(toDateTime64(0, 3), 2)]),
    ('metric', map('job', 'api', 'instance', 'a'), [(toDateTime64(0, 3), 3)]),
    ('target_info', map('job', 'api', 'job_data', 'job-only'), [(toDateTime64(0, 3), 1)]),
    ('target_info', map('instance', 'standalone', 'instance_data', 'instance-only'), [(toDateTime64(0, 3), 1)]),
    ('target_info', map('job', 'api', 'instance', 'a', 'pair_data', 'api-a'), [(toDateTime64(0, 3), 1)]),
    ('build_info', map('job', 'api', 'instance', 'a', 'pair_data', 'other'), [(toDateTime64(0, 3), 1)]),
    ('dup_info', map('job', 'api', 'instance', 'a', 'v', '1'), [(toDateTime64(0, 3), 1)]),
    ('dup_info', map('job', 'api', 'instance', 'a', 'v', '2'), [(toDateTime64(0, 3), 1)]);

SELECT '-- info series must have every identifying label found on the series';
SELECT * FROM prometheusQuery(ts, 'info(metric)', 0) ORDER BY ALL;
SELECT * FROM prometheusQuery(ts, 'info(metric{instance="standalone"})', 0) ORDER BY ALL;
SELECT '-- different values of a data label from two info metrics';
SELECT * FROM prometheusQuery(ts, 'info(metric, {__name__=~"target_info|build_info"})', 0); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT '-- two series of one info metric with the same timestamp';
SELECT * FROM prometheusQuery(ts, 'info(metric, {__name__="dup_info"})', 0); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT '-- the second argument must be a label selector';
SELECT * FROM prometheusQuery(ts, 'info(metric, metric + 1)', 0); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

DROP TABLE ts;
CREATE TABLE ts ENGINE = TimeSeries;

-- 9218868437227405314 is 0x7ff0000000000002, the stale marker of Prometheus.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('metric', map('instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 0), (toDateTime64(60, 3), 1), (toDateTime64(120, 3), 2), (toDateTime64(180, 3), 3), (toDateTime64(240, 3), 4)]),
    ('target_info', map('instance', 'a', 'job', '1', 'version', 'v1'), [(toDateTime64(0, 3), 1), (toDateTime64(60, 3), 1), (toDateTime64(120, 3), reinterpretAsFloat64(toUInt64(9218868437227405314)))]),
    ('target_info', map('instance', 'a', 'job', '1', 'version', 'v2'), [(toDateTime64(240, 3), 1)]);

SELECT '-- a stale info series is not used';
SELECT * FROM prometheusQueryRange(ts, 'info(metric)', 0, 240, 60) ORDER BY ALL;

DROP TABLE ts;

-- More cases from promql/info_test.go and edge cases, with the results of Prometheus v3.5.0 on the same samples.

SELECT '-- info metrics joined on instance and job, data label matchers, custom __name__ (promql/info_test.go)';
DROP TABLE IF EXISTS ts_a;
CREATE TABLE ts_a ENGINE = TimeSeries;
INSERT INTO ts_a (metric_name, tags, samples) VALUES
    ('metric', map('instance', 'a', 'job', '1', 'label', 'value'), [(toDateTime64(0, 3), 0), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 2)]),
    ('metric_not_matching_target_info', map('instance', 'a', 'job', '2', 'label', 'value'), [(toDateTime64(0, 3), 0), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 2)]),
    ('metric_with_overlapping_label', map('data', 'base', 'instance', 'a', 'job', '1', 'label', 'value'), [(toDateTime64(0, 3), 0), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 2)]),
    ('target_info', map('another_data', 'another info', 'data', 'info', 'instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 1), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 1)]),
    ('build_info', map('build_data', 'build', 'instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 1), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 1)]),
    ('conflicting_meta', map('build_data', 'other', 'instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 1), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 1)]),
    ('nolabels', map('x', 'y'), [(toDateTime64(0, 3), 0), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 2)]),
    ('metric_withdata', map('instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 5), (toDateTime64(300, 3), 5), (toDateTime64(600, 3), 5)]),
    ('metric_withdata', map('data', 'info', 'instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 6), (toDateTime64(300, 3), 6), (toDateTime64(600, 3), 6)]);
SELECT 'info(metric, {data=~".+", non_existent=~".*"}) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_a, 'info(metric, {data=~".+", non_existent=~".*"})', 0, 600, 300) ORDER BY tags;
SELECT 'info(metric, {__name__="target_info"}) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_a, 'info(metric, {__name__="target_info"})', 0, 600, 300) ORDER BY tags;
SELECT 'info(nolabels) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_a, 'info(nolabels)', 0, 600, 300) ORDER BY tags;
SELECT 'info(nolabels, {__name__="build_info"}) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_a, 'info(nolabels, {__name__="build_info"})', 0, 600, 300) ORDER BY tags;
SELECT 'info(metric_withdata) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_a, 'info(metric_withdata)', 0, 600, 300) ORDER BY tags; -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT 'info(metric_withdata, {data=~".+"}) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_a, 'info(metric_withdata, {data=~".+"})', 0, 600, 300) ORDER BY tags;
SELECT 'info(metric, {data=~".*"} offset 5m) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_a, 'info(metric, {data=~".*"} offset 5m)', 0, 600, 300) ORDER BY tags;
SELECT 'info(metric, {"build_info"}) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_a, 'info(metric, {"build_info"})', 0, 600, 300) ORDER BY tags;
SELECT 'info(metric_with_overlapping_label, {data=~".+"}) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_a, 'info(metric_with_overlapping_label, {data=~".+"})', 0, 600, 300) ORDER BY tags;
SELECT 'info({__name__=~"metric|metric_not_matching_target_info"}) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_a, 'info({__name__=~"metric|metric_not_matching_target_info"})', 0, 600, 300) ORDER BY tags;
SELECT 'info(target_info) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_a, 'info(target_info)', 0, 600, 300) ORDER BY tags;
SELECT 'info(target_info, {data=~".+"}) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_a, 'info(target_info, {data=~".+"})', 0, 600, 300) ORDER BY tags;
SELECT 'info(metric * 2) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_a, 'info(metric * 2)', 0, 600, 300) ORDER BY tags;
SELECT 'info(metric) at 300';
SELECT tags, value FROM prometheusQuery(ts_a, 'info(metric)', 300) ORDER BY tags;
SELECT 'info(metric, {data=~".+"}) at 300';
SELECT tags, value FROM prometheusQuery(ts_a, 'info(metric, {data=~".+"})', 300) ORDER BY tags;
DROP TABLE ts_a;

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

SELECT '-- a label selector without a matcher that doesn't match the empty string';
DROP TABLE IF EXISTS ts_f;
CREATE TABLE ts_f ENGINE = TimeSeries;
INSERT INTO ts_f (metric_name, tags, samples) VALUES
    ('metric', map('instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 1), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 1)]),
    ('target_info', map('data', 'info', 'instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 1), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 1)]);
SELECT 'info(metric, {__name__!="metric"}) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_f, 'info(metric, {__name__!="metric"})', 0, 600, 300) ORDER BY tags;
SELECT 'info(metric, {__name__!="metric", data=~".*"}) from 0 to 600 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_f, 'info(metric, {__name__!="metric", data=~".*"})', 0, 600, 300) ORDER BY tags;
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

SELECT '-- @ and offset limit the time range of the info series';
DROP TABLE IF EXISTS ts_h;
CREATE TABLE ts_h ENGINE = TimeSeries;
INSERT INTO ts_h (metric_name, tags, samples) VALUES
    ('metric', map('instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 0), (toDateTime64(300, 3), 1), (toDateTime64(600, 3), 2), (toDateTime64(900, 3), 3)]),
    ('target_info', map('data', 'old', 'instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 1)]),
    ('target_info', map('data', 'new', 'instance', 'a', 'job', '1'), [(toDateTime64(300, 3), 1), (toDateTime64(600, 3), 1), (toDateTime64(900, 3), 1)]);
SELECT 'info(metric @ 300) from 0 to 900 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_h, 'info(metric @ 300)', 0, 900, 300) ORDER BY tags;
SELECT 'info(metric @ 300) from 300 to 900 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_h, 'info(metric @ 300)', 300, 900, 300) ORDER BY tags;
SELECT 'info(metric offset 5m) from 300 to 900 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_h, 'info(metric offset 5m)', 300, 900, 300) ORDER BY tags;
SELECT 'info(metric @ 300, {data=~".+"}) from 300 to 900 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_h, 'info(metric @ 300, {data=~".+"})', 300, 900, 300) ORDER BY tags;
SELECT 'info(sum by (instance, job) (metric @ 300)) from 300 to 900 step 300';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_h, 'info(sum by (instance, job) (metric @ 300))', 300, 900, 300) ORDER BY tags;
SELECT 'info(metric offset 5m) at 600';
SELECT tags, value FROM prometheusQuery(ts_h, 'info(metric offset 5m)', 600) ORDER BY tags;
SELECT 'info(metric offset 4m) at 600';
SELECT tags, value FROM prometheusQuery(ts_h, 'info(metric offset 4m)', 600) ORDER BY tags;
SELECT 'info(metric) at 600';
SELECT tags, value FROM prometheusQuery(ts_h, 'info(metric)', 600) ORDER BY tags;
DROP TABLE ts_h;

SELECT '-- invalid arguments';
CREATE TABLE ts_args ENGINE = TimeSeries;
SELECT * FROM prometheusQuery(ts_args, 'info()', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts_args, 'info(up, {a="b"}, {c="d"})', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts_args, 'info(up, 1)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts_args, 'info(up, abs({a="b"}))', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts_args, 'info(1)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
DROP TABLE ts_args;
