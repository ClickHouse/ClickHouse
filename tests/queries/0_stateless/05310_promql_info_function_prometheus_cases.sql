-- Tags: no-fasttest, no-tsan, long
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag long: the flaky check runs a changed test in many parallel copies, which makes the many `info` queries exceed 180 seconds.
-- Tag no-tsan: an `info` query needs more stack than the 5% that TSan allows.

-- PromQL info(): cases from promql/info_test.go with the results of Prometheus v3.5.0 on the same samples (a part of 05308_promql_info_function).

SET enable_time_series_table = 1;
SET session_timezone = 'UTC';

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
