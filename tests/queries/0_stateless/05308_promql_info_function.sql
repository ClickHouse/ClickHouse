-- Tags: no-fasttest, no-tsan, long
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag long: the flaky check runs a changed test in many parallel copies, which makes the many `info` queries exceed 180 seconds.
-- Tag no-tsan: an `info` query needs more stack than the 5% that TSan allows.

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
