-- Tags: no-fasttest, no-tsan, long
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag long: the flaky check runs a changed test in many parallel copies, which makes the many `info` queries exceed 180 seconds.
-- Tag no-tsan: an `info` query needs more stack than the 5% that TSan allows.

-- PromQL info(): choosing the newest info series, conflicts and stale series (a part of 05308_promql_info_function).

SET enable_time_series_table = 1;
SET session_timezone = 'UTC';

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
