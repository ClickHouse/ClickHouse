-- Tags: no-fasttest, no-tsan, long
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag long: the flaky check runs a changed test in many parallel copies, which makes the many `info` queries exceed 180 seconds.
-- Tag no-tsan: an `info` query needs more stack than the 5% that TSan allows.

-- PromQL info(): the @ and offset modifiers, invalid arguments (a part of 05308_promql_info_function).

SET enable_time_series_table = 1;
SET session_timezone = 'UTC';

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

SELECT '-- info(metric @ 300, {data=~".*"} @ 300) from 300 to 900 step 300';
CREATE TABLE ts_pin ENGINE = TimeSeries;
INSERT INTO ts_pin (metric_name, tags, samples) VALUES
    ('metric', map('instance', 'a', 'job', '1'), [(toDateTime64(300, 3), 1)]),
    ('target_info', map('instance', 'a', 'job', '1', 'data', 'new'), [(toDateTime64(300, 3), 1)]);
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts_pin, 'info(metric @ 300, {data=~".*"} @ 300)', 300, 900, 300) ORDER BY tags;
DROP TABLE ts_pin;

SELECT '-- invalid arguments';
CREATE TABLE ts_args ENGINE = TimeSeries;
SELECT * FROM prometheusQuery(ts_args, 'info()', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts_args, 'info(up, {a="b"}, {c="d"})', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts_args, 'info(up, 1)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts_args, 'info(up, abs({a="b"}))', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts_args, 'info(1)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
DROP TABLE ts_args;
