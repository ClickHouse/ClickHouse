-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Tests the PromQL function `timestamp`. For a vector selector (optionally with `offset` or `@`) it returns
-- the timestamp of the selected sample, for any other expression it returns the evaluation time.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_f32;

CREATE TABLE ts ENGINE = TimeSeries;

-- The series `stale` ends with a stale marker (the NaN with the payload 0x7ff0000000000002) at 150.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('m', map('job', 'a'), [(toDateTime64(60.5, 3), 1.0), (toDateTime64(120, 3), -2.0), (toDateTime64(180, 3), 3.0)]),
    ('stale', map('job', 'b'), [(toDateTime64(100, 3), 5.0), (toDateTime64(150, 3), reinterpretAsFloat64(toUInt64(9218868437227405314)))]);

SELECT 'timestamp of the last sample, the metric name is dropped';
SELECT * FROM prometheusQuery(ts, 'timestamp(m)', 200) FORMAT TSVWithNamesAndTypes;

SELECT 'offset and @ select an older sample, its own timestamp is returned';
SELECT tags, value FROM prometheusQuery(ts, 'timestamp(m offset 1m)', 200);
SELECT tags, value FROM prometheusQuery(ts, 'timestamp(m @ 100)', 200);

SELECT 'any other expression returns the evaluation time';
SELECT tags, value FROM prometheusQuery(ts, 'timestamp(abs(m))', 200.25);

SELECT 'range query';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts, 'timestamp(m)', 100, 200, 50);
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts, 'timestamp(m offset 1m)', 100, 200, 50);
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts, 'timestamp(m @ 100)', 100, 200, 50);
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts, 'timestamp(abs(m))', 100, 200, 50);

SELECT 'a stale marker hides the series as it does for the selector itself';
SELECT tags, value FROM prometheusQuery(ts, 'timestamp(stale)', 140);
SELECT tags, value FROM prometheusQuery(ts, 'timestamp(stale)', 200);
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples) FROM prometheusQueryRange(ts, 'timestamp(stale)', 100, 200, 50);

SELECT 'Float32 values do not affect the precision of the timestamps';
CREATE TABLE ts_f32 (samples Array(Tuple(DateTime64(3, 'UTC'), Float32))) ENGINE = TimeSeries;
INSERT INTO ts_f32 (metric_name, tags, samples) VALUES ('up', map('job', 'j'), [(toDateTime64(1764498605.125, 3, 'UTC'), 1)]);
SELECT * FROM prometheusQuery(ts_f32, 'timestamp(up)', 1764498610) FORMAT TSVWithNamesAndTypes;

DROP TABLE ts;
DROP TABLE ts_f32;
