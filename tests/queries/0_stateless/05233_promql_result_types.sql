-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
--
-- The table functions `prometheusQuery` and `prometheusQueryRange` return values as `Float64` and timestamps as `DateTime64`
-- with a scale of at least 3 regardless of the types of the samples in the TimeSeries table.
-- Values are kept in the table's type (for example `Float32`) only while reading raw samples, everything else is `Float64`.
-- The time zone of the timestamps comes from the `DateTime` or `DateTime64` arguments (the evaluation time, or the start
-- and the end of the range) if they specify the same time zone; otherwise it is the time zone of the timestamps in the table.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_f32;
DROP TABLE IF EXISTS ts_dt;
DROP TABLE IF EXISTS ts_u32;
DROP TABLE IF EXISTS ts_dt64_1;
DROP TABLE IF EXISTS ts_dt64_4;
DROP TABLE IF EXISTS ts_dt64_9;
DROP TABLE IF EXISTS ts_tokyo;

CREATE TABLE ts_f32 (samples Array(Tuple(DateTime64(3, 'UTC'), Float32))) ENGINE = TimeSeries;
CREATE TABLE ts_dt (samples Array(Tuple(DateTime('UTC'), Float32))) ENGINE = TimeSeries;
CREATE TABLE ts_u32 (samples Array(Tuple(UInt32, Float64))) ENGINE = TimeSeries;
CREATE TABLE ts_dt64_1 (samples Array(Tuple(DateTime64(1, 'UTC'), Float64))) ENGINE = TimeSeries;
CREATE TABLE ts_dt64_4 (samples Array(Tuple(DateTime64(4, 'UTC'), Float32))) ENGINE = TimeSeries;
CREATE TABLE ts_dt64_9 (samples Array(Tuple(DateTime64(9, 'UTC'), Float64))) ENGINE = TimeSeries;
CREATE TABLE ts_tokyo (samples Array(Tuple(DateTime64(3, 'Asia/Tokyo'), Float64))) ENGINE = TimeSeries;

INSERT INTO ts_f32 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime64(1000, 3, 'UTC'), 0.1), (toDateTime64(1015, 3, 'UTC'), 0.2), (toDateTime64(1030, 3, 'UTC'), 0.3)]);
INSERT INTO ts_dt (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime(1000, 'UTC'), 0.1), (toDateTime(1015, 'UTC'), 0.2), (toDateTime(1030, 'UTC'), 0.3)]);
INSERT INTO ts_u32 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(1000, 0.1), (1015, 0.2), (1030, 0.3)]);
INSERT INTO ts_dt64_1 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime64(1000, 1, 'UTC'), 0.1), (toDateTime64(1015.5, 1, 'UTC'), 0.2), (toDateTime64(1030, 1, 'UTC'), 0.3)]);
INSERT INTO ts_dt64_4 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime64(1000, 4, 'UTC'), 0.1), (toDateTime64(1015, 4, 'UTC'), 0.2), (toDateTime64(1030, 4, 'UTC'), 0.3)]);
INSERT INTO ts_dt64_9 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime64('1970-01-01 00:16:40.000000001', 9, 'UTC'), 0.1), (toDateTime64(1015, 9, 'UTC'), 0.2), (toDateTime64(1030, 9, 'UTC'), 0.3)]);
INSERT INTO ts_tokyo (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime64(1000, 3, 'Asia/Tokyo'), 1), (toDateTime64(1015, 3, 'Asia/Tokyo'), 2), (toDateTime64(1030, 3, 'Asia/Tokyo'), 3)]);

SELECT '-- Scalar and string results';
DESCRIBE prometheusQuery(ts_f32, '1 + 2', 1030);
DESCRIBE prometheusQuery(ts_f32, '"str"', 1030);

SELECT '-- Float32 values are widened to Float64 exactly';
SELECT * FROM prometheusQuery(ts_f32, 'up', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_f32, 'up * up', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_f32, 'max_over_time(up[1m])', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_f32, 'sum_over_time(up[1m])', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_f32, 'up[1m]', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQueryRange(ts_f32, 'up', 1000, 1030, 15) FORMAT TSVWithNamesAndTypes;

SELECT '-- DateTime and UInt32 timestamps are returned as DateTime64(3)';
SELECT * FROM prometheusQuery(ts_dt, 'up', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt, 'up[1m]', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt, 'rate(up[1m])', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_u32, 'up', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_u32, 'up[1m]', 1030) FORMAT TSVWithNamesAndTypes;

SELECT '-- Sub-second evaluation times and offsets work with DateTime and UInt32 timestamps';
SELECT * FROM prometheusQuery(ts_dt, 'up', 1030.5) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt, 'up[1m]', 1029.999) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt, 'up offset 500ms', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt, 'up[1m] offset 500ms', 1030.5) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_u32, 'rate(up[1m] offset 500ms)', 1030.5) FORMAT TSVWithNamesAndTypes;

SELECT '-- DateTime64(1) timestamps are returned as DateTime64(3), sub-second evaluation times and offsets keep the tenths of a second';
SELECT * FROM prometheusQuery(ts_dt64_1, 'up[1m]', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt64_1, 'up', 1015.5) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt64_1, 'up[1m] offset 500ms', 1030.5) FORMAT TSVWithNamesAndTypes;

SELECT '-- Offsets keep the scale of DateTime64(4)';
SELECT * FROM prometheusQuery(ts_dt64_4, 'up[1m] offset 1ms', 1030.001) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt64_4, 'last_over_time(up[1m] offset 1ms)', 1030.001) FORMAT TSVWithNamesAndTypes;

SELECT '-- DateTime64(9) keeps nanoseconds in the results and in the evaluation time';
SELECT * FROM prometheusQuery(ts_dt64_9, 'up[1m]', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt64_9, 'up[1m] offset 1ms', 1030.001) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt64_9, 'rate(up[1m])', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt64_9, 'time()', 1030.25) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQueryRange(ts_dt64_9, 'up', 1000, 1030, 15) FORMAT TSVWithNamesAndTypes;

SELECT '-- Functions of the evaluation time';
SELECT * FROM prometheusQuery(ts_f32, 'time()', 1030.25) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt, 'minute(vector(time()))', 1030.25) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQueryRange(ts_dt, 'minute()', 1000, 1030, 15) FORMAT TSVWithNamesAndTypes;

SELECT '-- Arguments without a time zone: the time zone of the table is used';
DESCRIBE prometheusQuery(ts_tokyo, 'up', 1030);
DESCRIBE prometheusQuery(ts_dt, 'up', toDateTime64(1030, 3));
DESCRIBE prometheusQueryRange(ts_u32, 'up', toDateTime(1000), toDateTime64(1030, 3), 15);

SELECT '-- Arguments with the same time zone: that time zone is used';
DESCRIBE prometheusQuery(ts_dt, 'up', toDateTime(1030, 'Asia/Tokyo'));
DESCRIBE prometheusQuery(ts_u32, 'up', toDateTime64(1030, 3, 'Asia/Tokyo'));
DESCRIBE prometheusQuery(ts_tokyo, 'up[1m]', toDateTime64(1030, 3, 'UTC'));
DESCRIBE prometheusQueryRange(ts_dt, 'up', toDateTime64(1000, 3, 'Asia/Tokyo'), toDateTime64(1030, 3, 'Asia/Tokyo'), 15);
DESCRIBE prometheusQueryRange(ts_dt, 'up', toDateTime64(1000, 3, 'Asia/Tokyo'), 1030, 15);
DESCRIBE prometheusQueryRange(ts_u32, 'up', 1000, toDateTime(1030, 'Asia/Tokyo'), 15);
SELECT * FROM prometheusQuery(ts_dt, 'up', toDateTime(1030, 'Asia/Tokyo')) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQueryRange(ts_tokyo, 'up', toDateTime64(1000, 3, 'UTC'), toDateTime64(1030, 3, 'UTC'), 15) FORMAT TSVWithNamesAndTypes;

SELECT '-- Arguments with different time zones: the time zone of the table is used';
DESCRIBE prometheusQueryRange(ts_dt, 'up', toDateTime64(1000, 3, 'Asia/Tokyo'), toDateTime64(1030, 3, 'Europe/Amsterdam'), 15);
DESCRIBE prometheusQueryRange(ts_u32, 'up', toDateTime64(1000, 3, 'Asia/Tokyo'), toDateTime64(1030, 3, 'Europe/Amsterdam'), 15);

DROP TABLE ts_f32;
DROP TABLE ts_dt;
DROP TABLE ts_u32;
DROP TABLE ts_dt64_1;
DROP TABLE ts_dt64_4;
DROP TABLE ts_dt64_9;
DROP TABLE ts_tokyo;
