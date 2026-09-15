-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
--
-- The table functions `prometheusQuery` and `prometheusQueryRange` return values as `Float64` and timestamps as `DateTime64`
-- with a scale of at least 3 regardless of the types of the samples in the TimeSeries table.
-- Values are kept in the table's type (for example `Float32`) only while reading raw samples, everything else is `Float64`.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_f32;
DROP TABLE IF EXISTS ts_dt;
DROP TABLE IF EXISTS ts_u32;
DROP TABLE IF EXISTS ts_dt64_4;

CREATE TABLE ts_f32 (samples Array(Tuple(DateTime64(3, 'UTC'), Float32))) ENGINE = TimeSeries;
CREATE TABLE ts_dt (samples Array(Tuple(DateTime('UTC'), Float32))) ENGINE = TimeSeries;
CREATE TABLE ts_u32 (samples Array(Tuple(UInt32, Float64))) ENGINE = TimeSeries;
CREATE TABLE ts_dt64_4 (samples Array(Tuple(DateTime64(4, 'UTC'), Float32))) ENGINE = TimeSeries;

INSERT INTO ts_f32 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime64(1000, 3, 'UTC'), 0.1), (toDateTime64(1015, 3, 'UTC'), 0.2), (toDateTime64(1030, 3, 'UTC'), 0.3)]);
INSERT INTO ts_dt (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime(1000, 'UTC'), 0.1), (toDateTime(1015, 'UTC'), 0.2), (toDateTime(1030, 'UTC'), 0.3)]);
INSERT INTO ts_u32 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(1000, 0.1), (1015, 0.2), (1030, 0.3)]);
INSERT INTO ts_dt64_4 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime64(1000, 4, 'UTC'), 0.1), (toDateTime64(1015, 4, 'UTC'), 0.2), (toDateTime64(1030, 4, 'UTC'), 0.3)]);

SELECT '-- Result types do not depend on the types in the table';
DESCRIBE prometheusQuery(ts_f32, '1 + 2', 1030);
DESCRIBE prometheusQuery(ts_f32, '"str"', 1030);
DESCRIBE prometheusQuery(ts_f32, 'up', 1030);
DESCRIBE prometheusQuery(ts_f32, 'up[1m]', 1030);
DESCRIBE prometheusQueryRange(ts_f32, 'up', 1000, 1030, 15);
DESCRIBE prometheusQuery(ts_dt, 'up', 1030);
DESCRIBE prometheusQuery(ts_dt, 'up[1m]', 1030);
DESCRIBE prometheusQuery(ts_u32, 'up', 1030);
DESCRIBE prometheusQuery(ts_u32, 'up[1m]', 1030);

SELECT '-- DateTime64 with a scale greater than 3 keeps the scale';
DESCRIBE prometheusQuery(ts_dt64_4, 'up', 1030);
DESCRIBE prometheusQuery(ts_dt64_4, 'up[1m]', 1030);

SELECT '-- Float32 values are widened to Float64 exactly';
SELECT * FROM prometheusQuery(ts_f32, 'up', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_f32, '-up', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_f32, 'up + up', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_f32, 'up > bool 0.2', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_f32, 'last_over_time(up[1m])', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_f32, 'max_over_time(up[1m])', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_f32, 'sum_over_time(up[1m])', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_f32, 'increase(up[1m])', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_f32, 'up[1m]', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQueryRange(ts_f32, 'up', 1000, 1030, 15) FORMAT TSVWithNamesAndTypes;

SELECT '-- DateTime and UInt32 timestamps are returned as DateTime64(3)';
SELECT * FROM prometheusQuery(ts_dt, 'up', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt, 'up[1m]', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt, 'rate(up[1m])', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_u32, 'up', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_u32, 'up[1m]', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_u32, 'rate(up[1m])', 1030) FORMAT TSVWithNamesAndTypes;

SELECT '-- Sub-second evaluation times and offsets work with DateTime and UInt32 timestamps';
SELECT * FROM prometheusQuery(ts_dt, 'up', 1030.5) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt, 'up[1m]', 1029.999) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt, 'up[30s]', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt, 'up offset 500ms', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt, 'up[1m] offset 500ms', 1030.5) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_u32, 'up[1m] offset 500ms', 1030.5) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_u32, 'rate(up[1m] offset 500ms)', 1030.5) FORMAT TSVWithNamesAndTypes;

SELECT '-- Offsets keep the scale of DateTime64(4)';
SELECT * FROM prometheusQuery(ts_dt64_4, 'up[1m] offset 1ms', 1030.001) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt64_4, 'last_over_time(up[1m] offset 1ms)', 1030.001) FORMAT TSVWithNamesAndTypes;

SELECT '-- Functions of the evaluation time';
SELECT * FROM prometheusQuery(ts_f32, 'time()', 1030.25) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_f32, 'minute()', 1030.25) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt, 'minute(vector(time()))', 1030.25) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQueryRange(ts_dt, 'minute()', 1000, 1030, 15) FORMAT TSVWithNamesAndTypes;

DROP TABLE ts_f32;
DROP TABLE ts_dt;
DROP TABLE ts_u32;
DROP TABLE ts_dt64_4;
