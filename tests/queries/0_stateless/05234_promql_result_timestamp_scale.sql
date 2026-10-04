-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
--
-- The table functions `prometheusQuery` and `prometheusQueryRange` return timestamps as `DateTime64` with a scale of at least 3:
-- a TimeSeries table storing `DateTime64(1)` timestamps gives `DateTime64(3)` results, while tables with a higher scale keep it.
-- Sub-second evaluation times and offsets keep their precision in the results.
-- Tables storing timestamps as `DateTime`, `UInt32` or `DateTime64(3)` and the time zone of the results are covered by
-- 05234_promql_result_timestamp_type; the types of the values in the results by 05233_promql_result_value_type.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_dt64_1;
DROP TABLE IF EXISTS ts_dt64_4;
DROP TABLE IF EXISTS ts_dt64_9;

CREATE TABLE ts_dt64_1 (samples Array(Tuple(DateTime64(1, 'UTC'), Float64))) ENGINE = TimeSeries;
CREATE TABLE ts_dt64_4 (samples Array(Tuple(DateTime64(4, 'UTC'), Float32))) ENGINE = TimeSeries;
CREATE TABLE ts_dt64_9 (samples Array(Tuple(DateTime64(9, 'UTC'), Float64))) ENGINE = TimeSeries;

INSERT INTO ts_dt64_1 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime64(1000, 1, 'UTC'), 0.1), (toDateTime64(1015.5, 1, 'UTC'), 0.2), (toDateTime64(1030, 1, 'UTC'), 0.3)]);
INSERT INTO ts_dt64_4 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime64(1000, 4, 'UTC'), 0.1), (toDateTime64(1015, 4, 'UTC'), 0.2), (toDateTime64(1030, 4, 'UTC'), 0.3)]);
INSERT INTO ts_dt64_9 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime64('1970-01-01 00:16:40.000000001', 9, 'UTC'), 0.1), (toDateTime64(1015, 9, 'UTC'), 0.2), (toDateTime64(1030, 9, 'UTC'), 0.3)]);

SELECT '-- DateTime64(1) timestamps are returned as DateTime64(3), sub-second evaluation times and offsets keep the tenths of a second';
SELECT * FROM prometheusQuery(ts_dt64_1, 'up[1m]', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt64_1, 'up', 1015.5) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt64_1, 'up[1m] offset 500ms', 1030.5) FORMAT TSVWithNamesAndTypes;

SELECT '-- Offsets keep the scale of DateTime64(4)';
SELECT * FROM prometheusQuery(ts_dt64_4, 'up[1m] offset 1ms', 1030.001) FORMAT TSVWithNamesAndTypes;
DESCRIBE prometheusQuery(ts_dt64_4, 'last_over_time(up[1m] offset 1ms)', 1030.001);

SELECT '-- DateTime64(9) keeps nanoseconds in the results and in the evaluation time';
SELECT * FROM prometheusQuery(ts_dt64_9, 'up[1m]', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_dt64_9, 'up[1m] offset 1ms', 1030.001) FORMAT TSVWithNamesAndTypes;
DESCRIBE prometheusQuery(ts_dt64_9, 'rate(up[1m])', 1030);
DESCRIBE prometheusQuery(ts_dt64_9, 'time()', 1030.25);
SELECT * FROM prometheusQueryRange(ts_dt64_9, 'up', 1000, 1030, 15) FORMAT TSVWithNamesAndTypes;

DROP TABLE ts_dt64_1;
DROP TABLE ts_dt64_4;
DROP TABLE ts_dt64_9;
