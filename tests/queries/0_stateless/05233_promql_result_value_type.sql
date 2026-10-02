-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
--
-- The table functions `prometheusQuery` and `prometheusQueryRange` return values as `Float64` regardless of the type
-- of the values in the TimeSeries table. Values are kept in the table's type (for example `Float32`) only while reading
-- raw samples, everything else is `Float64`. The evaluation time keeps its sub-second precision in `time()` and in
-- the functions of the evaluation time.
-- The types of the timestamps in the results are covered by 05234_promql_result_timestamp_type.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_f32;

CREATE TABLE ts_f32 (samples Array(Tuple(DateTime64(3, 'UTC'), Float32))) ENGINE = TimeSeries;

INSERT INTO ts_f32 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime64(1000, 3, 'UTC'), 0.1), (toDateTime64(1015, 3, 'UTC'), 0.2), (toDateTime64(1030, 3, 'UTC'), 0.3)]);

SELECT '-- Scalar and string results';
DESCRIBE prometheusQuery(ts_f32, '1 + 2', 1030);
DESCRIBE prometheusQuery(ts_f32, '"str"', 1030);

SELECT '-- Float32 values are widened to Float64 exactly';
DESCRIBE prometheusQuery(ts_f32, 'up', 1030);
SELECT * FROM prometheusQuery(ts_f32, 'up * up', 1030) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_f32, 'sum_over_time(up[1m])', 1030) FORMAT TSVWithNamesAndTypes;
DESCRIBE prometheusQuery(ts_f32, 'up[1m]', 1030);
SELECT * FROM prometheusQueryRange(ts_f32, 'up', 1000, 1030, 15) FORMAT TSVWithNamesAndTypes;

SELECT '-- Functions of the evaluation time';
SELECT * FROM prometheusQuery(ts_f32, 'time()', 1030.25) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQuery(ts_f32, 'minute(vector(time()))', 1030.25) FORMAT TSVWithNamesAndTypes;
SELECT * FROM prometheusQueryRange(ts_f32, 'minute()', 1000, 1030, 15) FORMAT TSVWithNamesAndTypes;

DROP TABLE ts_f32;
