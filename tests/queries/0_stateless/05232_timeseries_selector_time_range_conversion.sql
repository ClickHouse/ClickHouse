-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
--
-- The arguments `min_time` and `max_time` of table function `timeSeriesSelector` have at least millisecond precision even if
-- the timestamps in the table are `DateTime` or `UInt32`: the time range is converted to the type of the table with the bounds
-- rounded towards the inside of the range, and it is intersected with the range of the timestamp type of the table, because
-- `DateTime` and `UInt32` can't hold timestamps before 1970 or after 2106. The returned columns keep the types of the table.
-- Tables storing timestamps as `DateTime64` are covered by 05232_timeseries_selector_time_range_scale.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_dt;
DROP TABLE IF EXISTS ts_u32;

CREATE TABLE ts_dt (samples Array(Tuple(DateTime('UTC'), Float64))) ENGINE = TimeSeries;
CREATE TABLE ts_u32 (samples Array(Tuple(UInt32, Float32))) ENGINE = TimeSeries;
CREATE TABLE ts_dt_v6 (samples Array(Tuple(DateTime('UTC'), Float64))) ENGINE = TimeSeries SETTINGS version = 6;

INSERT INTO ts_dt (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime(0, 'UTC'), 0), (toDateTime(1000, 'UTC'), 1), (toDateTime(1001, 'UTC'), 2), (toDateTime(1002, 'UTC'), 3), (toDateTime(1003, 'UTC'), 4), (toDateTime(4294967295, 'UTC'), 5)]);
INSERT INTO ts_u32 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(0, 0), (1000, 1), (1001, 2), (1002, 3), (1003, 4), (4294967295, 5)]);
INSERT INTO ts_dt_v6 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime(1000, 'UTC'), 1), (toDateTime(1001, 'UTC'), 2), (toDateTime(1002, 'UTC'), 3)]);

SELECT '-- The returned columns keep the types of the table';
DESCRIBE timeSeriesSelector(ts_dt, 'up', 1000.5, 1002.5);
DESCRIBE timeSeriesSelector(ts_u32, 'up', 1000.5, 1002.5);

SELECT '-- Version 6 keeps the row-shaped selector result';
DESCRIBE timeSeriesSelector(ts_dt_v6, 'up', 1000.5, 1002.5);
SELECT timestamp, value FROM timeSeriesSelector(ts_dt_v6, 'up', 1000.5, 1002.5) ORDER BY timestamp;

SELECT '-- [1000.5, 1002.5] selects the samples at 1001 and 1002';
SELECT tupleElement(sample, 1) AS timestamp, tupleElement(sample, 2) AS value
FROM timeSeriesSelector(ts_dt, 'up', 1000.5, 1002.5)
ARRAY JOIN time_series AS sample
ORDER BY timestamp;

SELECT '-- The bounds are inclusive at millisecond precision';
SELECT tupleElement(sample, 1) AS timestamp, tupleElement(sample, 2) AS value
FROM timeSeriesSelector(ts_dt, 'up', 1001, 1002)
ARRAY JOIN time_series AS sample
ORDER BY timestamp;
SELECT tupleElement(sample, 1) AS timestamp, tupleElement(sample, 2) AS value
FROM timeSeriesSelector(ts_dt, 'up', 1001.001, 1001.999)
ARRAY JOIN time_series AS sample
ORDER BY timestamp;

SELECT '-- String arguments';
SELECT tupleElement(sample, 1) AS timestamp, tupleElement(sample, 2) AS value
FROM timeSeriesSelector(ts_dt, 'up', '1970-01-01 00:16:40.5', '1970-01-01 00:16:42.5')
ARRAY JOIN time_series AS sample
ORDER BY timestamp;
SELECT tupleElement(sample, 1) AS timestamp, tupleElement(sample, 2) AS value
FROM timeSeriesSelector(ts_u32, 'up', '1000.5', '1002.5')
ARRAY JOIN time_series AS sample
ORDER BY timestamp;

SELECT '-- The range is checked before the conversion to the scale of the table';
SELECT count() FROM timeSeriesSelector(ts_dt, 'up', 1002.5, 1000.5) ARRAY JOIN time_series; -- { serverError BAD_ARGUMENTS }

SELECT '-- The time range is before 1970';
SELECT count() FROM timeSeriesSelector(ts_dt, 'up', -100, -50) ARRAY JOIN time_series;
SELECT count() FROM timeSeriesSelector(ts_u32, 'up', -100, -0.001) ARRAY JOIN time_series;

SELECT '-- The time range starts before 1970 and reaches the samples at 0 and 1000';
SELECT tupleElement(sample, 1) AS timestamp, tupleElement(sample, 2) AS value
FROM timeSeriesSelector(ts_dt, 'up', -100, 1000.5)
ARRAY JOIN time_series AS sample
ORDER BY timestamp;

SELECT '-- The time range is after 2106';
SELECT count() FROM timeSeriesSelector(ts_dt, 'up', 4294967296, 5000000000) ARRAY JOIN time_series;
SELECT count() FROM timeSeriesSelector(ts_u32, 'up', 4294967295.001, 5000000000) ARRAY JOIN time_series;

SELECT '-- The time range ends after 2106 and reaches the samples at 1003 and 4294967295';
SELECT tupleElement(sample, 1) AS timestamp, tupleElement(sample, 2) AS value
FROM timeSeriesSelector(ts_dt, 'up', 1002.5, 5000000000)
ARRAY JOIN time_series AS sample
ORDER BY timestamp;

SELECT '-- The time range covers the whole range of the type';
SELECT count() FROM timeSeriesSelector(ts_dt, 'up', -5000000000, 5000000000) ARRAY JOIN time_series;
SELECT count() FROM timeSeriesSelector(ts_u32, 'up', -5000000000, 5000000000) ARRAY JOIN time_series;

DROP TABLE ts_dt;
DROP TABLE ts_u32;
DROP TABLE ts_dt_v6;
