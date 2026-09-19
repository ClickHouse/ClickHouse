-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
--
-- The arguments `min_time` and `max_time` of table function `timeSeriesSelector` can have any scale: the time range keeps the greatest
-- of the scales of the arguments and the table, but at least milliseconds, and it is converted to the scale of the table
-- with the bounds rounded towards the inside of the range. The range is also intersected with the range of the timestamp type
-- of the table: `DateTime` and `UInt32` can't hold timestamps before 1970 or after 2106. The returned columns keep the types of the table.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_dt;
DROP TABLE IF EXISTS ts_u32;
DROP TABLE IF EXISTS ts_dt64_1;
DROP TABLE IF EXISTS ts_dt64_6;

CREATE TABLE ts_dt (samples Array(Tuple(DateTime('UTC'), Float64))) ENGINE = TimeSeries;
CREATE TABLE ts_u32 (samples Array(Tuple(UInt32, Float32))) ENGINE = TimeSeries;
CREATE TABLE ts_dt64_1 (samples Array(Tuple(DateTime64(1, 'UTC'), Float64))) ENGINE = TimeSeries;
CREATE TABLE ts_dt64_6 (samples Array(Tuple(DateTime64(6, 'UTC'), Float64))) ENGINE = TimeSeries;

INSERT INTO ts_dt (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime(0, 'UTC'), 0), (toDateTime(1000, 'UTC'), 1), (toDateTime(1001, 'UTC'), 2), (toDateTime(1002, 'UTC'), 3), (toDateTime(1003, 'UTC'), 4), (toDateTime(4294967295, 'UTC'), 5)]);
INSERT INTO ts_u32 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(0, 0), (1000, 1), (1001, 2), (1002, 3), (1003, 4), (4294967295, 5)]);
INSERT INTO ts_dt64_1 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime64(1000, 1, 'UTC'), 1), (toDateTime64(1001.5, 1, 'UTC'), 2), (toDateTime64(1002, 1, 'UTC'), 3), (toDateTime64(1003, 1, 'UTC'), 4)]);
INSERT INTO ts_dt64_6 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime64(1000, 6, 'UTC'), 1), (toDateTime64(1001.000001, 6, 'UTC'), 2), (toDateTime64(1002, 6, 'UTC'), 3)]);

SELECT '-- The returned columns keep the types of the table';
DESCRIBE timeSeriesSelector(ts_dt, 'up', 1000.5, 1002.5);
DESCRIBE timeSeriesSelector(ts_u32, 'up', 1000.5, 1002.5);
DESCRIBE timeSeriesSelector(ts_dt64_1, 'up', 1000.5, 1002.5);

SELECT '-- [1000.5, 1002.5] selects the samples at 1001 and 1002 (and 1001.5 in DateTime64(1))';
SELECT timestamp, value FROM timeSeriesSelector(ts_dt, 'up', 1000.5, 1002.5) ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_u32, 'up', 1000.5, 1002.5) ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_dt64_1, 'up', 1000.5, 1002.5) ORDER BY timestamp;

SELECT '-- The bounds are inclusive at millisecond precision';
SELECT timestamp, value FROM timeSeriesSelector(ts_dt, 'up', 1001, 1002) ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_dt, 'up', 1000.999, 1002.001) ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_dt, 'up', 1001.001, 1001.999) ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_u32, 'up', 1001.001, 1001.999) ORDER BY timestamp;

SELECT '-- DateTime64 and String arguments';
SELECT timestamp, value FROM timeSeriesSelector(ts_dt, 'up', toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1002.5, 3, 'UTC')) ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_dt, 'up', '1970-01-01 00:16:40.5', '1970-01-01 00:16:42.5') ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_u32, 'up', '1000.5', '1002.5') ORDER BY timestamp;

SELECT '-- A scale greater than 3 in the arguments is kept';
SELECT timestamp, value FROM timeSeriesSelector(ts_dt64_1, 'up', toDateTime64(1001.49999, 5, 'UTC'), toDateTime64(1001.50001, 5, 'UTC')) ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_dt64_1, 'up', toDateTime64(1001.50001, 5, 'UTC'), toDateTime64(1001.99999, 5, 'UTC')) ORDER BY timestamp;

SELECT '-- Arguments with a smaller scale than the table are widened to the scale of the table';
DESCRIBE timeSeriesSelector(ts_dt64_6, 'up', 1000.5, 1002.5);
SELECT timestamp, value FROM timeSeriesSelector(ts_dt64_6, 'up', 1001, 1002) ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_dt64_6, 'up', 1000.5, toDateTime64(1001.000001, 6, 'UTC')) ORDER BY timestamp;

SELECT '-- The range is checked before the conversion to the scale of the table';
SELECT count() FROM timeSeriesSelector(ts_dt, 'up', 1001.2, 1001.8);
SELECT count() FROM timeSeriesSelector(ts_dt, 'up', 1002.5, 1000.5); -- { serverError BAD_ARGUMENTS }

SELECT '-- The time range lies between two consecutive timestamps of a DateTime64(1) table';
SELECT count() FROM timeSeriesSelector(ts_dt64_1, 'up', toDateTime64(1000.02, 3, 'UTC'), toDateTime64(1000.08, 3, 'UTC'));
SELECT count() FROM timeSeriesSelector(ts_dt64_1, 'up', toDateTime64(1000.02, 3, 'UTC'), toDateTime64(1001.5, 3, 'UTC'));

SELECT '-- The time range is before 1970';
SELECT count() FROM timeSeriesSelector(ts_dt, 'up', -100, -50);
SELECT count() FROM timeSeriesSelector(ts_u32, 'up', -100, -50);
SELECT count() FROM timeSeriesSelector(ts_u32, 'up', -100, -0.001);

SELECT '-- The time range starts before 1970 and reaches the samples at 0 and 1000';
SELECT timestamp, value FROM timeSeriesSelector(ts_dt, 'up', -100, 1000.5) ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_u32, 'up', -100, 1000.5) ORDER BY timestamp;

SELECT '-- The time range is after 2106';
SELECT count() FROM timeSeriesSelector(ts_dt, 'up', 4294967296, 5000000000);
SELECT count() FROM timeSeriesSelector(ts_u32, 'up', 4294967296, 5000000000);
SELECT count() FROM timeSeriesSelector(ts_u32, 'up', 4294967295.001, 5000000000);

SELECT '-- The time range ends after 2106 and reaches the samples at 1003 and 4294967295';
SELECT timestamp, value FROM timeSeriesSelector(ts_dt, 'up', 1002.5, 5000000000) ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_u32, 'up', 1002.5, 5000000000) ORDER BY timestamp;

SELECT '-- The time range covers the whole range of the type';
SELECT count() FROM timeSeriesSelector(ts_dt, 'up', -5000000000, 5000000000);
SELECT count() FROM timeSeriesSelector(ts_u32, 'up', -5000000000, 5000000000);

SELECT '-- Arguments with a scale greater than 9 on a table with a recent samples table: the range is compared with the TTL without overflow';
DROP TABLE IF EXISTS ts_recent;
CREATE TABLE ts_recent (samples Array(Tuple(DateTime('UTC'), Float64))) ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;
INSERT INTO ts_recent (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime(1000, 'UTC'), 1), (toDateTime(1001, 'UTC'), 2), (toDateTime(1002, 'UTC'), 3), (toDateTime(1003, 'UTC'), 4)]);
SELECT count() FROM timeSeriesSelector(ts_recent, 'up', toDecimal64(1000, 12), toDecimal64(1003, 12));
SELECT count() FROM timeSeriesSelector(ts_recent, 'up', toDecimal64(1000.5, 12), toDecimal64(1002.5, 12));

DROP TABLE ts_dt;
DROP TABLE ts_u32;
DROP TABLE ts_dt64_1;
DROP TABLE ts_dt64_6;
DROP TABLE ts_recent;
