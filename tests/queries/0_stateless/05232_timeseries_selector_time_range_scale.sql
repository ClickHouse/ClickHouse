-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
--
-- The arguments `min_time` and `max_time` of table function `timeSeriesSelector` can have any scale: the time range keeps the greatest
-- of the scales of the arguments and the table, but at least milliseconds, and it is converted to the scale of the table
-- with the bounds rounded towards the inside of the range. On a table with a recent samples table the range is compared
-- with the TTL in seconds, so arguments with a scale greater than 9 don't overflow.
-- Tables storing timestamps as `DateTime` or `UInt32` are covered by 05232_timeseries_selector_time_range_conversion.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_dt64_1;
DROP TABLE IF EXISTS ts_dt64_6;
DROP TABLE IF EXISTS ts_recent;

CREATE TABLE ts_dt64_1 (samples Array(Tuple(DateTime64(1, 'UTC'), Float64))) ENGINE = TimeSeries;
CREATE TABLE ts_dt64_6 (samples Array(Tuple(DateTime64(6, 'UTC'), Float64))) ENGINE = TimeSeries;
CREATE TABLE ts_recent (samples Array(Tuple(DateTime('UTC'), Float64))) ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;

INSERT INTO ts_dt64_1 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime64(1000, 1, 'UTC'), 1), (toDateTime64(1001.5, 1, 'UTC'), 2), (toDateTime64(1002, 1, 'UTC'), 3), (toDateTime64(1003, 1, 'UTC'), 4)]);
INSERT INTO ts_dt64_6 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime64(1000, 6, 'UTC'), 1), (toDateTime64(1001.000001, 6, 'UTC'), 2), (toDateTime64(1002, 6, 'UTC'), 3)]);
INSERT INTO ts_recent (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime(1000, 'UTC'), 1), (toDateTime(1001, 'UTC'), 2), (toDateTime(1002, 'UTC'), 3), (toDateTime(1003, 'UTC'), 4)]);

SELECT '-- The returned columns keep the types of the table';
DESCRIBE timeSeriesSelector(ts_dt64_1, 'up', 1000.5, 1002.5);

SELECT '-- [1000.5, 1002.5] selects the samples at 1001.5 and 1002 in DateTime64(1)';
SELECT tupleElement(sample, 1) AS timestamp, tupleElement(sample, 2) AS value
FROM timeSeriesSelector(ts_dt64_1, 'up', 1000.5, 1002.5)
ARRAY JOIN time_series AS sample
ORDER BY timestamp;

SELECT '-- A scale greater than 3 in the arguments is kept';
SELECT tupleElement(sample, 1) AS timestamp, tupleElement(sample, 2) AS value
FROM timeSeriesSelector(ts_dt64_1, 'up', toDateTime64(1001.49999, 5, 'UTC'), toDateTime64(1001.50001, 5, 'UTC'))
ARRAY JOIN time_series AS sample
ORDER BY timestamp;

SELECT '-- Arguments with a smaller scale than the table are widened to the scale of the table';
DESCRIBE timeSeriesSelector(ts_dt64_6, 'up', 1000.5, 1002.5);
SELECT tupleElement(sample, 1) AS timestamp, tupleElement(sample, 2) AS value
FROM timeSeriesSelector(ts_dt64_6, 'up', 1000.5, toDateTime64(1001.000001, 6, 'UTC'))
ARRAY JOIN time_series AS sample
ORDER BY timestamp;

SELECT '-- The time range lies between two consecutive timestamps of a DateTime64(1) table';
SELECT count() FROM timeSeriesSelector(ts_dt64_1, 'up', toDateTime64(1000.02, 3, 'UTC'), toDateTime64(1000.08, 3, 'UTC'))
ARRAY JOIN time_series;

SELECT '-- Arguments with a scale greater than 9 on a table with a recent samples table: the range is compared with the TTL without overflow';
SELECT count() FROM timeSeriesSelector(ts_recent, 'up', toDecimal64(1000.5, 12), toDecimal64(1002.5, 12))
ARRAY JOIN time_series;

DROP TABLE ts_dt64_1;
DROP TABLE ts_dt64_6;
DROP TABLE ts_recent;
