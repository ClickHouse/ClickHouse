SET allow_experimental_time_series_table = 1;

-- The generated samples tables use `Delta`, `T64`, and `ZSTD(3)` for the timestamps of the `samples` column (a codec of a subcolumn)
-- and for the `bucket`, `min_time` and `max_time` columns, and preserve the timestamps, including fractional seconds.
CREATE TABLE ts_t64 ENGINE = TimeSeries;
SELECT name, type, if(name = 'samples', subcolumn_codecs['timestamp'], compression_codec) AS timestamp_codec, count()
FROM system.columns
WHERE database = currentDatabase() AND name IN ('samples', 'bucket', 'min_time', 'max_time')
    AND (table LIKE '.inner_id.samples.%' OR table LIKE '.inner_id.recentsamples.%')
GROUP BY name, type, timestamp_codec ORDER BY name;
INSERT INTO ts_t64 (metric_name, tags, samples)
SELECT 'metric', map(), groupArray((ts, toFloat64(toUnixTimestamp64Milli(ts))))
FROM (SELECT now64(3) + toIntervalMillisecond(number) AS ts FROM numbers(130));
SELECT count(), countIf(toUnixTimestamp64Milli(sample.1) != sample.2)
FROM (SELECT _table, arrayJoin(samples) AS sample FROM merge(currentDatabase(), '^\\.inner_id\\.(samples|recentsamples)\\.')) GROUP BY _table;
DROP TABLE ts_t64;

-- The codecs also apply to all supported timestamp types selected by the outer column.
CREATE TABLE ts_t64 (samples Array(Tuple(DateTime64(6), Float64))) ENGINE = TimeSeries;
SELECT type, subcolumn_codecs['timestamp'] AS timestamp_codec, count()
FROM system.columns
WHERE database = currentDatabase() AND name = 'samples'
    AND (table LIKE '.inner_id.samples.%' OR table LIKE '.inner_id.recentsamples.%')
GROUP BY type, timestamp_codec ORDER BY type, timestamp_codec;
INSERT INTO ts_t64 (metric_name, tags, samples)
SELECT 'metric', map(), groupArray((ts, toFloat64(toUnixTimestamp64Micro(ts))))
FROM (SELECT now64(6) + toIntervalMicrosecond(number) AS ts FROM numbers(130));
SELECT count(), countIf(toUnixTimestamp64Micro(sample.1) != sample.2)
FROM (SELECT _table, arrayJoin(samples) AS sample FROM merge(currentDatabase(), '^\\.inner_id\\.(samples|recentsamples)\\.')) GROUP BY _table;
DROP TABLE ts_t64;

CREATE TABLE ts_t64 (samples Array(Tuple(DateTime, Float64))) ENGINE = TimeSeries;
SELECT type, subcolumn_codecs['timestamp'] AS timestamp_codec, count()
FROM system.columns
WHERE database = currentDatabase() AND name = 'samples'
    AND (table LIKE '.inner_id.samples.%' OR table LIKE '.inner_id.recentsamples.%')
GROUP BY type, timestamp_codec ORDER BY type, timestamp_codec;
INSERT INTO ts_t64 (metric_name, tags, samples)
SELECT 'metric', map(), groupArray((ts, toFloat64(toUInt32(ts))))
FROM (SELECT now() + toIntervalSecond(number) AS ts FROM numbers(130));
SELECT count(), countIf(toUInt32(sample.1) != sample.2)
FROM (SELECT _table, arrayJoin(samples) AS sample FROM merge(currentDatabase(), '^\\.inner_id\\.(samples|recentsamples)\\.')) GROUP BY _table;
DROP TABLE ts_t64;

CREATE TABLE ts_t64 (samples Array(Tuple(UInt32, Float64))) ENGINE = TimeSeries;
SELECT type, subcolumn_codecs['timestamp'] AS timestamp_codec, count()
FROM system.columns
WHERE database = currentDatabase() AND name = 'samples'
    AND (table LIKE '.inner_id.samples.%' OR table LIKE '.inner_id.recentsamples.%')
GROUP BY type, timestamp_codec ORDER BY type, timestamp_codec;
INSERT INTO ts_t64 (metric_name, tags, samples)
SELECT 'metric', map(), groupArray((ts, toFloat64(ts)))
FROM (SELECT toUInt32(now()) + toUInt32(number) AS ts FROM numbers(130));
SELECT count(), countIf(sample.1 != sample.2)
FROM (SELECT _table, arrayJoin(samples) AS sample FROM merge(currentDatabase(), '^\\.inner_id\\.(samples|recentsamples)\\.')) GROUP BY _table;
DROP TABLE ts_t64;

-- Explicit timestamp codecs are preserved for each target independently.
CREATE TABLE ts_t64 ENGINE = TimeSeries
SAMPLES INNER COLUMNS (samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3), value Float64))), CODEC samples.timestamp (DoubleDelta, ZSTD(1)))
RECENT SAMPLES INNER COLUMNS (samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3), value Float64))), CODEC samples.timestamp (Delta, LZ4));
SELECT if(table LIKE '.inner_id.samples.%', 'samples', 'recent_samples') AS target, subcolumn_codecs
FROM system.columns
WHERE database = currentDatabase() AND name = 'samples'
    AND (table LIKE '.inner_id.samples.%' OR table LIKE '.inner_id.recentsamples.%')
ORDER BY target;
DROP TABLE ts_t64;

-- `CREATE TABLE AS` recognizes the generated codecs of the source and generates them for the declared type without duplicates.
CREATE TABLE ts_t64_source ENGINE = TimeSeries;
CREATE TABLE ts_t64 AS ts_t64_source ENGINE = TimeSeries
SAMPLES INNER COLUMNS (samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(6), value Float64))),
                       CODEC samples.timestamp (Delta, T64, ZSTD(3)), CODEC samples.value (ALP, ZSTD(3)));
DROP TABLE ts_t64_source;
SELECT type, subcolumn_codecs, count()
FROM system.columns
WHERE database = currentDatabase() AND name = 'samples'
    AND (table LIKE '.inner_id.samples.%' OR table LIKE '.inner_id.recentsamples.%')
GROUP BY type, subcolumn_codecs ORDER BY type;
DROP TABLE ts_t64;
