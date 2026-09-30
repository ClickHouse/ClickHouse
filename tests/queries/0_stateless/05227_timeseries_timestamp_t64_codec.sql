SET allow_experimental_time_series_table = 1;

-- Both version 5 row samples targets use `Delta`, `T64`, and `ZSTD(3)` and preserve timestamps, including fractional seconds.
CREATE TABLE ts_t64 ENGINE = TimeSeries SETTINGS version = 5;
SELECT type, compression_codec, count()
FROM system.columns
WHERE database = currentDatabase() AND name = 'timestamp'
    AND (table LIKE '.inner_id.samples.%' OR table LIKE '.inner_id.recentsamples.%')
GROUP BY type, compression_codec ORDER BY type, compression_codec;
INSERT INTO ts_t64 (metric_name, tags, samples)
SELECT 'metric', map(), groupArray((ts, toFloat64(toUnixTimestamp64Milli(ts))))
FROM (SELECT now64(3) + toIntervalMillisecond(number) AS ts FROM numbers(130));
SELECT count(), countIf(toUnixTimestamp64Milli(timestamp) != value)
FROM merge(currentDatabase(), '^\\.inner_id\\.(samples|recentsamples)\\.') GROUP BY _table;
DROP TABLE ts_t64;

-- The codecs also apply to all supported timestamp types selected by the outer column.
CREATE TABLE ts_t64 (samples Array(Tuple(DateTime64(6), Float64))) ENGINE = TimeSeries SETTINGS version = 5;
SELECT type, compression_codec, count()
FROM system.columns
WHERE database = currentDatabase() AND name = 'timestamp'
    AND (table LIKE '.inner_id.samples.%' OR table LIKE '.inner_id.recentsamples.%')
GROUP BY type, compression_codec ORDER BY type, compression_codec;
INSERT INTO ts_t64 (metric_name, tags, samples)
SELECT 'metric', map(), groupArray((ts, toFloat64(toUnixTimestamp64Micro(ts))))
FROM (SELECT now64(6) + toIntervalMicrosecond(number) AS ts FROM numbers(130));
SELECT count(), countIf(toUnixTimestamp64Micro(timestamp) != value)
FROM merge(currentDatabase(), '^\\.inner_id\\.(samples|recentsamples)\\.') GROUP BY _table;
DROP TABLE ts_t64;

CREATE TABLE ts_t64 (samples Array(Tuple(DateTime, Float64))) ENGINE = TimeSeries SETTINGS version = 5;
SELECT type, compression_codec, count()
FROM system.columns
WHERE database = currentDatabase() AND name = 'timestamp'
    AND (table LIKE '.inner_id.samples.%' OR table LIKE '.inner_id.recentsamples.%')
GROUP BY type, compression_codec ORDER BY type, compression_codec;
INSERT INTO ts_t64 (metric_name, tags, samples)
SELECT 'metric', map(), groupArray((ts, toFloat64(toUInt32(ts))))
FROM (SELECT now() + toIntervalSecond(number) AS ts FROM numbers(130));
SELECT count(), countIf(toUInt32(timestamp) != value)
FROM merge(currentDatabase(), '^\\.inner_id\\.(samples|recentsamples)\\.') GROUP BY _table;
DROP TABLE ts_t64;

CREATE TABLE ts_t64 (samples Array(Tuple(UInt32, Float64))) ENGINE = TimeSeries SETTINGS version = 5;
SELECT type, compression_codec, count()
FROM system.columns
WHERE database = currentDatabase() AND name = 'timestamp'
    AND (table LIKE '.inner_id.samples.%' OR table LIKE '.inner_id.recentsamples.%')
GROUP BY type, compression_codec ORDER BY type, compression_codec;
INSERT INTO ts_t64 (metric_name, tags, samples)
SELECT 'metric', map(), groupArray((ts, toFloat64(ts)))
FROM (SELECT toUInt32(now()) + toUInt32(number) AS ts FROM numbers(130));
SELECT count(), countIf(timestamp != value)
FROM merge(currentDatabase(), '^\\.inner_id\\.(samples|recentsamples)\\.') GROUP BY _table;
DROP TABLE ts_t64;

-- Explicit timestamp codecs are preserved for each target independently.
CREATE TABLE ts_t64 ENGINE = TimeSeries SETTINGS version = 5
SAMPLES INNER COLUMNS (timestamp DateTime64(3) CODEC(DoubleDelta, ZSTD(1)))
RECENT SAMPLES INNER COLUMNS (timestamp DateTime64(3) CODEC(Delta, LZ4));
SELECT if(table LIKE '.inner_id.samples.%', 'samples', 'recent_samples') AS target, compression_codec
FROM system.columns
WHERE database = currentDatabase() AND name = 'timestamp'
    AND (table LIKE '.inner_id.samples.%' OR table LIKE '.inner_id.recentsamples.%')
ORDER BY target;
DROP TABLE ts_t64;

-- `CREATE TABLE AS` recognizes both current and historical generated timestamp codecs.
CREATE TABLE ts_t64_source ENGINE = TimeSeries SETTINGS version = 5;
CREATE TABLE ts_t64 AS ts_t64_source ENGINE = TimeSeries SETTINGS version = 5
SAMPLES INNER COLUMNS (timestamp DateTime64(6) CODEC(Delta, T64, ZSTD(3)));
DROP TABLE ts_t64_source;
SELECT type, compression_codec, count()
FROM system.columns
WHERE database = currentDatabase() AND name = 'timestamp'
    AND (table LIKE '.inner_id.samples.%' OR table LIKE '.inner_id.recentsamples.%')
GROUP BY type, compression_codec ORDER BY type, compression_codec;
DROP TABLE ts_t64;

CREATE TABLE ts_t64_source ENGINE = TimeSeries SETTINGS version = 5
SAMPLES INNER COLUMNS (timestamp DateTime64(3) CODEC(DoubleDelta, ZSTD(1)))
RECENT SAMPLES INNER COLUMNS (timestamp DateTime64(3) CODEC(DoubleDelta, ZSTD(1)));
CREATE TABLE ts_t64 AS ts_t64_source ENGINE = TimeSeries SETTINGS version = 5
SAMPLES INNER COLUMNS (timestamp DateTime64(6) CODEC(Delta, T64, ZSTD(3)));
DROP TABLE ts_t64_source;
SELECT type, compression_codec, count()
FROM system.columns
WHERE database = currentDatabase() AND name = 'timestamp'
    AND (table LIKE '.inner_id.samples.%' OR table LIKE '.inner_id.recentsamples.%')
GROUP BY type, compression_codec ORDER BY type, compression_codec;
DROP TABLE ts_t64;
