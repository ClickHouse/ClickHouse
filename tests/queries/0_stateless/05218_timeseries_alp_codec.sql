SET allow_experimental_time_series_table = 1;
SET enable_alp_codec = 0;

-- Generated version 5 row samples and recent samples use `ALP` without enabling it for the session.
CREATE TABLE ts_alp64 ENGINE = TimeSeries SETTINGS version = 5;
SELECT type, compression_codec, count()
FROM system.columns
WHERE database = currentDatabase() AND name = 'value'
    AND (table LIKE '.inner_id.samples.%' OR table LIKE '.inner_id.recentsamples.%')
GROUP BY type, compression_codec
ORDER BY type, compression_codec;
SELECT toUInt8(getSetting('enable_alp_codec'));

-- Keep floating-point bit patterns, including the Prometheus stale marker.
INSERT INTO ts_alp64 (metric_name, tags, samples)
SELECT 'metric', map('kind', 'special'),
    arrayMap(x -> (now64(3), reinterpretAsFloat64(x)),
        [toUInt64(0x3FF8000000000000), 0xBFF8000000000000, 0x7FF0000000000000,
         0xFFF0000000000000, 0x7FF0000000000002, 0x7FF8000000000001, 0x0000000000000001]);
SELECT arraySort(groupArray(reinterpretAsUInt64(value))) FROM timeSeriesSamples(ts_alp64);
SELECT arraySort(groupArray(reinterpretAsUInt64(value))) FROM merge(currentDatabase(), '^\\.inner_id\\.recentsamples\\.');
DROP TABLE ts_alp64;

-- The codec also applies when the outer column selects `Float32` values.
CREATE TABLE ts_alp32 (samples Array(Tuple(DateTime64(3), Float32))) ENGINE = TimeSeries SETTINGS version = 5;
SELECT type, compression_codec, count()
FROM system.columns
WHERE database = currentDatabase() AND name = 'value'
    AND (table LIKE '.inner_id.samples.%' OR table LIKE '.inner_id.recentsamples.%')
GROUP BY type, compression_codec
ORDER BY type, compression_codec;
INSERT INTO ts_alp32 (metric_name, tags, samples)
SELECT 'metric', map('kind', 'special'),
    arrayMap(x -> (now64(3), reinterpretAsFloat32(x)),
        [toUInt32(0x3FC00000), 0xBFC00000, 0x7F800000, 0xFF800000, 0x7FC00001, 0x00000001]);
SELECT arraySort(groupArray(reinterpretAsUInt32(value))) FROM timeSeriesSamples(ts_alp32);
SELECT arraySort(groupArray(reinterpretAsUInt32(value))) FROM merge(currentDatabase(), '^\\.inner_id\\.recentsamples\\.');
DROP TABLE ts_alp32;
