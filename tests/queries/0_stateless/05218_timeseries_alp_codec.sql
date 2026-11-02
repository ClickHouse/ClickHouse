-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: the initiator sends the argument of `timeSeriesSamples` to the replicas unqualified,
-- so they look the table up in the `default` database, see https://github.com/ClickHouse/ClickHouse/issues/118130.

SET allow_experimental_time_series_table = 1;

-- The generated samples and recent samples tables declare `ALP` for the values of the `samples` column as a codec of a subcolumn.
-- Both are gated features, and the engine enables them for its inner tables itself, so the session keeps them disabled here.
SET enable_alp_codec = 0;
SET enable_subcolumn_codecs = 0;

CREATE TABLE ts_alp64 ENGINE = TimeSeries;
SELECT type, subcolumn_codecs['value'] AS value_codec, count()
FROM system.columns
WHERE database = currentDatabase() AND name = 'samples'
    AND (table LIKE '.inner_id.samples.%' OR table LIKE '.inner_id.recentsamples.%')
GROUP BY type, value_codec
ORDER BY type, value_codec;
SELECT toUInt8(getSetting('enable_alp_codec')), toUInt8(getSetting('enable_subcolumn_codecs'));

-- Keep floating-point bit patterns, including the Prometheus stale marker.
-- The samples get distinct timestamps: the samples of a series with the same timestamp are deduplicated on insertion.
INSERT INTO ts_alp64 (metric_name, tags, samples)
SELECT 'metric', map('kind', 'special'), arrayMap((x, i) -> (now64(3) + toIntervalMillisecond(i), reinterpretAsFloat64(x)), bits, arrayEnumerate(bits))
FROM (SELECT [toUInt64(0x3FF8000000000000), 0xBFF8000000000000, 0x7FF0000000000000,
              0xFFF0000000000000, 0x7FF0000000000002, 0x7FF8000000000001, 0x0000000000000001] AS bits);
SELECT arraySort(groupArray(reinterpretAsUInt64(sample.2))) FROM (SELECT arrayJoin(samples) AS sample FROM timeSeriesSamples(ts_alp64));
SELECT arraySort(groupArray(reinterpretAsUInt64(sample.2))) FROM (SELECT arrayJoin(samples) AS sample FROM merge(currentDatabase(), '^\\.inner_id\\.recentsamples\\.'));
DROP TABLE ts_alp64;

-- The codec also applies when the outer column selects `Float32` values.
CREATE TABLE ts_alp32 (samples Array(Tuple(DateTime64(3), Float32))) ENGINE = TimeSeries;
SELECT type, subcolumn_codecs['value'] AS value_codec, count()
FROM system.columns
WHERE database = currentDatabase() AND name = 'samples'
    AND (table LIKE '.inner_id.samples.%' OR table LIKE '.inner_id.recentsamples.%')
GROUP BY type, value_codec
ORDER BY type, value_codec;
INSERT INTO ts_alp32 (metric_name, tags, samples)
SELECT 'metric', map('kind', 'special'), arrayMap((x, i) -> (now64(3) + toIntervalMillisecond(i), reinterpretAsFloat32(x)), bits, arrayEnumerate(bits))
FROM (SELECT [toUInt32(0x3FC00000), 0xBFC00000, 0x7F800000, 0xFF800000, 0x7FC00001, 0x00000001] AS bits);
SELECT arraySort(groupArray(reinterpretAsUInt32(sample.2))) FROM (SELECT arrayJoin(samples) AS sample FROM timeSeriesSamples(ts_alp32));
SELECT arraySort(groupArray(reinterpretAsUInt32(sample.2))) FROM (SELECT arrayJoin(samples) AS sample FROM merge(currentDatabase(), '^\\.inner_id\\.recentsamples\\.'));
DROP TABLE ts_alp32;
