-- The codec settings must not be persisted in a table definition readable by pre-v7 servers.
SET allow_experimental_time_series_table = 1;

CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS version = 6, samples_compression_codec = 'ZSTD(3)'; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS version = 6, recent_samples_compression_codec = 'ZSTD(3)'; -- { serverError INVALID_SETTING_VALUE }

CREATE TABLE ts_source ENGINE = TimeSeries SETTINGS samples_compression_codec = 'ZSTD(3)', recent_samples_compression_codec = 'ZSTD(3)';
CREATE TABLE ts_v6 AS ts_source SETTINGS version = 6;
SELECT position(create_table_query, 'samples_compression_codec') = 0, position(create_table_query, 'recent_samples_compression_codec') = 0
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v6';

CREATE TABLE ts_without_recent AS ts_source SETTINGS recent_samples_ttl_seconds = 0;
SELECT position(create_table_query, 'recent_samples_compression_codec') = 0
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_without_recent';

DROP TABLE ts_without_recent;
DROP TABLE ts_v6;
DROP TABLE ts_source;

-- A bucketed `AggregatingMergeTree` must aggregate samples even if the user explicitly declares the column.
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER COLUMNS (samples Array(Tuple(DateTime64(3), Float64))); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER COLUMNS (samples Array(Tuple(DateTime64(3), Float64)))
SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS allow_dimensions_outside_sorting_key = 1; -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts_bad ENGINE = TimeSeries
RECENT SAMPLES INNER COLUMNS (samples Array(Tuple(DateTime64(3), Float64))); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = ReplacingMergeTree; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts_bad ENGINE = TimeSeries
RECENT SAMPLES INNER ENGINE = SummingMergeTree; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER COLUMNS (min_time DateTime64(3))
SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS allow_dimensions_outside_sorting_key = 1; -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts_bad ENGINE = TimeSeries
RECENT SAMPLES INNER COLUMNS (max_time DateTime64(3))
RECENT SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS allow_dimensions_outside_sorting_key = 1; -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER COLUMNS (min_time SimpleAggregateFunction(max, DateTime64(3))); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE ts_good ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER COLUMNS (min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))));
DROP TABLE ts_good;

-- External bucketed samples targets need the same merge-safe schema as inner targets.
-- Full-definition `ATTACH` checks are in 05257, which generates fresh UUIDs for reruns.
CREATE TABLE ts_external_raw
(
    id UUID,
    samples Array(Tuple(DateTime64(3), Float64)),
    bucket DateTime64(3),
    min_time DateTime64(3),
    max_time DateTime64(3)
) ENGINE = AggregatingMergeTree ORDER BY (id, bucket) SETTINGS allow_dimensions_outside_sorting_key = 1;
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES ts_external_raw; -- { serverError BAD_TYPE_OF_FIELD }
DROP TABLE ts_external_raw;

CREATE TABLE ts_external_replacing
(
    id UUID,
    samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(DateTime64(3), Float64))),
    bucket DateTime64(3),
    min_time SimpleAggregateFunction(min, DateTime64(3)),
    max_time SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = ReplacingMergeTree ORDER BY (id, bucket);
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES ts_external_replacing; -- { serverError INVALID_SETTING_VALUE }
DROP TABLE ts_external_replacing;

CREATE TABLE ts_external_good
(
    id UUID,
    samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(DateTime64(3), Float64))),
    bucket DateTime64(3),
    min_time SimpleAggregateFunction(min, DateTime64(3)),
    max_time SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree ORDER BY (id, bucket);
CREATE TABLE ts_external_good_owner ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES ts_external_good;
INSERT INTO ts_external_good VALUES ('00000000-0000-0000-0000-000000000001', [(toDateTime64(1000, 3), 1.)], toDateTime64(0, 3), toDateTime64(1000, 3), toDateTime64(1000, 3));
INSERT INTO ts_external_good VALUES ('00000000-0000-0000-0000-000000000001', [(toDateTime64(1001, 3), 2.)], toDateTime64(0, 3), toDateTime64(1001, 3), toDateTime64(1001, 3));
OPTIMIZE TABLE ts_external_good FINAL;
SELECT count(), sum(length(samples)) FROM ts_external_good;

-- Recheck the physical target before reads and writes after an independent target ALTER.
CREATE TABLE ts_alter_target AS ts_external_good ENGINE = AggregatingMergeTree ORDER BY (id, bucket);
CREATE TABLE ts_alter_owner ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0 SAMPLES ts_alter_target;
ALTER TABLE ts_alter_target MODIFY COLUMN samples Array(Tuple(DateTime64(3), Float64));
SELECT * FROM ts_alter_owner LIMIT 1; -- { serverError BAD_TYPE_OF_FIELD }
INSERT INTO ts_alter_owner (metric_name, tags, samples)
VALUES ('m', map(), [(toDateTime64(1000, 3), 1.)]); -- { serverError BAD_TYPE_OF_FIELD }
DROP TABLE ts_alter_owner;
DROP TABLE ts_alter_target;

-- RESTORE must validate the currently bound physical target before restoring any data.
CREATE TABLE ts_restore_target AS ts_external_good ENGINE = AggregatingMergeTree ORDER BY (id, bucket);
CREATE TABLE ts_restore_owner ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0 SAMPLES ts_restore_target;
BACKUP TABLE ts_restore_owner TO Memory('05243_ts_unsafe_restore') FORMAT Null;
DROP TABLE ts_restore_target;
CREATE TABLE ts_restore_target AS ts_external_good ENGINE = ReplacingMergeTree ORDER BY (id, bucket);
SELECT * FROM ts_restore_owner LIMIT 1; -- { serverError INVALID_SETTING_VALUE }
DROP TABLE ts_restore_owner;
RESTORE TABLE ts_restore_owner FROM Memory('05243_ts_unsafe_restore') FORMAT Null; -- { serverError INVALID_SETTING_VALUE }
DROP TABLE IF EXISTS ts_restore_owner;
DROP TABLE ts_restore_target;

DROP TABLE ts_external_good_owner;
DROP TABLE ts_external_good;
CREATE TABLE ts_plain ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER COLUMNS (samples Array(Tuple(DateTime64(3), Float64)))
SAMPLES INNER ENGINE = MergeTree;
DROP TABLE ts_plain;

-- Plain off-key dimensions are not safe under aggregate merges; aggregate measures are supported.
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER COLUMNS (extra UInt8); -- { serverError BAD_ARGUMENTS }
CREATE TABLE ts_bad ENGINE = TimeSeries
RECENT SAMPLES INNER COLUMNS (extra UInt8); -- { serverError BAD_ARGUMENTS }
CREATE TABLE ts_good ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER COLUMNS (extra SimpleAggregateFunction(sum, UInt64));
DROP TABLE ts_good;
CREATE TABLE ts_good ENGINE = TimeSeries
RECENT SAMPLES INNER COLUMNS (extra SimpleAggregateFunction(sum, UInt64));
DROP TABLE ts_good;
