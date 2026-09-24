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
