SET allow_experimental_time_series_table = 1;

-- Aggregate merges must distinguish every `(id, bucket)` pair.
CREATE TABLE ts_bucketed_unsafe_key
(
    id UUID,
    samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(DateTime64(3), Float64))),
    bucket DateTime64(3),
    min_time SimpleAggregateFunction(min, DateTime64(3)),
    max_time SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree ORDER BY id SETTINGS allow_dimensions_outside_sorting_key = 1;

CREATE TABLE ts_bucketed_unsafe_owner ENGINE = TimeSeries
SETTINGS version = 7, recent_samples_ttl_seconds = 0
SAMPLES ts_bucketed_unsafe_key; -- { serverError INVALID_SETTING_VALUE }

CREATE TABLE ts_bucketed_unsafe_inline ENGINE = TimeSeries
SETTINGS version = 7, recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = AggregatingMergeTree ORDER BY id SETTINGS allow_dimensions_outside_sorting_key = 1; -- { serverError INVALID_SETTING_VALUE }

DROP TABLE IF EXISTS ts_bucketed_unsafe_inline;
DROP TABLE IF EXISTS ts_bucketed_unsafe_owner;
DROP TABLE ts_bucketed_unsafe_key;

-- Referencing `bucket` through a lossy function is not enough to keep buckets separate.
CREATE TABLE ts_bucketed_lossy_key
(
    id UUID,
    samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(DateTime64(3), Float64))),
    bucket DateTime64(3),
    min_time SimpleAggregateFunction(min, DateTime64(3)),
    max_time SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree ORDER BY (id, toDate(bucket)) SETTINGS allow_dimensions_outside_sorting_key = 1;

CREATE TABLE ts_bucketed_lossy_owner ENGINE = TimeSeries
SETTINGS version = 7, recent_samples_ttl_seconds = 0
SAMPLES ts_bucketed_lossy_key; -- { serverError INVALID_SETTING_VALUE }

DROP TABLE IF EXISTS ts_bucketed_lossy_owner;
DROP TABLE ts_bucketed_lossy_key;

-- The generated and explicit inner aggregation keys must still be accepted.
CREATE TABLE ts_bucketed_default_owner ENGINE = TimeSeries
SETTINGS version = 7, recent_samples_ttl_seconds = 0;
SELECT count() FROM ts_bucketed_default_owner;

CREATE TABLE ts_bucketed_safe_inline ENGINE = TimeSeries
SETTINGS version = 7, recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = AggregatingMergeTree ORDER BY (id, bucket);
SELECT count() FROM ts_bucketed_safe_inline;

CREATE TABLE ts_bucketed_primary_only_inline ENGINE = TimeSeries
SETTINGS version = 7, recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = AggregatingMergeTree PRIMARY KEY (id, bucket);
SELECT count() FROM ts_bucketed_primary_only_inline;

DROP TABLE ts_bucketed_primary_only_inline;
DROP TABLE ts_bucketed_safe_inline;
DROP TABLE ts_bucketed_default_owner;

CREATE TABLE ts_bucketed_safe_key
(
    id UUID,
    samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(DateTime64(3), Float64))),
    bucket DateTime64(3),
    min_time SimpleAggregateFunction(min, DateTime64(3)),
    max_time SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree ORDER BY (id, bucket);

CREATE TABLE ts_bucketed_safe_owner ENGINE = TimeSeries
SETTINGS version = 7, recent_samples_ttl_seconds = 0
SAMPLES ts_bucketed_safe_key;
SELECT count() FROM ts_bucketed_safe_owner;

-- A different direct-column order remains safe, though it does not use the ordered native read.
CREATE TABLE ts_bucketed_reversed_key AS ts_bucketed_safe_key
ENGINE = AggregatingMergeTree ORDER BY (bucket, id);
CREATE TABLE ts_bucketed_reversed_owner ENGINE = TimeSeries
SETTINGS version = 7, recent_samples_ttl_seconds = 0
SAMPLES ts_bucketed_reversed_key;
SELECT count() FROM ts_bucketed_reversed_owner;

CREATE TABLE ts_bucketed_desc_key AS ts_bucketed_safe_key
ENGINE = AggregatingMergeTree ORDER BY (id DESC, bucket) SETTINGS allow_experimental_reverse_key = 1;
CREATE TABLE ts_bucketed_desc_owner ENGINE = TimeSeries
SETTINGS version = 7, recent_samples_ttl_seconds = 0
SAMPLES ts_bucketed_desc_key;
SELECT count() FROM ts_bucketed_desc_owner;

-- A local `Distributed` definition cannot prove that its remote engine preserves bucketed rows.
CREATE TABLE ts_bucketed_proxy AS ts_bucketed_safe_key
ENGINE = Distributed(test_shard_localhost, currentDatabase(), ts_bucketed_safe_key);
CREATE TABLE ts_bucketed_proxy_owner ENGINE = TimeSeries
SETTINGS version = 7, recent_samples_ttl_seconds = 0
SAMPLES ts_bucketed_proxy; -- { serverError INVALID_SETTING_VALUE }

DROP TABLE IF EXISTS ts_bucketed_proxy_owner;
DROP TABLE ts_bucketed_proxy;
DROP TABLE ts_bucketed_desc_owner;
DROP TABLE ts_bucketed_desc_key;
DROP TABLE ts_bucketed_reversed_owner;
DROP TABLE ts_bucketed_reversed_key;
DROP TABLE ts_bucketed_safe_owner;
DROP TABLE ts_bucketed_safe_key;
