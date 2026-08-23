-- Tags: no-replicated-database
-- Tag no-replicated-database: `DatabaseReplicated` does not drop `TimeSeries` inner tables synchronously; deferred DROPs are rejected.

-- When the samples / recent samples pair degrades because one target lacks `is_stale_marker`, the
-- target that still has the column must receive explicit zeros: omitting the column would
-- materialize that external table's DEFAULT, which needs not be 0, so ordinary samples could be
-- marked stale on one side of the pair only (see TimeSeriesSink.cpp).

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_tags;
DROP TABLE IF EXISTS ts_samples;
DROP TABLE IF EXISTS ts_recent;

CREATE TABLE ts_tags
(
    id UInt64,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time DateTime64(3),
    max_time DateTime64(3)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE ts_samples
(
    id UInt64,
    timestamp DateTime64(3),
    value Float64,
    is_stale_marker UInt8 DEFAULT 1
) ENGINE = MergeTree() ORDER BY (id, timestamp);

CREATE TABLE ts_recent
(
    id UInt64,
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree() ORDER BY (id, timestamp);

CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000 SAMPLES ts_samples TAGS ts_tags RECENT SAMPLES ts_recent;

SELECT '-- a legacy recent table degrades the pair; the samples table gets explicit zeros, not its DEFAULT 1';

INSERT INTO ts (metric_name, tags, time_series) VALUES
    ('m', map('env', 'prod'), [(now64(3) - INTERVAL 3 MINUTE, 42.), (now64(3) - INTERVAL 2 MINUTE, 43.)]);

SELECT count(), sum(is_stale_marker) FROM ts_samples;
SELECT count() FROM ts_recent;

DROP TABLE ts;
DROP TABLE ts_samples;
DROP TABLE ts_recent;

SELECT '-- mirrored: a legacy samples table with a flag-carrying recent table, explicit zeros again';

CREATE TABLE ts_samples
(
    id UInt64,
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree() ORDER BY (id, timestamp);

CREATE TABLE ts_recent
(
    id UInt64,
    timestamp DateTime64(3),
    value Float64,
    is_stale_marker UInt8 DEFAULT 1
) ENGINE = MergeTree() ORDER BY (id, timestamp);

CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000 SAMPLES ts_samples TAGS ts_tags RECENT SAMPLES ts_recent;

INSERT INTO ts (metric_name, tags, time_series) VALUES
    ('m', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 7.)]);

SELECT count() FROM ts_samples;
SELECT count(), sum(is_stale_marker) FROM ts_recent;

DROP TABLE ts;
DROP TABLE ts_tags;
DROP TABLE ts_samples;
DROP TABLE ts_recent;
