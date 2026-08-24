-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: `timeSeriesSelector` parses its PromQL selector with ANTLR4, which is disabled in the fast-test build.
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
DROP TABLE ts_samples;
DROP TABLE ts_recent;

SELECT '-- pre-existing rows in a mixed pair read the same through both paths';

-- The write-side degrade cannot fix rows that were in the tables before the pair was attached; the
-- read side must degrade table-wide too, or the same query flips between "stale marker" and
-- "ordinary NaN sample" depending only on which table the recent-samples preference picks.
DROP TABLE ts_tags;

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
    is_stale_marker UInt8
) ENGINE = MergeTree() ORDER BY (id, timestamp);

CREATE TABLE ts_recent
(
    id UInt64,
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree() ORDER BY (id, timestamp);

INSERT INTO ts_tags (id, metric_name, tags, min_time, max_time) VALUES
    (101, 'm', map(), now64(3) - INTERVAL 10 MINUTE, now64(3));
INSERT INTO ts_samples (id, timestamp, value, is_stale_marker) VALUES
    (101, now64(3) - INTERVAL 3 MINUTE, 1., 0),
    (101, now64(3) - INTERVAL 2 MINUTE, nan, 1);
INSERT INTO ts_recent (id, timestamp, value) VALUES
    (101, now64(3) - INTERVAL 3 MINUTE, 1.),
    (101, now64(3) - INTERVAL 2 MINUTE, nan);

CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000 SAMPLES ts_samples TAGS ts_tags RECENT SAMPLES ts_recent;

SELECT count(), sum(is_stale_marker) FROM timeSeriesSelector(ts, 'm', now() - INTERVAL 10 MINUTE, now());
SELECT count(), sum(is_stale_marker) FROM timeSeriesSelector(ts, 'm', now() - INTERVAL 10 MINUTE, now())
SETTINGS time_series_prefer_recent_samples_table = 0;

DROP TABLE ts;
DROP TABLE ts_tags;
DROP TABLE ts_samples;
DROP TABLE ts_recent;

SELECT '-- a range older than the recent TTL window reads only samples and honors its real flags';

-- Only a range the recent table could also serve can diverge between the read paths; a historical
-- query reads `samples` under either preference, so a legacy sibling must not hide its flags.
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
    is_stale_marker UInt8
) ENGINE = MergeTree() ORDER BY (id, timestamp);

CREATE TABLE ts_recent
(
    id UInt64,
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree() ORDER BY (id, timestamp);

INSERT INTO ts_tags (id, metric_name, tags, min_time, max_time) VALUES
    (201, 'm', map(), now64(3) - INTERVAL 3 HOUR, now64(3) - INTERVAL 1 HOUR);
INSERT INTO ts_samples (id, timestamp, value, is_stale_marker) VALUES
    (201, now64(3) - INTERVAL 2 HOUR, 1., 0),
    (201, now64(3) - INTERVAL 119 MINUTE, nan, 1);

CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 600 SAMPLES ts_samples TAGS ts_tags RECENT SAMPLES ts_recent;

SELECT count(), sum(is_stale_marker) FROM timeSeriesSelector(ts, 'm', now() - INTERVAL 3 HOUR, now() - INTERVAL 1 HOUR);
SELECT count(), sum(is_stale_marker) FROM timeSeriesSelector(ts, 'm', now() - INTERVAL 3 HOUR, now() - INTERVAL 1 HOUR)
SETTINGS time_series_prefer_recent_samples_table = 0;

DROP TABLE ts;
DROP TABLE ts_tags;
DROP TABLE ts_samples;
DROP TABLE ts_recent;

SELECT '-- a range crossing the recent TTL boundary degrades like one wholly inside it';

-- The same row must read the same way however far back the query starts. Keying the degrade off
-- "the whole range fits the window" made the marker at now() - 5m ordinary for the wide range and
-- stale for the narrow one, even though both read it from `samples`.
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
    is_stale_marker UInt8
) ENGINE = MergeTree() ORDER BY (id, timestamp);

CREATE TABLE ts_recent
(
    id UInt64,
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree() ORDER BY (id, timestamp);

INSERT INTO ts_tags (id, metric_name, tags, min_time, max_time) VALUES
    (202, 'm', map(), now64(3) - INTERVAL 6 MINUTE, now64(3) - INTERVAL 5 MINUTE);
INSERT INTO ts_samples (id, timestamp, value, is_stale_marker) VALUES
    (202, now64(3) - INTERVAL 6 MINUTE, 1., 0),
    (202, now64(3) - INTERVAL 5 MINUTE, nan, 1);

CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 3600 SAMPLES ts_samples TAGS ts_tags RECENT SAMPLES ts_recent;

SELECT count(), sum(is_stale_marker) FROM timeSeriesSelector(ts, 'm', now() - INTERVAL 30 MINUTE, now())
SETTINGS time_series_prefer_recent_samples_table = 0;
SELECT count(), sum(is_stale_marker) FROM timeSeriesSelector(ts, 'm', now() - INTERVAL 2 HOUR, now())
SETTINGS time_series_prefer_recent_samples_table = 0;

DROP TABLE ts;
DROP TABLE ts_tags;
DROP TABLE ts_samples;
DROP TABLE ts_recent;
