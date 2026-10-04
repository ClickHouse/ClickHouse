-- Tags: no-replicated-database
-- Tag no-replicated-database: `DatabaseReplicated` does not drop `TimeSeries` inner tables synchronously; deferred DROPs are rejected.

-- The samples expired by the `TTL ... DELETE` of an external recent samples table are not written to it,
-- but the tables which the materialized views over it write to keep their own TTL semantics.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_views;
DROP TABLE IF EXISTS mv_recent;
DROP TABLE IF EXISTS mv_recent_ttl;
DROP TABLE IF EXISTS audit;
DROP TABLE IF EXISTS recent_ttl;
DROP TABLE IF EXISTS recent_no_ttl;

-- `audit` expires a sample after 1 day; its TTL merges are stopped, so only an insert-time filter could drop a row there.
CREATE TABLE audit (id Tuple(UInt64, UUID), timestamp DateTime64(3), value Float64, source String)
ENGINE = MergeTree ORDER BY (id, timestamp) TTL toDateTime(timestamp) + INTERVAL 1 DAY;
SYSTEM STOP TTL MERGES audit;

-- An external recent samples table with a 3-day TTL and another one without a TTL.
CREATE TABLE recent_ttl (id Tuple(UInt64, UUID), timestamp DateTime64(3), value Float64)
ENGINE = MergeTree ORDER BY (id, timestamp) TTL toDateTime(timestamp) + INTERVAL 3 DAY;
SYSTEM STOP TTL MERGES recent_ttl;

CREATE TABLE recent_no_ttl (id Tuple(UInt64, UUID), timestamp DateTime64(3), value Float64)
ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE MATERIALIZED VIEW mv_recent_ttl TO audit AS SELECT id, timestamp, value, 'ttl' AS source FROM recent_ttl;
CREATE MATERIALIZED VIEW mv_recent TO audit AS SELECT id, timestamp, value, 'no_ttl' AS source FROM recent_no_ttl;

CREATE TABLE ts_views ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000 RECENT SAMPLES recent_ttl;

INSERT INTO ts_views (metric_name, tags, samples) VALUES
    ('m', map('env', 'prod'), [(now64(3) - INTERVAL 10 DAY, 1.), (now64(3) - INTERVAL 2 DAY, 2.), (now64(3) - INTERVAL 1 MINUTE, 3.)]);

SELECT '-- the recent samples table drops the sample its TTL expired, the view target keeps the others';
SELECT value FROM recent_ttl ORDER BY value;
SELECT source, value FROM audit ORDER BY source, value;

DROP TABLE ts_views;
TRUNCATE TABLE audit;

CREATE TABLE ts_views ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000 RECENT SAMPLES recent_no_ttl;

INSERT INTO ts_views (metric_name, tags, samples) VALUES
    ('m', map('env', 'prod'), [(now64(3) - INTERVAL 10 DAY, 1.), (now64(3) - INTERVAL 2 DAY, 2.), (now64(3) - INTERVAL 1 MINUTE, 3.)]);

SELECT '-- a recent samples table without a TTL gets every sample, and so does the view target';
SELECT value FROM recent_no_ttl ORDER BY value;
SELECT source, value FROM audit ORDER BY source, value;

DROP TABLE ts_views;
DROP TABLE mv_recent;
DROP TABLE mv_recent_ttl;
DROP TABLE audit;
DROP TABLE recent_ttl;
DROP TABLE recent_no_ttl;
