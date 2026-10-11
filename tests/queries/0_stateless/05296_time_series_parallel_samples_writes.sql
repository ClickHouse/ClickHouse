-- A TimeSeries insert writes the samples and recent samples tables in background pipelines.
-- Tags commit first. Samples and recent samples are written in parallel after that.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts, ts_ext, ts_after, ext_tags, ext_samples, ext_recent;

SELECT '--- an insert writes the same rows to the tags, samples and recent samples tables ---';

-- TTL 10 days, and the samples are close to now(), so the background TTL cannot drop them during the test.
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('m', map('env', 'prod'), [(now64(3) - INTERVAL 3 MINUTE, 1.), (now64(3) - INTERVAL 2 MINUTE, 2.)]),
    ('m', map('env', 'dev'), [(now64(3) - INTERVAL 1 MINUTE, 3.)]);

SELECT metric_name, tags['env'] AS env FROM timeSeriesTags(ts) ORDER BY env;
SELECT value FROM timeSeriesSamples(ts) ORDER BY value;
SELECT sum(total_rows) FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%';

DROP TABLE ts;

SELECT '--- external target tables get the same rows ---';

CREATE TABLE ext_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3)),
    CONSTRAINT c CHECK metric_name != 'bad_tags'
) ENGINE = MergeTree ORDER BY (metric_name, id);

CREATE TABLE ext_samples (id Tuple(UInt64, UUID), timestamp DateTime64(3), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE ext_recent
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64,
    CONSTRAINT c CHECK value != 2000
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE ts_ext ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA ext_samples TAGS ext_tags RECENT SAMPLES ext_recent;

INSERT INTO ts_ext (metric_name, tags, samples) VALUES ('m', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 1.)]);

SELECT metric_name, tags['env'] AS env FROM ext_tags ORDER BY env;
SELECT value FROM ext_samples ORDER BY value;
SELECT value FROM ext_recent ORDER BY value;

SELECT '--- a failed tags write rejects the bad metric name ---';

INSERT INTO ts_ext (metric_name, tags, samples) VALUES ('bad_tags', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 10.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM ext_tags WHERE metric_name = 'bad_tags';
SELECT count() FROM ext_samples WHERE value = 10;
SELECT count() FROM ext_recent WHERE value = 10;

SELECT '--- a failed recent samples write rejects the bad value after the tags are committed ---';

INSERT INTO ts_ext (metric_name, tags, samples) VALUES ('bad_recent', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 2000.)]); -- { serverError VIOLATED_CONSTRAINT }

-- The samples table is written in parallel with the recent samples table, so its content is not checked here.
SELECT count() FROM ext_tags WHERE metric_name = 'bad_recent';
SELECT count() FROM ext_recent WHERE value = 2000;

SELECT '--- an insert after a failure still writes all three tables ---';

CREATE TABLE ts_after ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;

INSERT INTO ts_after (metric_name, tags, samples) VALUES ('m', map('env', 'dev'), [(now64(3) - INTERVAL 1 MINUTE, 5.)]);

SELECT metric_name, tags['env'] AS env, count() FROM timeSeriesTags(ts_after) GROUP BY metric_name, env ORDER BY env;
SELECT value FROM timeSeriesSamples(ts_after) ORDER BY value;
SELECT sum(total_rows) FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%';

SELECT '--- early block commit still rejects a bad metric name ---';

CREATE TABLE ext_samples_dest
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE MATERIALIZED VIEW ext_samples_mv TO ext_samples_dest AS SELECT * FROM ext_samples;

INSERT INTO ts_ext (metric_name, tags, samples) SETTINGS input_format_connection_handling = 1, input_format_max_block_wait_ms = 1 VALUES ('bad_tags', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 11.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM ext_tags WHERE metric_name = 'bad_tags';
SELECT count() FROM ext_samples WHERE value = 11;
SELECT count() FROM ext_recent WHERE value = 11;
SELECT count() FROM ext_samples_dest WHERE value = 11;

INSERT INTO ts_ext (metric_name, tags, samples) SETTINGS wait_for_part_commit_in_dependent_materialized_views = 1 VALUES ('bad_tags', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 12.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM ext_tags WHERE metric_name = 'bad_tags';
SELECT count() FROM ext_samples WHERE value = 12;
SELECT count() FROM ext_recent WHERE value = 12;
SELECT count() FROM ext_samples_dest WHERE value = 12;

SELECT '--- a materialized view on the time series table receives the inserted rows ---';

CREATE TABLE ts_outer ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;
CREATE TABLE ts_outer_dest (metric_name String) ENGINE = MergeTree ORDER BY metric_name;
CREATE MATERIALIZED VIEW ts_outer_mv TO ts_outer_dest AS SELECT metric_name FROM ts_outer;

INSERT INTO ts_outer (metric_name, tags, samples) VALUES ('mv', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 7.)]);

SELECT metric_name FROM ts_outer_dest;

DROP VIEW ext_samples_mv;
DROP VIEW ts_outer_mv;
DROP TABLE ts_ext, ts_after, ext_tags, ext_samples, ext_recent, ext_samples_dest, ts_outer, ts_outer_dest;
