-- A TimeSeries insert writes tags, samples, and recent samples on the insert executor.
-- Each inner table commits when its own flush returns.

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
CREATE TABLE ext_recent (id Tuple(UInt64, UUID), timestamp DateTime64(3), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE ts_ext ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA ext_samples TAGS ext_tags RECENT SAMPLES ext_recent;

INSERT INTO ts_ext (metric_name, tags, samples) VALUES ('m', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 1.)]);

SELECT metric_name, tags['env'] AS env FROM ext_tags ORDER BY env;
SELECT value FROM ext_samples ORDER BY value;
SELECT value FROM ext_recent ORDER BY value;

SELECT '--- a failed tags write rejects the bad metric name ---';

INSERT INTO ts_ext (metric_name, tags, samples) VALUES ('bad_tags', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 10.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM ext_tags WHERE metric_name = 'bad_tags';

SELECT '--- an insert after a failure still writes all three tables ---';

CREATE TABLE ts_after ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;

INSERT INTO ts_after (metric_name, tags, samples) VALUES ('m', map('env', 'dev'), [(now64(3) - INTERVAL 1 MINUTE, 5.)]);

SELECT metric_name, tags['env'] AS env, count() FROM timeSeriesTags(ts_after) GROUP BY metric_name, env ORDER BY env;
SELECT value FROM timeSeriesSamples(ts_after) ORDER BY value;
SELECT sum(total_rows) FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%';

DROP TABLE ts_ext, ts_after, ext_tags, ext_samples, ext_recent;
