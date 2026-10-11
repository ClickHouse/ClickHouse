-- A target sink throws inside `consume` when one block spans two partitions and
-- `max_partitions_per_insert_block = 1`. Tags commit before samples and recent samples are written.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET max_partitions_per_insert_block = 1;

DROP TABLE IF EXISTS ts_ext;
DROP TABLE IF EXISTS ext_tags;
DROP TABLE IF EXISTS ext_samples;
DROP TABLE IF EXISTS ext_recent;

CREATE TABLE ext_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = MergeTree PARTITION BY metric_name ORDER BY (metric_name, id);

CREATE TABLE ext_samples (id Tuple(UInt64, UUID), timestamp DateTime64(3), value Float64)
    ENGINE = MergeTree PARTITION BY (value > 100) ORDER BY (id, timestamp);

CREATE TABLE ext_recent (id Tuple(UInt64, UUID), timestamp DateTime64(3), value Float64)
    ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE ts_ext ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA ext_samples TAGS ext_tags RECENT SAMPLES ext_recent;

SELECT '--- the tags sink throws: no table gets the block ---';

INSERT INTO ts_ext (metric_name, tags, samples) VALUES
    ('tags_fail_a', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 10.)]),
    ('tags_fail_b', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 11.)]); -- { serverError TOO_MANY_PARTS }

SELECT count() FROM ext_tags WHERE metric_name LIKE 'tags_fail%';
SELECT count() FROM ext_samples WHERE value IN (10, 11);
SELECT count() FROM ext_recent WHERE value IN (10, 11);

SELECT '--- the samples sink throws: tags are committed, samples are empty ---';

INSERT INTO ts_ext (metric_name, tags, samples) VALUES
    ('samples_fail', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 20.), (now64(3) - INTERVAL 2 MINUTE, 200.)]); -- { serverError TOO_MANY_PARTS }

SELECT count() FROM ext_tags WHERE metric_name = 'samples_fail';
SELECT count() FROM ext_samples WHERE value IN (20, 200);

SELECT '--- no sink throws: every table gets the block ---';

INSERT INTO ts_ext (metric_name, tags, samples) VALUES
    ('ok', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 30.), (now64(3) - INTERVAL 2 MINUTE, 31.)]);

SELECT count() FROM ext_tags WHERE metric_name = 'ok';
SELECT count() FROM ext_samples WHERE value IN (30, 31);
SELECT count() FROM ext_recent WHERE value IN (30, 31);

DROP TABLE ts_ext;
DROP TABLE ext_tags;
DROP TABLE ext_samples;
DROP TABLE ext_recent;
