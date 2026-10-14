-- Tags: no-async-insert
-- Strict insert block limits are disabled for async inserts, so the test runs with synchronous inserts.
-- The samples table is partitioned by day, so a block with four days violates max_partitions_per_insert_block = 2
-- unless the inner insert splits it at max_insert_block_size = 2.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET use_strict_insert_block_limits = 1, max_insert_block_size = 2, min_insert_block_size_rows = 2, min_insert_block_size_bytes = 0, max_partitions_per_insert_block = 2;

DROP TABLE IF EXISTS ts_strict, ts_strict_recent, strict_tags, strict_samples, strict_recent;

CREATE TABLE strict_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = MergeTree ORDER BY (metric_name, id);

CREATE TABLE strict_samples (id Tuple(UInt64, UUID), timestamp DateTime64(3), value Float64)
ENGINE = MergeTree PARTITION BY toDate(timestamp) ORDER BY (id, timestamp);

CREATE TABLE ts_strict ENGINE = TimeSeries DATA strict_samples TAGS strict_tags;

SELECT '--- four days in one series are written as blocks of two rows ---';

INSERT INTO ts_strict (metric_name, tags, samples) VALUES
    ('m', map('env', 'prod'), [('2026-01-01 00:00:00.000', 1.), ('2026-01-02 00:00:00.000', 2.), ('2026-01-03 00:00:00.000', 3.), ('2026-01-04 00:00:00.000', 4.)]);

SELECT count() FROM strict_samples;

SELECT '--- the same insert with one thread ---';

TRUNCATE TABLE strict_samples;

INSERT INTO ts_strict (metric_name, tags, samples) SETTINGS max_threads = 1 VALUES
    ('m', map('env', 'prod'), [('2026-01-01 00:00:00.000', 1.), ('2026-01-02 00:00:00.000', 2.), ('2026-01-03 00:00:00.000', 3.), ('2026-01-04 00:00:00.000', 4.)]);

SELECT count() FROM strict_samples;

SELECT '--- the same insert with a recent samples table ---';

DROP TABLE ts_strict;
TRUNCATE TABLE strict_samples;

CREATE TABLE strict_recent (id Tuple(UInt64, UUID), timestamp DateTime64(3), value Float64)
ENGINE = MergeTree PARTITION BY toDate(timestamp) ORDER BY (id, timestamp);

CREATE TABLE ts_strict_recent ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA strict_samples TAGS strict_tags RECENT SAMPLES strict_recent;

INSERT INTO ts_strict_recent (metric_name, tags, samples) VALUES
    ('m', map('env', 'prod'), [('2026-01-01 00:00:00.000', 1.), ('2026-01-02 00:00:00.000', 2.), ('2026-01-03 00:00:00.000', 3.), ('2026-01-04 00:00:00.000', 4.)]);

SELECT count() FROM strict_samples;
SELECT count() FROM strict_recent;

DROP TABLE ts_strict_recent, strict_tags, strict_samples, strict_recent;
