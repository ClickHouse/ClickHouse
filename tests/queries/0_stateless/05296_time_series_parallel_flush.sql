-- Tags: no-async-insert, no-random-settings
-- A TimeSeries insert finishes the tags table, then writes the samples and recent samples tables.
-- A failed samples write leaves recent samples and dependent views empty.
-- Strict insert block limits still split a block that covers too many partitions.

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

SELECT '--- Memory engine target tables get the same rows ---';

CREATE TABLE mem_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = Memory;

CREATE TABLE mem_samples (id Tuple(UInt64, UUID), timestamp DateTime64(3), value Float64) ENGINE = Memory;
CREATE TABLE mem_recent (id Tuple(UInt64, UUID), timestamp DateTime64(3), value Float64) ENGINE = Memory;

CREATE TABLE ts_mem ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA mem_samples TAGS mem_tags RECENT SAMPLES mem_recent;

INSERT INTO ts_mem (metric_name, tags, samples) VALUES ('m', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 1.)]);

SELECT count() FROM mem_tags;
SELECT count() FROM mem_samples;
SELECT count() FROM mem_recent;

DROP VIEW ext_samples_mv;
DROP VIEW ts_outer_mv;
DROP TABLE ts_ext, ts_after, ext_tags, ext_samples, ext_recent, ext_samples_dest, ts_outer, ts_outer_dest, ts_mem, mem_tags, mem_samples, mem_recent;
DROP TABLE IF EXISTS ts_fail, ts_fail_mem, fail_tags, fail_samples, fail_recent, fail_mv_dest, mem_tags, mem_samples, mem_recent;

SELECT '--- a failed samples write leaves no recent rows and no view rows ---';

CREATE TABLE fail_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = MergeTree ORDER BY (metric_name, id);

CREATE TABLE fail_samples
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64,
    CONSTRAINT c CHECK value < 40
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE fail_recent
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE ts_fail ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA fail_samples TAGS fail_tags RECENT SAMPLES fail_recent;

CREATE TABLE fail_mv_dest (metric_name String) ENGINE = MergeTree ORDER BY metric_name;
CREATE MATERIALIZED VIEW fail_mv TO fail_mv_dest AS SELECT metric_name FROM ts_fail;

INSERT INTO ts_fail (metric_name, tags, samples) VALUES ('ok', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 1.)]);

INSERT INTO ts_fail (metric_name, tags, samples) VALUES ('bad', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 42.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM fail_tags WHERE metric_name = 'bad';
SELECT count() FROM fail_samples WHERE value >= 40;
SELECT count() FROM fail_recent WHERE value >= 40;
SELECT count() FROM fail_mv_dest WHERE metric_name = 'bad';
SELECT metric_name FROM fail_mv_dest ORDER BY metric_name;

SELECT '--- max_threads = 1 ---';

INSERT INTO ts_fail (metric_name, tags, samples) SETTINGS max_threads = 1 VALUES ('bad_sync', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 41.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM fail_samples WHERE value >= 40;
SELECT count() FROM fail_recent WHERE value >= 40;
SELECT count() FROM fail_mv_dest WHERE metric_name = 'bad_sync';

SELECT '--- Memory engine recent samples stay empty when samples fail ---';

CREATE TABLE mem_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = Memory;

CREATE TABLE mem_samples
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64,
    CONSTRAINT c CHECK value < 40
) ENGINE = Memory;

CREATE TABLE mem_recent (id Tuple(UInt64, UUID), timestamp DateTime64(3), value Float64) ENGINE = Memory;

CREATE TABLE ts_fail_mem ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA mem_samples TAGS mem_tags RECENT SAMPLES mem_recent;

INSERT INTO ts_fail_mem (metric_name, tags, samples) VALUES ('bad_mem', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 42.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM mem_samples WHERE value >= 40;
SELECT count() FROM mem_recent WHERE value >= 40;

SELECT '--- a later block failure leaves no recent rows for that block ---';

INSERT INTO ts_fail (metric_name, tags, samples)
SELECT if(number = 0, 'good_block', 'bad_block'), map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, if(number = 0, toFloat64(1), toFloat64(42)))]
FROM numbers(2)
SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0, max_insert_block_size = 1; -- { serverError VIOLATED_CONSTRAINT }

SELECT value FROM fail_samples ORDER BY value;
SELECT value FROM fail_recent ORDER BY value;
SELECT count() FROM fail_mv_dest WHERE metric_name = 'bad_block';

SELECT '--- a partitioned recent target keeps no rows when samples fail ---';

CREATE TABLE part_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = MergeTree ORDER BY (metric_name, id);

CREATE TABLE part_samples
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64,
    CONSTRAINT c CHECK sleepEachRow(1) = 0 AND value < 40
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE part_recent
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (id, timestamp)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE ts_part ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA part_samples TAGS part_tags RECENT SAMPLES part_recent;

INSERT INTO ts_part (metric_name, tags, samples)
SETTINGS max_insert_delayed_streams_for_parallel_write = 0
VALUES ('m', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 42.), (toDateTime64('2020-01-02 00:00:00', 3), 43.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM part_samples;
SELECT count() FROM part_recent;
SELECT count() FROM part_recent WHERE value = 42;

SELECT '--- max_insert_delayed_streams_for_parallel_write = 6 ---';

INSERT INTO ts_part (metric_name, tags, samples)
SETTINGS max_insert_delayed_streams_for_parallel_write = 6
VALUES ('m6', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 42.), (toDateTime64('2020-01-02 00:00:00', 3), 43.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM part_samples;
SELECT count() FROM part_recent;
SELECT count() FROM part_recent WHERE value = 42;

SELECT '--- a Buffer recent target keeps no rows when samples fail ---';

CREATE TABLE buf_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = MergeTree ORDER BY (metric_name, id);

CREATE TABLE buf_samples
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64,
    CONSTRAINT c CHECK value < 40
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE buf_dest
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE buf_recent
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = Buffer(currentDatabase(), buf_dest, 1, 86400, 86400, 10000000, 10000000, 100000000, 100000000);

CREATE TABLE ts_buf ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA buf_samples TAGS buf_tags RECENT SAMPLES buf_recent;

INSERT INTO ts_buf (metric_name, tags, samples)
VALUES ('m', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 42.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM buf_samples;
SELECT count() FROM buf_recent;
SELECT count() FROM buf_recent WHERE value = 42;

SELECT '--- a recent target stores a default column ---';

CREATE TABLE def_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = MergeTree ORDER BY (metric_name, id);

CREATE TABLE def_samples
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE def_recent
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64,
    extra UInt8 DEFAULT 7
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE ts_def ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA def_samples TAGS def_tags RECENT SAMPLES def_recent;

INSERT INTO ts_def (metric_name, tags, samples) VALUES ('m', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]);

SELECT extra FROM def_recent;

SELECT '--- a Distributed recent target keeps no rows when samples fail ---';

CREATE TABLE dist_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = MergeTree ORDER BY (metric_name, id);

CREATE TABLE dist_samples
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64,
    CONSTRAINT c CHECK sleepEachRow(1) = 0 AND value < 40
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE dist_recent_dest
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE dist_recent
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_recent_dest);

CREATE TABLE ts_dist ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA dist_samples TAGS dist_tags RECENT SAMPLES dist_recent;

INSERT INTO ts_dist (metric_name, tags, samples)
SETTINGS insert_distributed_sync = 1
VALUES ('m', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 42.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM dist_samples;
SELECT count() FROM dist_recent_dest;
SELECT count() FROM dist_recent_dest WHERE value = 42;

DROP VIEW fail_mv;
DROP TABLE ts_fail, ts_fail_mem, fail_tags, fail_samples, fail_recent, fail_mv_dest, mem_tags, mem_samples, mem_recent;
DROP TABLE ts_part, part_tags, part_samples, part_recent;
DROP TABLE ts_buf, buf_tags, buf_samples, buf_recent, buf_dest;
DROP TABLE ts_def, def_tags, def_samples, def_recent;
DROP TABLE ts_dist, dist_tags, dist_samples, dist_recent, dist_recent_dest;
DROP VIEW IF EXISTS view_recent_mv;
DROP TABLE IF EXISTS ts_view_barrier, view_barrier_tags, view_barrier_samples, view_barrier_recent, view_barrier_buf, view_barrier_mem;

SELECT '--- rejected sample ---';

CREATE TABLE view_barrier_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = MergeTree ORDER BY (metric_name, id);

CREATE TABLE view_barrier_samples
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64,
    CONSTRAINT c CHECK sleepEachRow(2) = 0 AND value < 40
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE view_barrier_recent
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE view_barrier_mem
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = Memory;

CREATE TABLE view_barrier_buf
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = Buffer(currentDatabase(), view_barrier_mem, 1, 1000, 1000, 1000, 1000, 1000000, 1000000);

CREATE MATERIALIZED VIEW view_recent_mv TO view_barrier_buf AS SELECT id, timestamp, value FROM view_barrier_recent;

CREATE TABLE ts_view_barrier ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA view_barrier_samples TAGS view_barrier_tags RECENT SAMPLES view_barrier_recent;

INSERT INTO ts_view_barrier (metric_name, tags, samples) SETTINGS max_threads = 4, parallel_view_processing = 1, max_block_size = 1 VALUES ('m', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 42.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM view_barrier_samples;
SELECT count() FROM view_barrier_recent;
SELECT count() FROM view_barrier_buf;

SELECT '--- accepted sample ---';

INSERT INTO ts_view_barrier (metric_name, tags, samples) SETTINGS max_threads = 4, parallel_view_processing = 1, max_block_size = 1 VALUES ('m', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]);

SELECT count() FROM view_barrier_samples;
SELECT count() FROM view_barrier_recent;
SELECT count() FROM view_barrier_buf;

DROP VIEW view_recent_mv;
DROP TABLE ts_view_barrier, view_barrier_tags, view_barrier_samples, view_barrier_recent, view_barrier_buf, view_barrier_mem;

-- Strict insert block limits are disabled for async inserts, so this section runs with synchronous inserts.
-- The samples table is partitioned by day, so a block with four days violates max_partitions_per_insert_block = 2
-- unless the inner insert splits it at max_insert_block_size = 2.
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
