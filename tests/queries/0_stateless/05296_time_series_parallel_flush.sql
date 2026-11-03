-- Tags: no-async-insert, no-random-settings
-- A TimeSeries insert finishes the tags table, then writes the samples and recent samples tables.
-- A failed samples write leaves recent samples and dependent views empty.
-- Strict insert block limits still split a block that covers too many partitions.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts, ts_ext, ts_after, ts_outer, ts_outer_dest, ext_samples_dest, ts_fail, fail_mv_dest, ts_part, ts_def, ts_view_barrier, view_dest, ts_strict, ts_strict_recent;
DROP VIEW IF EXISTS ext_samples_mv, ts_outer_mv, fail_mv, view_recent_mv;

SELECT '--- an insert writes the same rows to the tags, samples and recent samples tables ---';

-- TTL 10 days. The samples stay inside the current hour, so the background TTL cannot drop them during the test.
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('m', map('env', 'prod'), [(toDateTime64(toStartOfHour(now64(3)), 3) + INTERVAL 1 MINUTE, 1.), (toDateTime64(toStartOfHour(now64(3)), 3) + INTERVAL 2 MINUTE, 2.)]),
    ('m', map('env', 'dev'), [(toDateTime64(toStartOfHour(now64(3)), 3) + INTERVAL 3 MINUTE, 3.)]);

SELECT metric_name, tags['env'] AS env FROM timeSeriesTags(ts) ORDER BY env;
SELECT arrayJoin(arrayMap(x -> x.2, samples)) AS value FROM timeSeriesSamples(ts) ORDER BY value;
SELECT count() FROM merge(currentDatabase(), '^\\.inner_id\\.recentsamples\\.');

DROP TABLE ts;

SELECT '--- inner target tables get the same rows ---';

CREATE TABLE ts_ext UUID '05296000-0000-4000-8000-000000000001' ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;

ALTER TABLE `.inner_id.tags.05296000-0000-4000-8000-000000000001` ADD CONSTRAINT c CHECK metric_name != 'bad_tags';
ALTER TABLE `.inner_id.recentsamples.05296000-0000-4000-8000-000000000001` ADD CONSTRAINT c CHECK arrayAll(x -> x.2 != 2000, samples);

INSERT INTO ts_ext (metric_name, tags, samples) VALUES ('m', map('env', 'prod'), [(toDateTime64(toStartOfHour(now64(3)), 3) + INTERVAL 1 MINUTE, 1.)]);

SELECT metric_name, tags['env'] AS env FROM `.inner_id.tags.05296000-0000-4000-8000-000000000001` ORDER BY env;
SELECT arrayJoin(arrayMap(x -> x.2, samples)) AS value FROM `.inner_id.samples.05296000-0000-4000-8000-000000000001` ORDER BY value;
SELECT arrayJoin(arrayMap(x -> x.2, samples)) AS value FROM `.inner_id.recentsamples.05296000-0000-4000-8000-000000000001` ORDER BY value;

SELECT '--- a failed tags write rejects the bad metric name ---';

INSERT INTO ts_ext (metric_name, tags, samples) VALUES ('bad_tags', map('env', 'prod'), [(toDateTime64(toStartOfHour(now64(3)), 3) + INTERVAL 1 MINUTE, 10.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM `.inner_id.tags.05296000-0000-4000-8000-000000000001` WHERE metric_name = 'bad_tags';
SELECT count() FROM `.inner_id.samples.05296000-0000-4000-8000-000000000001` WHERE arrayExists(x -> x.2 = 10, samples);
SELECT count() FROM `.inner_id.recentsamples.05296000-0000-4000-8000-000000000001` WHERE arrayExists(x -> x.2 = 10, samples);

SELECT '--- a failed recent samples write rejects the bad value after the tags are committed ---';

INSERT INTO ts_ext (metric_name, tags, samples) VALUES ('bad_recent', map('env', 'prod'), [(toDateTime64(toStartOfHour(now64(3)), 3) + INTERVAL 1 MINUTE, 2000.)]); -- { serverError VIOLATED_CONSTRAINT }

-- The samples table is written in parallel with the recent samples table, so its content is not checked here.
SELECT count() FROM `.inner_id.tags.05296000-0000-4000-8000-000000000001` WHERE metric_name = 'bad_recent';
SELECT count() FROM `.inner_id.recentsamples.05296000-0000-4000-8000-000000000001` WHERE arrayExists(x -> x.2 = 2000, samples);

SELECT '--- an insert after a failure still writes all three tables ---';

CREATE TABLE ts_after UUID '05296000-0000-4000-8000-000000000002' ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;

INSERT INTO ts_after (metric_name, tags, samples) VALUES ('m', map('env', 'dev'), [(toDateTime64(toStartOfHour(now64(3)), 3) + INTERVAL 1 MINUTE, 5.)]);

SELECT metric_name, tags['env'] AS env, count() FROM timeSeriesTags(ts_after) GROUP BY metric_name, env ORDER BY env;
SELECT arrayJoin(arrayMap(x -> x.2, samples)) AS value FROM timeSeriesSamples(ts_after) ORDER BY value;
SELECT count() FROM `.inner_id.recentsamples.05296000-0000-4000-8000-000000000002`;

SELECT '--- early block commit still rejects a bad metric name ---';

CREATE TABLE ext_samples_dest (bucket DateTime64(3)) ENGINE = MergeTree ORDER BY bucket;
CREATE MATERIALIZED VIEW ext_samples_mv TO ext_samples_dest AS SELECT bucket FROM `.inner_id.samples.05296000-0000-4000-8000-000000000001`;

INSERT INTO ts_ext (metric_name, tags, samples) SETTINGS input_format_connection_handling = 1, input_format_max_block_wait_ms = 1 VALUES ('bad_tags', map('env', 'prod'), [(toDateTime64(toStartOfHour(now64(3)), 3) + INTERVAL 1 MINUTE, 11.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM `.inner_id.tags.05296000-0000-4000-8000-000000000001` WHERE metric_name = 'bad_tags';
SELECT count() FROM `.inner_id.samples.05296000-0000-4000-8000-000000000001` WHERE arrayExists(x -> x.2 = 11, samples);
SELECT count() FROM `.inner_id.recentsamples.05296000-0000-4000-8000-000000000001` WHERE arrayExists(x -> x.2 = 11, samples);
SELECT count() FROM ext_samples_dest;

INSERT INTO ts_ext (metric_name, tags, samples) SETTINGS wait_for_part_commit_in_dependent_materialized_views = 1 VALUES ('bad_tags', map('env', 'prod'), [(toDateTime64(toStartOfHour(now64(3)), 3) + INTERVAL 1 MINUTE, 12.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM `.inner_id.tags.05296000-0000-4000-8000-000000000001` WHERE metric_name = 'bad_tags';
SELECT count() FROM `.inner_id.samples.05296000-0000-4000-8000-000000000001` WHERE arrayExists(x -> x.2 = 12, samples);
SELECT count() FROM `.inner_id.recentsamples.05296000-0000-4000-8000-000000000001` WHERE arrayExists(x -> x.2 = 12, samples);
SELECT count() FROM ext_samples_dest;

SELECT '--- a materialized view on the time series table receives the inserted rows ---';

CREATE TABLE ts_outer ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;
CREATE TABLE ts_outer_dest (metric_name String) ENGINE = MergeTree ORDER BY metric_name;
CREATE MATERIALIZED VIEW ts_outer_mv TO ts_outer_dest AS SELECT metric_name FROM ts_outer;

INSERT INTO ts_outer (metric_name, tags, samples) VALUES ('mv', map('env', 'prod'), [(toDateTime64(toStartOfHour(now64(3)), 3) + INTERVAL 1 MINUTE, 7.)]);

SELECT metric_name FROM ts_outer_dest;

DROP VIEW ext_samples_mv;
DROP VIEW ts_outer_mv;
DROP TABLE ts_ext, ts_after, ext_samples_dest, ts_outer, ts_outer_dest;

SELECT '--- a failed samples write leaves no recent rows and no view rows ---';

CREATE TABLE ts_fail UUID '05296000-0000-4000-8000-000000000003' ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;
ALTER TABLE `.inner_id.samples.05296000-0000-4000-8000-000000000003` ADD CONSTRAINT c CHECK arrayAll(x -> x.2 < 40, samples);

CREATE TABLE fail_mv_dest (metric_name String) ENGINE = MergeTree ORDER BY metric_name;
CREATE MATERIALIZED VIEW fail_mv TO fail_mv_dest AS SELECT metric_name FROM ts_fail;

INSERT INTO ts_fail (metric_name, tags, samples) VALUES ('ok', map('env', 'prod'), [(toDateTime64(toStartOfHour(now64(3)), 3) + INTERVAL 1 MINUTE, 1.)]);

INSERT INTO ts_fail (metric_name, tags, samples) VALUES ('bad', map('env', 'prod'), [(toDateTime64(toStartOfHour(now64(3)), 3) + INTERVAL 1 MINUTE, 42.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM `.inner_id.tags.05296000-0000-4000-8000-000000000003` WHERE metric_name = 'bad';
SELECT count() FROM `.inner_id.samples.05296000-0000-4000-8000-000000000003` WHERE arrayExists(x -> x.2 >= 40, samples);
SELECT count() FROM `.inner_id.recentsamples.05296000-0000-4000-8000-000000000003` WHERE arrayExists(x -> x.2 >= 40, samples);
SELECT count() FROM fail_mv_dest WHERE metric_name = 'bad';
SELECT metric_name FROM fail_mv_dest ORDER BY metric_name;

SELECT '--- max_threads = 1 ---';

INSERT INTO ts_fail (metric_name, tags, samples) SETTINGS max_threads = 1 VALUES ('bad_sync', map('env', 'prod'), [(toDateTime64(toStartOfHour(now64(3)), 3) + INTERVAL 1 MINUTE, 41.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM `.inner_id.samples.05296000-0000-4000-8000-000000000003` WHERE arrayExists(x -> x.2 >= 40, samples);
SELECT count() FROM `.inner_id.recentsamples.05296000-0000-4000-8000-000000000003` WHERE arrayExists(x -> x.2 >= 40, samples);
SELECT count() FROM fail_mv_dest WHERE metric_name = 'bad_sync';

SELECT '--- a later block failure leaves no recent rows for that block ---';

INSERT INTO ts_fail (metric_name, tags, samples)
SELECT if(number = 0, 'good_block', 'bad_block'), map('env', 'prod'), [(toDateTime64(toStartOfHour(now64(3)), 3) + INTERVAL 1 MINUTE, if(number = 0, toFloat64(1), toFloat64(42)))]
FROM numbers(2)
SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0, max_insert_block_size = 1; -- { serverError VIOLATED_CONSTRAINT }

SELECT arrayJoin(arrayMap(x -> x.2, samples)) AS value FROM `.inner_id.samples.05296000-0000-4000-8000-000000000003` ORDER BY value;
SELECT arrayJoin(arrayMap(x -> x.2, samples)) AS value FROM `.inner_id.recentsamples.05296000-0000-4000-8000-000000000003` ORDER BY value;
SELECT count() FROM fail_mv_dest WHERE metric_name = 'bad_block';

SELECT '--- a partitioned recent target keeps no rows when samples fail ---';

CREATE TABLE ts_part UUID '05296000-0000-4000-8000-000000000004' ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 864000, recent_samples_partition_by = 'toDate(bucket)';

ALTER TABLE `.inner_id.samples.05296000-0000-4000-8000-000000000004` ADD CONSTRAINT c CHECK sleepEachRow(1) = 0 AND arrayAll(x -> x.2 < 40, samples);

INSERT INTO ts_part (metric_name, tags, samples)
SETTINGS max_insert_delayed_streams_for_parallel_write = 0
VALUES ('m', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 42.), (toDateTime64('2020-01-02 00:00:00', 3), 43.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM `.inner_id.samples.05296000-0000-4000-8000-000000000004`;
SELECT count() FROM `.inner_id.recentsamples.05296000-0000-4000-8000-000000000004`;
SELECT count() FROM `.inner_id.recentsamples.05296000-0000-4000-8000-000000000004` WHERE arrayExists(x -> x.2 = 42, samples);

SELECT '--- max_insert_delayed_streams_for_parallel_write = 6 ---';

INSERT INTO ts_part (metric_name, tags, samples)
SETTINGS max_insert_delayed_streams_for_parallel_write = 6
VALUES ('m6', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 42.), (toDateTime64('2020-01-02 00:00:00', 3), 43.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM `.inner_id.samples.05296000-0000-4000-8000-000000000004`;
SELECT count() FROM `.inner_id.recentsamples.05296000-0000-4000-8000-000000000004`;
SELECT count() FROM `.inner_id.recentsamples.05296000-0000-4000-8000-000000000004` WHERE arrayExists(x -> x.2 = 42, samples);

SELECT '--- a recent target stores a default column ---';

CREATE TABLE ts_def UUID '05296000-0000-4000-8000-000000000005' ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
RECENT SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS allow_dimensions_outside_sorting_key = 1
RECENT SAMPLES INNER COLUMNS (extra UInt8 DEFAULT 7);

INSERT INTO ts_def (metric_name, tags, samples) VALUES ('m', map('env', 'prod'), [(toDateTime64(toStartOfHour(now64(3)), 3) + INTERVAL 1 MINUTE, 1.)]);

SELECT extra FROM `.inner_id.recentsamples.05296000-0000-4000-8000-000000000005`;

DROP VIEW fail_mv;
DROP TABLE ts_fail, fail_mv_dest, ts_part, ts_def;

SELECT '--- rejected sample ---';

CREATE TABLE ts_view_barrier UUID '05296000-0000-4000-8000-000000000006' ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;
ALTER TABLE `.inner_id.samples.05296000-0000-4000-8000-000000000006` ADD CONSTRAINT c CHECK sleepEachRow(2) = 0 AND arrayAll(x -> x.2 < 40, samples);

CREATE TABLE view_dest (bucket DateTime64(3)) ENGINE = MergeTree ORDER BY bucket;
CREATE MATERIALIZED VIEW view_recent_mv TO view_dest AS SELECT bucket FROM `.inner_id.recentsamples.05296000-0000-4000-8000-000000000006`;

INSERT INTO ts_view_barrier (metric_name, tags, samples) SETTINGS max_threads = 4, parallel_view_processing = 1, max_block_size = 1 VALUES ('m', map('env', 'prod'), [(toDateTime64(toStartOfHour(now64(3)), 3) + INTERVAL 1 MINUTE, 42.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM `.inner_id.samples.05296000-0000-4000-8000-000000000006`;
SELECT count() FROM `.inner_id.recentsamples.05296000-0000-4000-8000-000000000006`;
SELECT count() FROM view_dest;

SELECT '--- accepted sample ---';

INSERT INTO ts_view_barrier (metric_name, tags, samples) SETTINGS max_threads = 4, parallel_view_processing = 1, max_block_size = 1 VALUES ('m', map('env', 'prod'), [(toDateTime64(toStartOfHour(now64(3)), 3) + INTERVAL 1 MINUTE, 1.)]);

SELECT count() FROM `.inner_id.samples.05296000-0000-4000-8000-000000000006`;
SELECT count() FROM `.inner_id.recentsamples.05296000-0000-4000-8000-000000000006`;
SELECT count() FROM view_dest;

DROP VIEW view_recent_mv;
DROP TABLE ts_view_barrier, view_dest;

-- Strict insert block limits are disabled for async inserts, so this section runs with synchronous inserts.
-- The samples table is partitioned by day, so a block with four days violates max_partitions_per_insert_block = 2
-- unless the inner insert splits it at max_insert_block_size = 2.
SET use_strict_insert_block_limits = 1, max_insert_block_size = 2, min_insert_block_size_rows = 2, min_insert_block_size_bytes = 0, max_partitions_per_insert_block = 2;

SELECT '--- four days in one series are written as blocks of two rows ---';

CREATE TABLE ts_strict UUID '05296000-0000-4000-8000-000000000007' ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0, samples_partition_by = 'toDate(bucket)';

INSERT INTO ts_strict (metric_name, tags, samples) VALUES
    ('m', map('env', 'prod'), [('2027-01-01 00:00:00.000', 1.), ('2027-01-02 00:00:00.000', 2.), ('2027-01-03 00:00:00.000', 3.), ('2027-01-04 00:00:00.000', 4.)]);

SELECT count() FROM `.inner_id.samples.05296000-0000-4000-8000-000000000007`;

SELECT '--- the same insert with one thread ---';

DROP TABLE ts_strict;

CREATE TABLE ts_strict UUID '05296000-0000-4000-8000-000000000007' ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0, samples_partition_by = 'toDate(bucket)';

INSERT INTO ts_strict (metric_name, tags, samples) SETTINGS max_threads = 1 VALUES
    ('m', map('env', 'prod'), [('2027-01-01 00:00:00.000', 1.), ('2027-01-02 00:00:00.000', 2.), ('2027-01-03 00:00:00.000', 3.), ('2027-01-04 00:00:00.000', 4.)]);

SELECT count() FROM `.inner_id.samples.05296000-0000-4000-8000-000000000007`;

SELECT '--- the same insert with a recent samples table ---';

CREATE TABLE ts_strict_recent UUID '05296000-0000-4000-8000-000000000008' ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 864000, samples_partition_by = 'toDate(bucket)', recent_samples_partition_by = 'toDate(bucket)';

INSERT INTO ts_strict_recent (metric_name, tags, samples) VALUES
    ('m', map('env', 'prod'), [('2027-01-01 00:00:00.000', 1.), ('2027-01-02 00:00:00.000', 2.), ('2027-01-03 00:00:00.000', 3.), ('2027-01-04 00:00:00.000', 4.)]);

SELECT count() FROM `.inner_id.samples.05296000-0000-4000-8000-000000000008`;
SELECT count() FROM `.inner_id.recentsamples.05296000-0000-4000-8000-000000000008`;

DROP TABLE ts_strict, ts_strict_recent;
