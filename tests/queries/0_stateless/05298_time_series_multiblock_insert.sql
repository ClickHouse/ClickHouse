-- One TimeSeries insert can contain more than one block.
-- The samples and recent samples of a block are written after the tags of that block are committed.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_multi, ts_ext, ext_tags, ext_samples, ext_recent, ext_samples_dest;
DROP VIEW IF EXISTS ext_samples_mv;

SELECT '--- two input blocks complete ---';

CREATE TABLE ts_multi ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;

INSERT INTO ts_multi (metric_name, tags, samples)
SELECT
    concat('m', toString(number)),
    map('env', toString(number)),
    [(now64(3) - INTERVAL 1 MINUTE, toFloat64(number))]
FROM numbers(2)
SETTINGS max_block_size = 1, max_insert_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_threads = 4;

SELECT metric_name, tags['env'] AS env FROM timeSeriesTags(ts_multi) ORDER BY metric_name;
SELECT value FROM timeSeriesSamples(ts_multi) ORDER BY value;

SELECT '--- two input blocks complete when a block commits inside consume ---';

INSERT INTO ts_multi (metric_name, tags, samples)
SELECT
    concat('e', toString(number)),
    map('env', toString(number)),
    [(now64(3) - INTERVAL 1 MINUTE, toFloat64(number + 10))]
FROM numbers(2)
SETTINGS max_block_size = 1, max_insert_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_threads = 4, input_format_max_block_wait_ms = 1;

SELECT metric_name FROM timeSeriesTags(ts_multi) WHERE metric_name LIKE 'e%' ORDER BY metric_name;
SELECT value FROM timeSeriesSamples(ts_multi) WHERE value >= 10 ORDER BY value;

SELECT '--- two input blocks complete when a dependent view waits for the part ---';

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
CREATE TABLE ext_samples_dest (id Tuple(UInt64, UUID), timestamp DateTime64(3), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE MATERIALIZED VIEW ext_samples_mv TO ext_samples_dest AS SELECT * FROM ext_samples;

CREATE TABLE ts_ext ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA ext_samples TAGS ext_tags RECENT SAMPLES ext_recent;

INSERT INTO ts_ext (metric_name, tags, samples)
SELECT
    concat('v', toString(number)),
    map('env', 'prod'),
    [(now64(3) - INTERVAL 1 MINUTE, toFloat64(number + 20))]
FROM numbers(2)
SETTINGS max_block_size = 1, max_insert_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_threads = 4, wait_for_part_commit_in_dependent_materialized_views = 1;

SELECT count() FROM ext_tags;
SELECT count() FROM ext_samples;
SELECT count() FROM ext_recent;
SELECT count() FROM ext_samples_dest;

SELECT '--- a failed later block drops the earlier block ---';

INSERT INTO ts_ext (metric_name, tags, samples)
SELECT
    if(number = 0, 'keep_me', 'bad_tags'),
    map('env', 'prod'),
    [(now64(3) - INTERVAL 1 MINUTE, if(number = 0, 30., 40.))]
FROM numbers(2)
SETTINGS max_block_size = 1, max_insert_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_threads = 4; -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM ext_tags WHERE metric_name = 'keep_me';
SELECT count() FROM ext_samples WHERE value IN (30, 40);
SELECT count() FROM ext_recent WHERE value IN (30, 40);
SELECT count() FROM ext_samples_dest WHERE value IN (30, 40);

-- A tags block that already committed stays. A block that fails the check does not.
-- The samples of a committed tags block are released only after the next tags block is consumed
-- or the tags table finished, so a failure in the next block leaves them unwritten.
SELECT '--- a failed later block keeps the earlier committed tags block ---';

INSERT INTO ts_ext (metric_name, tags, samples)
SELECT
    if(number = 0, 'keep_early', 'bad_tags'),
    map('env', 'prod'),
    [(now64(3) - INTERVAL 1 MINUTE, if(number = 0, 50., 60.))]
FROM numbers(2)
SETTINGS max_block_size = 1, max_insert_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_threads = 4, input_format_max_block_wait_ms = 1; -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM ext_tags WHERE metric_name = 'keep_early';
SELECT count() FROM ext_samples WHERE value = 50;
SELECT count() FROM ext_samples WHERE value = 60;
SELECT count() FROM ext_recent WHERE value = 50;
SELECT count() FROM ext_recent WHERE value = 60;
SELECT count() FROM ext_samples_dest WHERE value = 50;
SELECT count() FROM ext_samples_dest WHERE value = 60;

INSERT INTO ts_ext (metric_name, tags, samples)
SELECT
    if(number = 0, 'keep_mv', 'bad_tags'),
    map('env', 'prod'),
    [(now64(3) - INTERVAL 1 MINUTE, if(number = 0, 70., 80.))]
FROM numbers(2)
SETTINGS max_block_size = 1, max_insert_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_threads = 4, wait_for_part_commit_in_dependent_materialized_views = 1; -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM ext_tags WHERE metric_name = 'keep_mv';
SELECT count() FROM ext_samples WHERE value = 70;
SELECT count() FROM ext_samples WHERE value = 80;
SELECT count() FROM ext_samples_dest WHERE value = 70;
SELECT count() FROM ext_samples_dest WHERE value = 80;

DROP VIEW ext_samples_mv;
DROP TABLE ts_multi, ts_ext, ext_tags, ext_samples, ext_recent, ext_samples_dest;
