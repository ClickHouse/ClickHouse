-- Tags: no-parallel, no-fasttest
-- A sink throws after TimeSeriesCommitGate has released the block.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

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
) ENGINE = MergeTree ORDER BY (metric_name, id);

CREATE TABLE ext_samples (id Tuple(UInt64, UUID), timestamp DateTime64(3), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE ext_recent (id Tuple(UInt64, UUID), timestamp DateTime64(3), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE ts_ext ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA ext_samples TAGS ext_tags RECENT SAMPLES ext_recent;

SELECT '--- tags commit failure ---';

SYSTEM ENABLE FAILPOINT time_series_sink_commit_throw_tags;
INSERT INTO ts_ext (metric_name, tags, samples) VALUES ('tags_fail', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 10.)]); -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT time_series_sink_commit_throw_tags;

SELECT count() FROM ext_tags WHERE metric_name = 'tags_fail';
SELECT count() FROM ext_samples WHERE value = 10;
SELECT count() FROM ext_recent WHERE value = 10;

SELECT '--- samples commit failure ---';

SYSTEM ENABLE FAILPOINT time_series_sink_commit_throw_samples;
INSERT INTO ts_ext (metric_name, tags, samples) VALUES ('samples_fail', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 20.)]); -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT time_series_sink_commit_throw_samples;

SELECT count() FROM ext_tags WHERE metric_name = 'samples_fail';
SELECT count() FROM ext_samples WHERE value = 20;
SELECT count() FROM ext_recent WHERE value = 20;

SELECT '--- tags commit failure when the block commits inside consume ---';

SYSTEM ENABLE FAILPOINT time_series_sink_commit_throw_tags;
INSERT INTO ts_ext (metric_name, tags, samples) SETTINGS input_format_connection_handling = 1, input_format_max_block_wait_ms = 1 VALUES ('tags_fail_inline', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 30.)]); -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT time_series_sink_commit_throw_tags;

SELECT count() FROM ext_tags WHERE metric_name = 'tags_fail_inline';
SELECT count() FROM ext_samples WHERE value = 30;
SELECT count() FROM ext_recent WHERE value = 30;

SELECT '--- samples commit failure when the block commits inside consume ---';

SYSTEM ENABLE FAILPOINT time_series_sink_commit_throw_samples;
INSERT INTO ts_ext (metric_name, tags, samples) SETTINGS input_format_connection_handling = 1, input_format_max_block_wait_ms = 1 VALUES ('samples_fail_inline', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 40.)]); -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT time_series_sink_commit_throw_samples;

SELECT count() FROM ext_tags WHERE metric_name = 'samples_fail_inline';
SELECT count() FROM ext_samples WHERE value = 40;
SELECT count() FROM ext_recent WHERE value = 40;

DROP TABLE ts_ext;
DROP TABLE ext_tags;
DROP TABLE ext_samples;
DROP TABLE ext_recent;
