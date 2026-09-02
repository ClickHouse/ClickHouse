-- Tags: no-fasttest, no-parallel-replicas, no-object-storage, no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS tags_table;
DROP TABLE IF EXISTS samples_table;
DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;
SET max_threads = 4;

CREATE TABLE tags_table
(
    id UInt64,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time DateTime64(3),
    max_time DateTime64(3)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE samples_table
(
    id UInt64,
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree() ORDER BY (id, timestamp)
SETTINGS index_granularity = 32768, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE prometheus ENGINE = TimeSeries SAMPLES samples_table TAGS tags_table;

INSERT INTO prometheus (metric_name, tags, time_series) VALUES ('m', map('host', 'h1'), [(toDateTime64(0, 3), 0)]);
INSERT INTO samples_table SELECT (SELECT id FROM tags_table LIMIT 1), toDateTime64(number, 3), number FROM numbers(720000);
ALTER TABLE tags_table UPDATE max_time = toDateTime64(720000, 3) WHERE 1 SETTINGS mutations_sync = 2;
OPTIMIZE TABLE samples_table FINAL;

SELECT max(toUInt64OrNull(extract(explain, '× (\\d+)'))) > 1 AS is_parallel
FROM (EXPLAIN PIPELINE SELECT count() FROM timeSeriesSelector(prometheus, 'm{host="h1"}', 0, 720000))
WHERE explain LIKE '%MergeTreeSelect%';

SELECT sum(explain LIKE '%MergeTreeSelect%') = 1 AS has_single_read,
       maxIf(toUInt64OrNull(extract(explain, '× (\\d+)')), explain LIKE '%MergeTreeSelect%') IS NULL AS is_single_stream
FROM (EXPLAIN PIPELINE SELECT count() FROM timeSeriesSelector(prometheus, 'm{host="h1"}', 0, 720000)
      SETTINGS merge_tree_min_bytes_for_concurrent_read = 251658240);

SELECT count(), sum(value) FROM timeSeriesSelector(prometheus, 'm{host="h1"}', 0, 720000);
SELECT count(), sum(value) FROM timeSeriesSelector(prometheus, 'm{host="h1"}', 0, 720000)
SETTINGS merge_tree_min_bytes_for_concurrent_read = 251658240;

DROP TABLE prometheus;
DROP TABLE samples_table;
DROP TABLE tags_table;
