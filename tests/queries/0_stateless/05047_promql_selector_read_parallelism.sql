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

-- A row of the samples table contains the samples of one series within a 60-second bucket.
CREATE TABLE samples_table
(
    id UInt64,
    samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3), value Float64))),
    bucket DateTime64(3),
    min_time SimpleAggregateFunction(min, DateTime64(3)),
    max_time SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree() ORDER BY (id, bucket)
SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE prometheus ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 60 SAMPLES samples_table TAGS tags_table;

INSERT INTO prometheus (metric_name, tags, time_series) VALUES ('m', map('host', 'h1'), [(toDateTime64(0, 3), 0)]);
INSERT INTO samples_table
    SELECT (SELECT id FROM tags_table LIMIT 1), timeSeriesGroupArray(toDateTime64(number, 3), toFloat64(number)),
           toDateTime(intDiv(number, 60) * 60), min(toDateTime64(number, 3)), max(toDateTime64(number, 3))
    FROM numbers(720000) GROUP BY intDiv(number, 60);
ALTER TABLE tags_table UPDATE max_time = toDateTime64(720000, 3) WHERE 1 SETTINGS mutations_sync = 2;
OPTIMIZE TABLE samples_table FINAL;

SELECT max(toUInt64OrNull(extract(explain, '× (\\d+)'))) > 1 AS is_parallel
FROM (EXPLAIN PIPELINE SELECT count() FROM timeSeriesSelector(prometheus, 'm{host="h1"}', 0, 720000))
WHERE explain LIKE '%MergeTreeSelect%';

-- The thresholds set by the user are respected. Both thresholds are set here: the greater of the two applies,
-- and by default the selector lowers the threshold in bytes and disables the threshold in rows.
SELECT sum(explain LIKE '%MergeTreeSelect%') = 1 AS has_single_read,
       maxIf(toUInt64OrNull(extract(explain, '× (\\d+)')), explain LIKE '%MergeTreeSelect%') IS NULL AS is_single_stream
FROM (EXPLAIN PIPELINE SELECT count() FROM timeSeriesSelector(prometheus, 'm{host="h1"}', 0, 720000)
      SETTINGS merge_tree_min_bytes_for_concurrent_read = 251658240, merge_tree_min_rows_for_concurrent_read = 163840);

SELECT sum(length(time_series)), sum(arraySum(x -> x.2, time_series)) FROM timeSeriesSelector(prometheus, 'm{host="h1"}', 0, 720000);
SELECT sum(length(time_series)), sum(arraySum(x -> x.2, time_series)) FROM timeSeriesSelector(prometheus, 'm{host="h1"}', 0, 720000)
SETTINGS merge_tree_min_bytes_for_concurrent_read = 251658240, merge_tree_min_rows_for_concurrent_read = 163840;

DROP TABLE prometheus;
DROP TABLE samples_table;
DROP TABLE tags_table;
