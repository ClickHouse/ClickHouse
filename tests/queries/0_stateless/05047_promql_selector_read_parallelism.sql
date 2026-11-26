-- Tags: no-fasttest, no-parallel-replicas, no-object-storage, no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;
SET max_threads = 4;

-- A row of the samples table contains the samples of one series within a 60-second bucket, so the table has 12000 rows below.
-- The samples table is written in wide parts, so that its columns can be read in parallel.
CREATE TABLE prometheus ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 60
TAGS INNER COLUMNS (id UInt64)
SAMPLES INNER ENGINE = AggregatingMergeTree ORDER BY (id, bucket) SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO prometheus (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(0, 3), 0)]);
INSERT INTO prometheus (metric_name, tags, samples) SELECT 'm', map('host', 'h1'), groupArray((toDateTime64(number, 3), toFloat64(number))) FROM numbers(720000);
OPTIMIZE TABLE prometheus FINAL;

SELECT max(toUInt64OrNull(extract(explain, '× (\\d+)'))) > 1 AS is_parallel
FROM (EXPLAIN PIPELINE SELECT count() FROM timeSeriesSelector(prometheus, 'm{host="h1"}', 0, 720000, 'samples'))
WHERE explain LIKE '%MergeTreeSelect%';

-- The thresholds set by the user are respected. Both thresholds are set here: the greater of the two applies,
-- and the threshold in rows is set to zero by the selector unless the user sets it.
SELECT sum(explain LIKE '%MergeTreeSelect%') = 1 AS has_single_read,
       maxIf(toUInt64OrNull(extract(explain, '× (\\d+)')), explain LIKE '%MergeTreeSelect%') IS NULL AS is_single_stream
FROM (EXPLAIN PIPELINE SELECT count() FROM timeSeriesSelector(prometheus, 'm{host="h1"}', 0, 720000, 'samples')
      SETTINGS merge_tree_min_bytes_for_concurrent_read = 251658240, merge_tree_min_rows_for_concurrent_read = 1000000);

-- The duplicate sample at the timestamp 0 is removed, so the table has 720000 samples.
SELECT sum(length(samples)), sum(arraySum(sample -> sample.2, samples)) FROM timeSeriesSelector(prometheus, 'm{host="h1"}', 0, 720000, 'samples');
SELECT sum(length(samples)), sum(arraySum(sample -> sample.2, samples)) FROM timeSeriesSelector(prometheus, 'm{host="h1"}', 0, 720000, 'samples')
SETTINGS merge_tree_min_bytes_for_concurrent_read = 251658240, merge_tree_min_rows_for_concurrent_read = 1000000;

DROP TABLE prometheus;
