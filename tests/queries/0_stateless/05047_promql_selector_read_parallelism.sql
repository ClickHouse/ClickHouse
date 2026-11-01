-- Tags: no-fasttest, no-parallel-replicas, no-object-storage, no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;
SET max_threads = 4;

-- The samples table is written in wide parts, so that its columns can be read in parallel.
CREATE TABLE prometheus ENGINE = TimeSeries
TAGS INNER COLUMNS (id UInt64)
SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp) SETTINGS index_granularity = 32768, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO prometheus (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(0, 3), 0)]);
INSERT INTO prometheus (metric_name, tags, samples) SELECT 'm', map('host', 'h1'), groupArray((toDateTime64(number, 3), toFloat64(number))) FROM numbers(720000);
OPTIMIZE TABLE prometheus FINAL;

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
