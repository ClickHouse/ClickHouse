-- Tags: no-parallel-replicas
-- no-parallel-replicas: the ordering assertion compares the local targets' part-log events.
SET allow_experimental_time_series_table = 1;
SET max_threads = 1;
SET insert_deduplicate = 1;

CREATE TABLE ts_cache_order ENGINE = TimeSeries
    SETTINGS tags_cache_max_series = 1000, recent_samples_ttl_seconds = 0
    TAGS INNER ENGINE = ReplacingMergeTree ORDER BY (metric_name, id) SETTINGS non_replicated_deduplication_window = 1000
    TAGS MIN MAX INNER ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS non_replicated_deduplication_window = 1000;

-- Eight chunks in one sink: the last seven are pending cache hits. Finishing the tags only in
-- `onFinish` lets the samples' delayed chunks commit first, even though no duplicate tags are written.
INSERT INTO ts_cache_order (metric_name, tags, samples)
SELECT 'm', map('job', 'api'), [(toDateTime64(1000 + number, 3), toFloat64(number))]
FROM numbers(8)
SETTINGS max_block_size = 1, max_insert_block_size = 1,
         min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1, max_insert_threads = 1,
         insert_deduplication_token = 'cached_commit_order_first';

SYSTEM FLUSH LOGS part_log;
WITH
    (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_cache_order') AS table_uuid,
    concat('.inner_id.tags.', table_uuid) AS tags_table,
    concat('.inner_id.samples.', table_uuid) AS samples_table,
    concat('.inner_id.tagsminmax.', table_uuid) AS bounds_table
SELECT
    sumIf(rows, table = tags_table),
    sumIf(rows, table = samples_table),
    sumIf(rows, table = bounds_table),
    maxIf(event_time_microseconds, table = tags_table) <= minIf(event_time_microseconds, table = samples_table)
FROM system.part_log
WHERE database = currentDatabase() AND event_type = 'NewPart' AND table IN (tags_table, samples_table, bounds_table);

SELECT min(min_time) = toDateTime64(1000, 3), max(max_time) = toDateTime64(1007, 3)
FROM timeSeriesTagsMinMax(ts_cache_order);

-- A cache miss after a hit must reopen the finished tags pipeline. Part-log row counts detect
-- duplicate writes even if a background merge has already collapsed the duplicate series.
-- Explicit deduplication tokens must not cause the reopened pipelines to lose later blocks.
INSERT INTO ts_cache_order (metric_name, tags, samples)
SELECT concat('m', toString(intDiv(number, 2))), map('job', 'api'), [(toDateTime64(2000 + number, 3), toFloat64(number))]
FROM numbers(8)
SETTINGS max_block_size = 1, max_insert_block_size = 1,
         min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1, max_insert_threads = 1,
         insert_deduplication_token = 'cached_commit_order_new_series';

SYSTEM FLUSH LOGS part_log;
WITH
    (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_cache_order') AS table_uuid,
    concat('.inner_id.tags.', table_uuid) AS tags_table,
    concat('.inner_id.samples.', table_uuid) AS samples_table
SELECT sumIf(rows, table = tags_table), sumIf(rows, table = samples_table)
FROM system.part_log
WHERE database = currentDatabase() AND event_type = 'NewPart' AND table IN (tags_table, samples_table);

SELECT count() FROM timeSeriesTags(ts_cache_order);
SELECT count() FROM timeSeriesSamples(ts_cache_order);
DROP TABLE ts_cache_order;
