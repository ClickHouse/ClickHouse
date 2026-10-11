-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: the ordering check compares the part log events of the local target tables.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_bad_tags, ts_bad_tags_no_cache, ts_bad_bounds, ts_order, ts_token;

-- Every block below has one time series with 1000 samples. The squashing threshold keeps back the small tags and bounds
-- blocks but lets each samples block through, so the samples can be committed before the tags and bounds they need.

SELECT '--- a failed tags insert leaves no samples, also when the later blocks are hits of the tags cache ---';
CREATE TABLE ts_bad_tags ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
    TAGS INNER COLUMNS (bad UInt8 MATERIALIZED throwIf(metric_name = 'bad'));
INSERT INTO ts_bad_tags (metric_name, tags, samples)
SELECT 'bad', map('job', 'api'), arrayMap(i -> (toDateTime64(number * 1000 + i, 3), toFloat64(i)), range(1000)) FROM numbers(8)
SETTINGS max_block_size = 1, max_insert_threads = 1, max_threads = 1, optimize_trivial_insert_select = 0,
    min_insert_block_size_rows = 0, min_insert_block_size_bytes = 4096; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT count() FROM timeSeriesSamples(currentDatabase(), 'ts_bad_tags');

SELECT '--- the same without the tags cache ---';
CREATE TABLE ts_bad_tags_no_cache ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0, tags_deduplication_cache_size_bytes = 0
    TAGS INNER COLUMNS (bad UInt8 MATERIALIZED throwIf(metric_name = 'bad'));
INSERT INTO ts_bad_tags_no_cache (metric_name, tags, samples)
SELECT 'bad', map('job', 'api'), arrayMap(i -> (toDateTime64(number * 1000 + i, 3), toFloat64(i)), range(1000)) FROM numbers(8)
SETTINGS max_block_size = 1, max_insert_threads = 1, max_threads = 1, optimize_trivial_insert_select = 0,
    min_insert_block_size_rows = 0, min_insert_block_size_bytes = 4096; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT count() FROM timeSeriesSamples(currentDatabase(), 'ts_bad_tags_no_cache');

SELECT '--- a failed bounds insert leaves no samples a time-bounded selector would miss ---';
CREATE TABLE ts_bad_bounds ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
    TAGS MIN MAX INNER COLUMNS (bad UInt8 MATERIALIZED throwIf(metric_name = 'bad'));
INSERT INTO ts_bad_bounds (metric_name, tags, samples)
SELECT 'bad', map('job', 'api'), arrayMap(i -> (toDateTime64(number * 1000 + i, 3), toFloat64(i)), range(1000)) FROM numbers(8)
SETTINGS max_block_size = 1, max_insert_threads = 1, max_threads = 1, optimize_trivial_insert_select = 0,
    min_insert_block_size_rows = 0, min_insert_block_size_bytes = 4096; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT count() FROM timeSeriesSamples(currentDatabase(), 'ts_bad_bounds');
SELECT count() FROM timeSeriesTagsMinMax(currentDatabase(), 'ts_bad_bounds');

SELECT '--- each block commits its tags and bounds before its samples ---';
CREATE TABLE ts_order ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;
INSERT INTO ts_order (metric_name, tags, samples)
SELECT 'm', map('job', 'api'), arrayMap(i -> (toDateTime64(number * 1000 + i, 3), toFloat64(i)), range(1000)) FROM numbers(8)
SETTINGS max_block_size = 1, max_insert_threads = 1, max_threads = 1, optimize_trivial_insert_select = 0,
    min_insert_block_size_rows = 0, min_insert_block_size_bytes = 4096;
SYSTEM FLUSH LOGS part_log;
WITH
    (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_order') AS table_uuid,
    arraySort(groupArrayIf(event_time_microseconds, table = concat('.inner_id.tags.', table_uuid))) AS tags_parts,
    arraySort(groupArrayIf(event_time_microseconds, table = concat('.inner_id.tagsminmax.', table_uuid))) AS bounds_parts,
    arraySort(groupArrayIf(event_time_microseconds, table = concat('.inner_id.samples.', table_uuid))) AS samples_parts
SELECT length(tags_parts), length(bounds_parts), length(samples_parts),
    tags_parts[1] <= samples_parts[1], arrayAll(i -> bounds_parts[i] <= samples_parts[i], arrayEnumerate(samples_parts))
FROM system.part_log
WHERE database = currentDatabase() AND event_type = 'NewPart' AND error = 0;
SELECT min(min_time) = toDateTime64(0, 3), max(max_time) = toDateTime64(7999, 3) FROM timeSeriesTagsMinMax(currentDatabase(), 'ts_order');

SELECT '--- with a deduplication token, the tags and bounds of later blocks are kept, and a retry is deduplicated ---';
CREATE TABLE ts_token ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
    TAGS INNER ENGINE = ReplacingMergeTree ORDER BY (metric_name, id) SETTINGS non_replicated_deduplication_window = 100
    TAGS MIN MAX INNER ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO ts_token (metric_name, tags, samples)
SELECT concat('m', toString(intDiv(number, 2))), map('job', 'api'), arrayMap(i -> (toDateTime64(number * 1000 + i, 3), toFloat64(i)), range(1000))
FROM numbers(8)
SETTINGS max_block_size = 1, max_insert_threads = 1, max_threads = 1, optimize_trivial_insert_select = 0,
    min_insert_block_size_rows = 0, min_insert_block_size_bytes = 4096, insert_deduplication_token = 'ts_token_insert';
SELECT uniqExact(id) FROM timeSeriesTags(currentDatabase(), 'ts_token');
SELECT uniqExact(id), max(max_time) = toDateTime64(7999, 3) FROM timeSeriesTagsMinMax(currentDatabase(), 'ts_token');
SYSTEM CLEAR TIME SERIES CACHES ts_token;
INSERT INTO ts_token (metric_name, tags, samples)
SELECT concat('m', toString(intDiv(number, 2))), map('job', 'api'), arrayMap(i -> (toDateTime64(number * 1000 + i, 3), toFloat64(i)), range(1000))
FROM numbers(8)
SETTINGS max_block_size = 1, max_insert_threads = 1, max_threads = 1, optimize_trivial_insert_select = 0,
    min_insert_block_size_rows = 0, min_insert_block_size_bytes = 4096, insert_deduplication_token = 'ts_token_insert';
SYSTEM FLUSH LOGS part_log;
WITH
    (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_token') AS table_uuid,
    concat('.inner_id.tags.', table_uuid) AS tags_table,
    concat('.inner_id.tagsminmax.', table_uuid) AS bounds_table
SELECT countIf(table = tags_table AND error = 0), countIf(table = bounds_table AND error = 0),
    countIf(table = tags_table AND error != 0), countIf(table = bounds_table AND error != 0)
FROM system.part_log
WHERE database = currentDatabase() AND event_type = 'NewPart';

DROP TABLE ts_bad_tags, ts_bad_tags_no_cache, ts_bad_bounds, ts_order, ts_token;
