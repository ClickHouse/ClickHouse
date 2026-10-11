-- Tags: no-fasttest

-- A Parquet INT64 column without a logical type, read as DateTime (as ClickBench stores EventTime):
-- its row group statistics are used to filter and to skip row groups in ORDER BY ... LIMIT.
-- A bound outside the DateTime range is not used, because the cast saturates such values.

SET engine_file_truncate_on_insert = 1;
SET input_format_parquet_filter_push_down = 1;
SET use_top_k_dynamic_filtering = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET max_threads = 1;

-- 5 row groups of 100 rows with disjoint, increasing timestamps.
INSERT INTO FUNCTION file(currentDatabase() || '_05372.parquet', Parquet, 'ts Int64, v UInt32')
    SELECT 1700000000 + number, number FROM numbers(500)
    SETTINGS output_format_parquet_row_group_size = 100, max_block_size = 1000000;

SELECT count(), min(ts), max(ts)
FROM file(currentDatabase() || '_05372.parquet', Parquet, 'ts DateTime(\'UTC\'), v UInt32')
WHERE ts < toDateTime(1700000100, 'UTC')
SETTINGS log_comment = 'q05372_where';

SELECT ts, v
FROM file(currentDatabase() || '_05372.parquet', Parquet, 'ts DateTime(\'UTC\'), v UInt32')
ORDER BY ts DESC LIMIT 3
SETTINGS log_comment = 'q05372_top_k', max_parsing_threads = 1;

SELECT ts, v
FROM file(currentDatabase() || '_05372.parquet', Parquet, 'ts DateTime(\'UTC\'), v UInt32')
ORDER BY ts DESC LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 0;

-- Row groups [-200, -100], [-100, 1700000000], [1700000000, 5000000000] and [5000000000, 6000000000].
-- Values below 0 are read as 0 and values above 4294967295 as 4294967295, so only the in-range
-- bounds may be used.
INSERT INTO FUNCTION file(currentDatabase() || '_05372_range.parquet', Parquet, 'ts Int64')
    SELECT arrayJoin(CAST([-200, -100, -100, 1700000000, 1700000000, 5000000000, 5000000000, 6000000000] AS Array(Int64)))
    SETTINGS output_format_parquet_row_group_size = 2;

SELECT count() FROM file(currentDatabase() || '_05372_range.parquet', Parquet, 'ts DateTime(\'UTC\')')
WHERE ts <= toDateTime(1, 'UTC') SETTINGS log_comment = 'q05372_range_low';
SELECT count() FROM file(currentDatabase() || '_05372_range.parquet', Parquet, 'ts DateTime(\'UTC\')')
WHERE ts <= toDateTime(1, 'UTC') SETTINGS input_format_parquet_filter_push_down = 0;
SELECT count() FROM file(currentDatabase() || '_05372_range.parquet', Parquet, 'ts DateTime(\'UTC\')')
WHERE ts BETWEEN toDateTime(4294967000, 'UTC') AND toDateTime(4294967295, 'UTC') SETTINGS log_comment = 'q05372_range_high';
SELECT count() FROM file(currentDatabase() || '_05372_range.parquet', Parquet, 'ts DateTime(\'UTC\')')
WHERE ts BETWEEN toDateTime(4294967000, 'UTC') AND toDateTime(4294967295, 'UTC') SETTINGS input_format_parquet_filter_push_down = 0;
SELECT toUnixTimestamp(ts) FROM file(currentDatabase() || '_05372_range.parquet', Parquet, 'ts DateTime(\'UTC\')')
ORDER BY ts DESC LIMIT 1;
SELECT toUnixTimestamp(ts) FROM file(currentDatabase() || '_05372_range.parquet', Parquet, 'ts DateTime(\'UTC\')')
ORDER BY ts LIMIT 1;

-- An IPv4 read of a 64-bit integer column still ignores the statistics.
SELECT count() FROM file(currentDatabase() || '_05372.parquet', Parquet, 'ts IPv4, v UInt32')
WHERE ts = toIPv4('1.2.3.4'); -- { serverError NOT_IMPLEMENTED }

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ParquetPrunedRowGroups']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('q05372_where', 'q05372_range_low', 'q05372_range_high')
ORDER BY log_comment;

-- The read starts with the row group holding the largest values; the threshold then skips others.
SELECT ProfileEvents['ParquetTopKSkippedRowGroups'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'q05372_top_k';
