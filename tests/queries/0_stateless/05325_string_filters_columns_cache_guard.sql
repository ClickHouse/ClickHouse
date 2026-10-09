-- The scan-time string filter (`apply_string_filters_during_scan`) replaces non-matching values with
-- empty strings. The columns read from a part may be written to the columns cache and served to other
-- queries, which would then observe the substituted empty strings. Therefore the optimization must be
-- disabled when the reader may write to the columns cache.

DROP TABLE IF EXISTS t_string_filter_columns_cache;

CREATE TABLE t_string_filter_columns_cache (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_string_filter_columns_cache SELECT number, if(number % 100 = 0, 'needle ' || toString(number), 'haystack ' || toString(number)) FROM numbers(10000);

SET use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1;

SELECT count(), sum(length(s)) FROM t_string_filter_columns_cache PREWHERE s LIKE '%needle%'
SETTINGS apply_string_filters_during_scan = 1, log_comment = '05325_filtered';

-- Served from the columns cache populated by the previous query.
SELECT count(), countIf(empty(s)), sum(length(s)) FROM t_string_filter_columns_cache
SETTINGS apply_string_filters_during_scan = 0;

SYSTEM FLUSH LOGS query_log;

SELECT sum(ProfileEvents['StringValueFilterValuesChecked'])
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05325_filtered' AND type = 'QueryFinish';

DROP TABLE t_string_filter_columns_cache;
