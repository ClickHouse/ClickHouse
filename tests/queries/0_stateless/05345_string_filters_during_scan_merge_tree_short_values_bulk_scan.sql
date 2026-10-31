-- The string filter from PREWHERE disables itself when the skipped values are too short to pay off
-- for the check of every value. With the default serialization of `String` columns (with a separate
-- stream for sizes), a substring condition is checked by a single scan of the buffer of concatenated
-- values instead of a search in every value, so the filter must stay enabled for short values.

-- The string filters are not applied when the read columns may be written to the columns cache.
-- The profile events of the reading are checked, so the query must read the data locally, not on parallel replicas.
SET use_columns_cache = 0;

DROP TABLE IF EXISTS t_string_filter_short_bulk;

CREATE TABLE t_string_filter_short_bulk (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0,
    serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream';

INSERT INTO t_string_filter_short_bulk SELECT number, ['ab', 'cd', 'ex'][number % 3 + 1] FROM numbers(100000);

SELECT count(), sum(cityHash64(s)) FROM t_string_filter_short_bulk PREWHERE s LIKE '%x%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_short_bulk PREWHERE s LIKE '%x%' SETTINGS apply_string_filters_during_scan = 1;

SELECT count() FROM t_string_filter_short_bulk PREWHERE s LIKE '%x%'
SETTINGS apply_string_filters_during_scan = 1, enable_parallel_replicas = 0, max_threads = 1, log_comment = '05345_short_values_bulk_scan';

SYSTEM FLUSH LOGS query_log;

-- Two thirds of the rows do not match. If the filter disabled itself after the first granule,
-- less than 10000 rows would be replaced.
SELECT sum(ProfileEvents['StringValueFilterValuesReplaced']) > 50000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05345_short_values_bulk_scan';

DROP TABLE t_string_filter_short_bulk;
