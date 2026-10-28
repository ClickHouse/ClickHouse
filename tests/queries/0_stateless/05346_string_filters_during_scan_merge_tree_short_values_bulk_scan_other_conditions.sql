-- With the default serialization of `String` columns (with a separate stream for sizes), the longest
-- substring of a compound condition is found by a single scan of the buffer of concatenated values,
-- but the found candidates are checked one by one against the other conditions. When there are many
-- candidates and they are short, the filter must disable itself, same as when every value is checked.
-- When there are few candidates, the bytes skipped by the scan pay off for their checks.

-- The string filters are not applied when the read columns may be written to the columns cache.
-- The profile events of the reading are checked, so the query must read the data locally, not on parallel replicas.
SET use_columns_cache = 0;

DROP TABLE IF EXISTS t_string_filter_short_bulk_other;
DROP TABLE IF EXISTS t_string_filter_long_bulk_other;

CREATE TABLE t_string_filter_short_bulk_other (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0,
    serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream';

-- Every value contains 'x', so every value is a candidate of the scan, and only one third matches `LIKE '%x%a%'`.
INSERT INTO t_string_filter_short_bulk_other SELECT number, ['xa', 'xb', 'xc'][number % 3 + 1] FROM numbers(100000);

SELECT count(), sum(cityHash64(s)) FROM t_string_filter_short_bulk_other PREWHERE s LIKE '%x%a%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_short_bulk_other PREWHERE s LIKE '%x%a%' SETTINGS apply_string_filters_during_scan = 1;

SELECT count() FROM t_string_filter_short_bulk_other PREWHERE s LIKE '%x%a%'
SETTINGS apply_string_filters_during_scan = 1, enable_parallel_replicas = 0, max_threads = 1, max_block_size = 8192, log_comment = '05346_many_candidates';

CREATE TABLE t_string_filter_long_bulk_other (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0,
    serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream';

-- Only one third of the values contains 'xa', so the scan itself replaces most of the values, and they are long enough.
INSERT INTO t_string_filter_long_bulk_other SELECT number, concat(['xa', 'yb', 'yc'][number % 3 + 1], repeat('z', 30)) FROM numbers(100000);

SELECT count(), sum(cityHash64(s)) FROM t_string_filter_long_bulk_other PREWHERE s LIKE '%xa%z%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_long_bulk_other PREWHERE s LIKE '%xa%z%' SETTINGS apply_string_filters_during_scan = 1;

SELECT count() FROM t_string_filter_long_bulk_other PREWHERE s LIKE '%xa%z%'
SETTINGS apply_string_filters_during_scan = 1, enable_parallel_replicas = 0, max_threads = 1, log_comment = '05346_few_candidates';

SELECT count(), sum(cityHash64(s)) FROM t_string_filter_short_bulk_other PREWHERE s LIKE '%x%' AND s LIKE '%a%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count(), sum(cityHash64(s)) FROM t_string_filter_short_bulk_other PREWHERE s LIKE '%x%' AND s LIKE '%a%' SETTINGS apply_string_filters_during_scan = 1;

SYSTEM FLUSH LOGS query_log;

-- Two thirds of the rows do not match, but the filter disables itself after the first granules,
-- because the checks of the short candidates do not pay off.
SELECT sum(ProfileEvents['StringValueFilterValuesReplaced']) < 50000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05346_many_candidates';

-- The filter stays enabled: the bytes skipped by the scan pay off for the checks of the candidates.
SELECT sum(ProfileEvents['StringValueFilterValuesReplaced']) > 50000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05346_few_candidates';

DROP TABLE t_string_filter_short_bulk_other;
DROP TABLE t_string_filter_long_bulk_other;
