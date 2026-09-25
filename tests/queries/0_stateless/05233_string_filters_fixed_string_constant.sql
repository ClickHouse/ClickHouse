-- `String = FixedString(N)` ignores the trailing zero padding of the constant, but the `Field` of
-- that constant still carries it, so a scan-time string filter built from it would look for the
-- padded needle and replace every value with an empty string, making the condition reject all rows.
-- The extractor must therefore ignore a constant that is not a `String`.

DROP TABLE IF EXISTS t_string_filter_fixed_string;

CREATE TABLE t_string_filter_fixed_string (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_string_filter_fixed_string
SELECT number, if(number % 3 = 0, 'hello', 'nothing interesting ' || toString(number)) FROM numbers(10000);

SELECT 'equals';
SELECT count() FROM t_string_filter_fixed_string PREWHERE s = toFixedString('hello', 10) SETTINGS apply_string_filters_during_scan = 0;
SELECT count() FROM t_string_filter_fixed_string PREWHERE s = toFixedString('hello', 10) SETTINGS apply_string_filters_during_scan = 1;

SELECT 'startsWith';
SELECT count() FROM t_string_filter_fixed_string PREWHERE startsWith(s, toFixedString('hel', 3)) SETTINGS apply_string_filters_during_scan = 0;
SELECT count() FROM t_string_filter_fixed_string PREWHERE startsWith(s, toFixedString('hel', 3)) SETTINGS apply_string_filters_during_scan = 1;

SELECT 'endsWith';
SELECT count() FROM t_string_filter_fixed_string PREWHERE endsWith(s, toFixedString('llo', 3)) SETTINGS apply_string_filters_during_scan = 0;
SELECT count() FROM t_string_filter_fixed_string PREWHERE endsWith(s, toFixedString('llo', 3)) SETTINGS apply_string_filters_during_scan = 1;

-- A plain `String` constant is still filtered during the scan, so the guard is not too wide.
SELECT 'control';
SELECT count() FROM t_string_filter_fixed_string PREWHERE s = 'hello' SETTINGS apply_string_filters_during_scan = 1, log_comment = '05233_string_filters_control';

SYSTEM FLUSH LOGS query_log;
SELECT sum(ProfileEvents['StringValueFilterValuesChecked']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05233_string_filters_control';

DROP TABLE t_string_filter_fixed_string;
