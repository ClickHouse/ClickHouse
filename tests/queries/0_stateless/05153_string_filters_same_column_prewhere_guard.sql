-- The scan-time string filter (`apply_string_filters_during_scan`) replaces non-matching values with
-- empty strings, and the row is rejected only after the whole PREWHERE expression has been evaluated.
-- Therefore the optimization must be disabled for a column that another PREWHERE expression reads:
-- that expression would otherwise be evaluated on the substituted empty string. Here `throwIf` would
-- fire for every non-matching row, and `length` would report zero for it.

DROP TABLE IF EXISTS t_string_filter_guard;

CREATE TABLE t_string_filter_guard (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_string_filter_guard
SELECT number, if(number % 3 = 0, 'lorem needle ipsum ' || toString(number), 'nothing interesting ' || toString(number))
FROM numbers(10000);

SET apply_string_filters_during_scan = 1, optimize_move_to_prewhere = 0;

-- A guard on the same column must still see the original value: no row has an empty `s`, so `throwIf`
-- must not fire, and the count must be the number of the matching rows.
SELECT 'guard before the substring condition';
SELECT count() FROM t_string_filter_guard PREWHERE throwIf(empty(s), 'the value was replaced') = 0 AND s LIKE '%needle%';

SELECT 'guard after the substring condition';
SELECT count() FROM t_string_filter_guard PREWHERE s LIKE '%needle%' AND throwIf(empty(s), 'the value was replaced') = 0;

-- Another expression on the same column must observe the original value, not an empty string.
SELECT 'another expression on the same column';
SELECT count(), min(length(s)) FROM t_string_filter_guard PREWHERE length(s) > 1 AND s LIKE '%needle%';

-- Control: without another reader of the column the pushdown is applied and the result is the same.
SELECT 'control';
SELECT count() FROM t_string_filter_guard PREWHERE s LIKE '%needle%';

DROP TABLE t_string_filter_guard;
