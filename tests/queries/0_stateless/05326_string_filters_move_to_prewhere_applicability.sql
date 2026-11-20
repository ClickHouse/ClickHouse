-- A substring search condition that uses all queried columns is moved to PREWHERE with
-- `apply_string_filters_during_scan` only if the reader can apply it as a string filter during the scan.
-- It cannot, when the column is read by the existing PREWHERE or the row policy,
-- or when the read columns may be written to the columns cache.

SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, optimize_prewhere_after_pushdown = 1;
SET apply_string_filters_during_scan = 1;
SET use_columns_cache = 0;

DROP TABLE IF EXISTS t_string_filter_applicability;
DROP ROW POLICY IF EXISTS p_string_filter_applicability ON t_string_filter_applicability;

CREATE TABLE t_string_filter_applicability (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_string_filter_applicability SELECT number, 'value ' || toString(number) FROM numbers(1000);

SELECT 'baseline';
SELECT countIf(explain LIKE '%Prewhere filter column%LIKE%') > 0 FROM (
    EXPLAIN actions = 1 SELECT s FROM t_string_filter_applicability WHERE s LIKE '%needle%');

SELECT 'columns cache writes';
SELECT countIf(explain LIKE '%Prewhere filter column%LIKE%') > 0 FROM (
    EXPLAIN actions = 1 SELECT s FROM t_string_filter_applicability WHERE s LIKE '%needle%'
    SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 1);
SELECT countIf(explain LIKE '%Prewhere filter column%LIKE%') > 0 FROM (
    EXPLAIN actions = 1 SELECT s FROM t_string_filter_applicability WHERE s LIKE '%needle%'
    SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 0);

SELECT 'existing PREWHERE';
SELECT countIf(explain LIKE '%Prewhere filter column%LIKE%') > 0 FROM (
    EXPLAIN actions = 1 SELECT s FROM t_string_filter_applicability PREWHERE notEmpty(s) WHERE s LIKE '%needle%');
-- The existing PREWHERE on another column does not prevent it.
SELECT countIf(explain LIKE '%Prewhere filter column%LIKE%') > 0 FROM (
    EXPLAIN actions = 1 SELECT s FROM t_string_filter_applicability PREWHERE id > 10 WHERE s LIKE '%needle%');

SELECT 'row policy';
CREATE ROW POLICY p_string_filter_applicability ON t_string_filter_applicability USING notEmpty(s) TO ALL;
SELECT countIf(explain LIKE '%Prewhere filter column%LIKE%') > 0 FROM (
    EXPLAIN actions = 1 SELECT s FROM t_string_filter_applicability WHERE s LIKE '%needle%');
-- The row policy on another column does not prevent it.
CREATE ROW POLICY OR REPLACE p_string_filter_applicability ON t_string_filter_applicability USING id >= 0 TO ALL;
SELECT countIf(explain LIKE '%Prewhere filter column%LIKE%') > 0 FROM (
    EXPLAIN actions = 1 SELECT s FROM t_string_filter_applicability WHERE s LIKE '%needle%');
DROP ROW POLICY p_string_filter_applicability ON t_string_filter_applicability;

DROP TABLE t_string_filter_applicability;
