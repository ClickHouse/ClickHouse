-- With `apply_string_filters_during_scan` enabled, substring search conditions are moved
-- from WHERE to PREWHERE even when they use all queried columns: they are still beneficial,
-- because the reader skips copying the values that do not match them.

-- The test is about the decision to move a condition to PREWHERE, so the optimization must be enabled
-- (it is randomized in tests), and the queries below that turn it off do it explicitly.
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

DROP TABLE IF EXISTS t_string_filter_move;

-- The sparse serialization is not supported by the scan filter, and it is randomized in tests.
CREATE TABLE t_string_filter_move (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_string_filter_move
SELECT number, if(number % 100 = 0, 'value with needle ' || toString(number), 'ordinary value ' || toString(number))
FROM numbers(100000);

-- The condition uses the only queried column, so normally it is not moved to PREWHERE.
SELECT countIf(explain LIKE '%Prewhere filter%') > 0 FROM (
    EXPLAIN SELECT count() FROM t_string_filter_move WHERE s LIKE '%needle%'
    SETTINGS apply_string_filters_during_scan = 0);
SELECT countIf(explain LIKE '%Prewhere filter%') > 0 FROM (
    EXPLAIN SELECT count() FROM t_string_filter_move WHERE s LIKE '%needle%'
    SETTINGS apply_string_filters_during_scan = 1);
SELECT countIf(explain LIKE '%Prewhere filter%') > 0 FROM (
    EXPLAIN SELECT count() FROM t_string_filter_move WHERE position(s, 'needle') > 0
    SETTINGS apply_string_filters_during_scan = 1);
SELECT countIf(explain LIKE '%Prewhere filter%') > 0 FROM (
    EXPLAIN SELECT s FROM t_string_filter_move WHERE startsWith(s, 'value with')
    SETTINGS apply_string_filters_during_scan = 1);

-- Plain equality is deliberately not moved: it is too common,
-- and the primary key index usually does the job for it anyway.
SELECT countIf(explain LIKE '%Prewhere filter%') > 0 FROM (
    EXPLAIN SELECT count() FROM t_string_filter_move WHERE s = 'ordinary value 1'
    SETTINGS apply_string_filters_during_scan = 1);

-- Conditions that cannot be used as a string filter during the scan are not affected.
SELECT countIf(explain LIKE '%Prewhere filter%') > 0 FROM (
    EXPLAIN SELECT count() FROM t_string_filter_move WHERE length(s) > 20
    SETTINGS apply_string_filters_during_scan = 1);
SELECT countIf(explain LIKE '%Prewhere filter%') > 0 FROM (
    EXPLAIN SELECT count() FROM t_string_filter_move WHERE NOT (s LIKE '%needle%')
    SETTINGS apply_string_filters_during_scan = 1);
SELECT countIf(explain LIKE '%Prewhere filter%') > 0 FROM (
    EXPLAIN SELECT count() FROM t_string_filter_move WHERE position(s, 'needle') = 0
    SETTINGS apply_string_filters_during_scan = 1);
SELECT countIf(explain LIKE '%Prewhere filter%') > 0 FROM (
    EXPLAIN SELECT count() FROM t_string_filter_move WHERE s LIKE '%%'
    SETTINGS apply_string_filters_during_scan = 1);

-- `optimize_move_to_prewhere` is still respected.
SELECT countIf(explain LIKE '%Prewhere filter%') > 0 FROM (
    EXPLAIN SELECT count() FROM t_string_filter_move WHERE s LIKE '%needle%'
    SETTINGS apply_string_filters_during_scan = 1, optimize_move_to_prewhere = 0);

-- The results must be the same, and the filter must actually be applied during the scan.
SELECT count() FROM t_string_filter_move WHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 0;
SELECT count() FROM t_string_filter_move WHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 1;
SELECT sum(cityHash64(s)) FROM t_string_filter_move WHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 0;
SELECT sum(cityHash64(s)) FROM t_string_filter_move WHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 1;

SELECT count() FROM t_string_filter_move WHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 1, log_comment = '05055_string_filter_applied';
SYSTEM FLUSH LOGS query_log;
SELECT
    sum(ProfileEvents['StringValueFilterValuesChecked']) > 0,
    sum(ProfileEvents['StringValueFilterValuesReplaced']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05055_string_filter_applied';

DROP TABLE t_string_filter_move;
