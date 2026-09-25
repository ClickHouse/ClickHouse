-- Tags: no-fasttest
-- no-fasttest: the `File(Parquet)` case below needs the Parquet format, which the fast-test
-- build does not have.
-- The scan-time string filter (`apply_string_filters_during_scan`) replaces non-matching values with
-- empty strings, and the row is rejected only after PREWHERE has been evaluated. The row-level filter
-- (row policy) is a separate expression that the readers evaluate on the scanned columns *before*
-- PREWHERE, so it would observe the substituted empty strings of every row that the substring condition
-- later rejects. Therefore the optimization must be disabled for a column that the row policy reads.
-- Here `throwIf` would fire for every non-matching row if the value had been replaced.

DROP TABLE IF EXISTS t_string_filter_row_policy;
DROP TABLE IF EXISTS t_string_filter_row_policy_parquet;

-- With parallel replicas the `INSERT INTO ... SELECT` into the `File` table would be executed by several
-- replicas writing the same file (`Lock timeout exceeded`), and the reads would be done by the replicas,
-- which makes the `ProfileEvents` assertion at the end depend on where the scan happened.
SET enable_parallel_replicas = 0;

CREATE TABLE t_string_filter_row_policy (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_string_filter_row_policy
SELECT number, if(number % 3 = 0, 'lorem needle ipsum ' || toString(number), 'nothing interesting ' || toString(number))
FROM numbers(10000);

CREATE TABLE t_string_filter_row_policy_parquet (id UInt32, s String) ENGINE = File(Parquet);

INSERT INTO t_string_filter_row_policy_parquet
SELECT number, if(number % 3 = 0, 'lorem needle ipsum ' || toString(number), 'nothing interesting ' || toString(number))
FROM numbers(10000);

CREATE ROW POLICY OR REPLACE guard_05227 ON t_string_filter_row_policy
    USING throwIf(empty(s), 'the value was replaced') = 0 TO ALL;

CREATE ROW POLICY OR REPLACE guard_05227_parquet ON t_string_filter_row_policy_parquet
    USING throwIf(empty(s), 'the value was replaced') = 0 TO ALL;

SET apply_string_filters_during_scan = 1, optimize_move_to_prewhere = 0;

-- No row has an empty `s`, so the row policy must not throw, and the count must be the number of
-- the matching rows.
SELECT 'MergeTree';
SELECT count() FROM t_string_filter_row_policy PREWHERE s LIKE '%needle%' SETTINGS log_comment = '05227_row_policy_guard';
SELECT count() FROM t_string_filter_row_policy PREWHERE startsWith(s, 'lorem') AND id % 2 = 0 SETTINGS log_comment = '05227_row_policy_guard';

SELECT 'Parquet';
SELECT count() FROM t_string_filter_row_policy_parquet PREWHERE s LIKE '%needle%' SETTINGS log_comment = '05227_row_policy_guard';
SELECT count() FROM t_string_filter_row_policy_parquet PREWHERE startsWith(s, 'lorem') AND id % 2 = 0 SETTINGS log_comment = '05227_row_policy_guard';

-- Control: a row policy on another column does not disable the pushdown, and the result is the same.
SELECT 'control';
CREATE ROW POLICY OR REPLACE guard_05227 ON t_string_filter_row_policy USING id >= 0 TO ALL;
SELECT count() FROM t_string_filter_row_policy PREWHERE s LIKE '%needle%' SETTINGS log_comment = '05227_row_policy_control';

-- The filter must have been disabled for the guarded queries and applied for the control.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, sum(ProfileEvents['StringValueFilterValuesChecked']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05227_row_policy_%'
GROUP BY log_comment ORDER BY log_comment;

DROP ROW POLICY guard_05227 ON t_string_filter_row_policy;
DROP ROW POLICY guard_05227_parquet ON t_string_filter_row_policy_parquet;
DROP TABLE t_string_filter_row_policy;
DROP TABLE t_string_filter_row_policy_parquet;
