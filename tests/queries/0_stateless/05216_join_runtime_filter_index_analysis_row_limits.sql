-- `enable_join_runtime_filters_index_analysis` prunes probe-side granules while the parts are read, which is
-- after the `max_rows_to_read` / `max_rows_to_read_leaf` estimate check of `filterPartsByPrimaryKeyAndSkipIndexes`
-- has run. Read-time skip indexes veto themselves under `read_overflow_mode = 'throw'` because they would
-- otherwise have been applied at plan time and shrunk that estimate. A runtime filter has no plan-time
-- alternative: the estimate is the same with the setting on or off, so a row-limited query that throws with
-- the setting on threw before it was enabled as well, and the setting must not be vetoed by the row limits -
-- the execution-time limit (`read_overflow_mode = 'break'`) does see the pruning and gets a complete result.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 1;
SET join_algorithm = 'hash';
SET max_bytes_ratio_before_external_join = 0;
SET use_skip_indexes_on_data_read = 1;
SET max_threads = 1;
SET max_block_size = 8192;

DROP TABLE IF EXISTS probe_row_limits;
DROP TABLE IF EXISTS build_row_limits;

CREATE TABLE probe_row_limits (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
INSERT INTO probe_row_limits SELECT number, number FROM numbers(100000);
CREATE TABLE build_row_limits (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO build_row_limits SELECT number * 10000 FROM numbers(10);

-- The estimate check sees the whole probe table, with the setting on and off alike.
SELECT count() FROM probe_row_limits JOIN build_row_limits ON probe_row_limits.k = build_row_limits.k
SETTINGS enable_join_runtime_filters_index_analysis = 0, max_rows_to_read = 1000, read_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }
SELECT count() FROM probe_row_limits JOIN build_row_limits ON probe_row_limits.k = build_row_limits.k
SETTINGS enable_join_runtime_filters_index_analysis = 1, max_rows_to_read = 1000, read_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }
SELECT count() FROM probe_row_limits JOIN build_row_limits ON probe_row_limits.k = build_row_limits.k
SETTINGS enable_join_runtime_filters_index_analysis = 0, max_rows_to_read_leaf = 1000, read_overflow_mode_leaf = 'throw'; -- { serverError TOO_MANY_ROWS }
SELECT count() FROM probe_row_limits JOIN build_row_limits ON probe_row_limits.k = build_row_limits.k
SETTINGS enable_join_runtime_filters_index_analysis = 1, max_rows_to_read_leaf = 1000, read_overflow_mode_leaf = 'throw'; -- { serverError TOO_MANY_ROWS }

-- The execution-time limit does see the pruning: only the ten granules holding a build key are read, which stays
-- below the limit, so the result is complete. Without the pruning the read is cut after the first block.
SELECT count() FROM probe_row_limits JOIN build_row_limits ON probe_row_limits.k = build_row_limits.k
SETTINGS enable_join_runtime_filters_index_analysis = 1, max_rows_to_read = 1000, read_overflow_mode = 'break';
SELECT count() < 10 FROM probe_row_limits JOIN build_row_limits ON probe_row_limits.k = build_row_limits.k
SETTINGS enable_join_runtime_filters_index_analysis = 0, max_rows_to_read = 1000, read_overflow_mode = 'break';

DROP TABLE probe_row_limits;
DROP TABLE build_row_limits;
