-- Tags: no-parallel
-- no-parallel: enables a failpoint, which affects merges of other tables.

-- An exception in the thread that reads the columns of a Vertical merge fails the merge,
-- the source parts stay, and the next merge succeeds.

DROP TABLE IF EXISTS t_read_thread_exception;

CREATE TABLE t_read_thread_exception (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_read_in_separate_thread = 1,
    max_bytes_to_merge_at_min_space_in_pool = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_read_thread_exception SELECT number, toString(number) FROM numbers(0, 1000);
INSERT INTO t_read_thread_exception SELECT number, toString(number) FROM numbers(1000, 1000);
INSERT INTO t_read_thread_exception SELECT number, toString(number) FROM numbers(2000, 1000);

SYSTEM ENABLE FAILPOINT merge_tree_sequential_source_throw_before_read;
OPTIMIZE TABLE t_read_thread_exception FINAL; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT merge_tree_sequential_source_throw_before_read;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_read_thread_exception' AND active;

OPTIMIZE TABLE t_read_thread_exception FINAL;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_read_thread_exception' AND active;
SELECT count(), sum(id), countIf(s = toString(id)) FROM t_read_thread_exception;

DROP TABLE t_read_thread_exception;
