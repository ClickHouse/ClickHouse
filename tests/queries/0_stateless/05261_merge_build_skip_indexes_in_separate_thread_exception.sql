-- Tags: no-parallel
-- no-parallel: enables a failpoint, which affects merges and inserts of other tables.

-- An exception in the thread that builds skip indexes fails the merge, the source parts stay,
-- and the next merge succeeds.

DROP TABLE IF EXISTS t_skip_thread_exception;

CREATE TABLE t_skip_thread_exception
(
    id UInt64,
    s String,
    INDEX idx_bf s TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    merge_build_skip_indexes_in_separate_thread = 1,
    max_bytes_to_merge_at_min_space_in_pool = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_skip_thread_exception SELECT number, toString(number) FROM numbers(0, 1000);
INSERT INTO t_skip_thread_exception SELECT number, toString(number) FROM numbers(1000, 1000);
INSERT INTO t_skip_thread_exception SELECT number, toString(number) FROM numbers(2000, 1000);

SYSTEM ENABLE FAILPOINT merge_tree_skip_indices_calculation_throw;
OPTIMIZE TABLE t_skip_thread_exception FINAL; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT merge_tree_skip_indices_calculation_throw;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_skip_thread_exception' AND active;

OPTIMIZE TABLE t_skip_thread_exception FINAL;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_skip_thread_exception' AND active;
SELECT count(), sum(id), countIf(s = toString(id)) FROM t_skip_thread_exception;
SELECT count() FROM t_skip_thread_exception WHERE s = '1500' SETTINGS force_data_skipping_indices = 'idx_bf';

DROP TABLE t_skip_thread_exception;
