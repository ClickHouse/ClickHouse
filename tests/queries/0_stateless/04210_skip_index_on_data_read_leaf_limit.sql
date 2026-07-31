-- Tags: no-parallel-replicas
-- no-parallel-replicas: use_skip_indexes_on_data_read is not supported with parallel replicas

-- Regression test for issue #104328:
-- With default `use_skip_indexes_on_data_read = 1`, queries with `max_rows_to_read_leaf` and
-- `read_overflow_mode_leaf = 'throw'` incorrectly threw TOO_MANY_ROWS even when the skip index
-- would reduce the read to far fewer rows than the leaf cap.
-- The non-leaf variant (`max_rows_to_read` + `read_overflow_mode = 'throw'`) was already
-- handled in PR #88504; this test covers the symmetric leaf path.

DROP TABLE IF EXISTS t_104328;

CREATE TABLE t_104328 (
    id UInt64,
    val UInt64,
    INDEX val_idx val TYPE minmax GRANULARITY 1
) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_104328 SELECT number, number FROM numbers(100);

-- Disable the query-condition cache so a previous query cannot mask the bug for the leaf path.
SET use_query_condition_cache = 0;

-- Already covered by PR #88504: `max_rows_to_read` + throw -> data-read skip index disabled,
-- planning-time index pruning cuts the estimate to 1 row, query succeeds.
SELECT count() FROM t_104328 WHERE val = 50
SETTINGS use_skip_indexes_on_data_read = 1, max_rows_to_read = 5, read_overflow_mode = 'throw';

-- Issue #104328: same setup but using the symmetric leaf settings.
-- Before the fix this threw TOO_MANY_ROWS (max rows: 5.00, current rows: 100.00) because the
-- early estimate at MergeTreeDataSelectExecutor.cpp:1083 saw 100 unfiltered rows.
-- Expected: 1.
SELECT count() FROM t_104328 WHERE val = 50
SETTINGS use_skip_indexes_on_data_read = 1, max_rows_to_read_leaf = 5, read_overflow_mode_leaf = 'throw';

-- Setting both leaf and non-leaf together also succeeds.
SELECT count() FROM t_104328 WHERE val = 50
SETTINGS use_skip_indexes_on_data_read = 1,
         max_rows_to_read = 5, read_overflow_mode = 'throw',
         max_rows_to_read_leaf = 5, read_overflow_mode_leaf = 'throw';

-- Sanity: with `use_skip_indexes_on_data_read = 0` the leaf path was already correct.
SELECT count() FROM t_104328 WHERE val = 50
SETTINGS use_skip_indexes_on_data_read = 0, max_rows_to_read_leaf = 5, read_overflow_mode_leaf = 'throw';

-- Sanity: a leaf limit larger than the actual rows must still succeed regardless of the fix
-- (this exercises the path where the fix returns false and the runtime check is satisfied).
SELECT count() FROM t_104328 WHERE val = 50
SETTINGS use_skip_indexes_on_data_read = 1, max_rows_to_read_leaf = 200, read_overflow_mode_leaf = 'throw';

-- Sanity: with `read_overflow_mode_leaf = 'break'` the fix does NOT trigger; the data-read
-- skip index path is still used, and the result is correct because the index drops 99 granules.
SELECT count() FROM t_104328 WHERE val = 50
SETTINGS use_skip_indexes_on_data_read = 1, max_rows_to_read_leaf = 5, read_overflow_mode_leaf = 'break';

DROP TABLE t_104328;
