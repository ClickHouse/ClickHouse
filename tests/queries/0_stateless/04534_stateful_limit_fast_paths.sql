-- Regression tests for stateful functions (`IFunctionBase::isStateful`, e.g. `neighbor`,
-- `runningAccumulate`, `logTrace`) under LIMIT-related optimizations. A limit must not truncate
-- or re-chunk the input of a stateful expression: the simple-LIMIT source fast paths, the generic
-- limit pushdown, and the read-in-order early termination all have to leave the stateful
-- expression's input intact.
-- See https://github.com/ClickHouse/ClickHouse/pull/110188.

SET allow_deprecated_error_prone_window_functions = 1;
-- The distributed plan of parallel replicas is not the mode under test.
SET enable_parallel_replicas = 0;

-- Plain LIMIT over a numbers-like source: without the guards, the limit is pushed into the source
-- (as a block size cap and as a row cap), so `neighbor` would see a single-row block and return
-- the default value 0 instead of the next row's value.
SELECT neighbor(number, 1) FROM system.numbers LIMIT 1 SETTINGS enable_analyzer = 1;
SELECT neighbor(number, 1) FROM system.numbers LIMIT 1 SETTINGS enable_analyzer = 0;

-- ORDER BY over the primary key with LIMIT: without the guard, read-in-order propagates the limit
-- into the storage read, so the first block is truncated to a single row and `neighbor` would
-- return the default value 0 instead of the next row's value.
DROP TABLE IF EXISTS t_stateful_limit;
CREATE TABLE t_stateful_limit (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO t_stateful_limit SELECT number, number + 100 FROM numbers(10);
SELECT neighbor(v, 1) FROM t_stateful_limit ORDER BY k LIMIT 1 SETTINGS optimize_read_in_order = 1, enable_analyzer = 1;
SELECT neighbor(v, 1) FROM t_stateful_limit ORDER BY k LIMIT 1 SETTINGS optimize_read_in_order = 1, enable_analyzer = 0;
DROP TABLE t_stateful_limit;
