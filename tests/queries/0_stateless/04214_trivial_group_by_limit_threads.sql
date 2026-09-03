-- Regression test for `OptimizeTrivialGroupByLimitPass` in multi-threaded mode.
--
-- The optimization sets `max_rows_to_group_by = LIMIT + OFFSET` with
-- `group_by_overflow_mode = 'any'`. That check is enforced per-thread, so
-- different threads can pick different "first N" key sets and drop rows for
-- keys not in their own set. When the projection contains an aggregate
-- function, the dropped rows mean the aggregate states for the kept keys
-- are missing contributions from those threads, so the resulting values
-- are wrong. The optimization must therefore not fire for queries with
-- aggregate functions in the projection.
--
-- `numbers_mt` is used so each parallel aggregation thread reads its own
-- contiguous range of `number` and therefore sees a different cycle of
-- keys early. With `max_block_size = 1` the per-thread `no_more_keys`
-- flag fires after the first few distinct keys, so each thread drops the
-- keys it has not yet seen and produces an undercount for the rest of
-- the input. The true count for every key is 100.
--
-- The result captures the LIMIT-5 output of the GROUP BY into a table
-- because the optimization pass currently only fires on the top-level
-- query, so the buggy run must be a standalone `INSERT … SELECT`.
--
-- Input size is kept small (10000 rows) because `max_block_size = 1`
-- has heavy per-block overhead and would otherwise exceed the
-- `max_estimated_execution_time` budget on debug/sanitized builds.

DROP TABLE IF EXISTS t_trivial_group_by_limit_threads;
CREATE TABLE t_trivial_group_by_limit_threads (k UInt64, c UInt64) ENGINE = Memory;

INSERT INTO t_trivial_group_by_limit_threads
SELECT k, count() AS c
FROM (SELECT number % 100 AS k FROM numbers_mt(10000))
GROUP BY k
LIMIT 5
SETTINGS optimize_trivial_group_by_limit_query = 1, max_threads = 4, max_block_size = 1;

-- The kept keys must all have count = 100. With the optimization-bug,
-- they get values like 25 instead, so `max(c != 100)` would be 1.
-- After the fix the optimization is skipped because of `count()`, so all
-- kept keys have the true count.
SELECT max(c != 100), count() FROM t_trivial_group_by_limit_threads;

DROP TABLE t_trivial_group_by_limit_threads;
