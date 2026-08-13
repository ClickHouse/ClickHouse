-- Regression test for the trivial `GROUP BY ... LIMIT` optimization in multi-threaded
-- mode with an aggregate function in the projection.
--
-- The optimization caps the aggregation at `LIMIT + OFFSET` distinct keys. A per-thread
-- cap (plain `max_rows_to_group_by` with `group_by_overflow_mode = 'any'`) would be
-- unsound here: different threads pick different "first N" key sets and drop rows for
-- keys not in their own set, so the kept keys come out of the merge with undercounted
-- values. Instead, once the first thread exceeds the cap, all threads are restricted to
-- a single shared set of kept keys, and the values of the returned keys stay exact.
--
-- `numbers_mt` is used so each parallel aggregation thread reads its own contiguous
-- range of `number` and therefore sees a different cycle of keys early. With
-- `max_block_size = 1` the cap is exceeded after the first few distinct keys, so
-- without the shared kept-keys cutoff each thread would drop the keys it has not yet
-- seen and produce an undercount. The true count for every key is 100. The key is cast
-- to `UInt64` because for 8/16-bit keys (fixed hash maps bounded by the key space) the
-- cutoff intentionally stays inert.
--
-- The LIMIT-5 output of the GROUP BY is captured into a table because a bare SELECT
-- would not demonstrate the wrong values deterministically enough in the output.
--
-- Input size is kept small (10000 rows) because `max_block_size = 1`
-- has heavy per-block overhead and would otherwise exceed the
-- `max_estimated_execution_time` budget on debug/sanitized builds.

DROP TABLE IF EXISTS t_trivial_group_by_limit_threads;
CREATE TABLE t_trivial_group_by_limit_threads (k UInt64, c UInt64) ENGINE = Memory;

INSERT INTO t_trivial_group_by_limit_threads
SELECT k, count() AS c
FROM (SELECT toUInt64(number % 100) AS k FROM numbers_mt(10000))
GROUP BY k
LIMIT 5
SETTINGS optimize_trivial_group_by_limit_query = 1, max_threads = 4, max_block_size = 1;

-- The kept keys must all have the exact count of 100. With a per-thread cutoff they
-- would get values like 25 instead, so `max(c != 100)` would be 1.
SELECT max(c != 100), count() FROM t_trivial_group_by_limit_threads;

DROP TABLE t_trivial_group_by_limit_threads;
