-- `lock_acquire_timeout` also bounds how long index analysis waits for a thread of the
-- `MergeTreeDataSelectExecutor` pool: its Int64 microsecond value is passed to
-- `ThreadPoolImpl::scheduleOrThrow`, which used to take the timeout as a `uint64_t`, so a negative
-- setting arrived as a huge unsigned count and `wait_for` overflowed Int64 while multiplying it by
-- 1'000 to reach nanoseconds.
--
-- This is an end-to-end check that such a setting reaches index analysis and the query still runs. It
-- does not saturate the pool, so it does not by itself reach the timed wait: the deterministic coverage
-- of that branch is in `src/Common/tests/gtest_thread_pool_schedule_timeout.cpp`.

DROP TABLE IF EXISTS t_05060;

-- Several parts, so index analysis takes the parallel branch and schedules jobs on that pool.
-- `merge_selector_algorithm = 'Manual'` keeps a background merge from collapsing them into one part.
CREATE TABLE t_05060 (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS merge_selector_algorithm = 'Manual', index_granularity = 1;

INSERT INTO t_05060 SELECT number FROM numbers(8);
INSERT INTO t_05060 SELECT number + 8 FROM numbers(8);
INSERT INTO t_05060 SELECT number + 16 FROM numbers(8);
INSERT INTO t_05060 SELECT number + 24 FROM numbers(8);

-- The parallel branch is taken only with more than one part, so assert the precondition.
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_05060' AND active;

-- `sum` (unlike `count`) cannot be answered from the implicit minmax_count projection, so the read
-- really goes through primary key analysis.
SELECT sum(x) FROM t_05060 WHERE x > 3 SETTINGS lock_acquire_timeout = -100000000000, max_threads = 4;

-- A huge positive timeout reaches the same multiplication and must be clamped as well.
SELECT sum(x) FROM t_05060 WHERE x > 3 SETTINGS lock_acquire_timeout = 100000000000, max_threads = 4;

DROP TABLE t_05060;
