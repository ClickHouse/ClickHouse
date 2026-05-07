-- Test that max_rows_in_join / max_bytes_in_join limits are enforced
-- when SpillingHashJoin is enabled but data fits in memory (no spill).
-- This is a regression test: previously SpillingHashJoin passed
-- check_limits=false to the underlying join, silently disabling
-- join_overflow_mode enforcement.

SET max_block_size = 1000;
-- ====================================================================
-- Single-thread path (hash join)
-- ====================================================================
SET join_algorithm = 'hash';
SET max_threads = 1;
-- Enable spilling but set a high threshold so it doesn't trigger.
SET max_bytes_before_external_join = 1000000000;

-- Test 1: max_rows_in_join with throw mode should raise an error.
SELECT 'test 1: hash throw on max_rows_in_join';
SELECT count()
FROM (SELECT number AS n FROM numbers(4)) AS t1
ANY LEFT JOIN (SELECT number * 2 AS n, number + 10 AS j FROM numbers(4000)) AS t2
USING (n)
SETTINGS max_rows_in_join = 1000, join_overflow_mode = 'throw'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- Test 2: max_bytes_in_join with throw mode should raise an error.
SELECT 'test 2: hash throw on max_bytes_in_join';
SELECT count()
FROM (SELECT number AS n FROM numbers(4)) AS t1
ANY LEFT JOIN (SELECT number * 2 AS n, number + 10 AS j FROM numbers(4000)) AS t2
USING (n)
SETTINGS max_bytes_in_join = 1000, join_overflow_mode = 'throw'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- ====================================================================
-- Concurrent path (parallel_hash join)
-- ====================================================================
SET join_algorithm = 'parallel_hash';
SET max_threads = 4;

-- Test 3: max_rows_in_join with throw mode should raise an error (concurrent).
SELECT 'test 3: parallel_hash throw on max_rows_in_join';
SELECT count()
FROM (SELECT number AS n FROM numbers(4)) AS t1
ANY LEFT JOIN (SELECT number * 2 AS n, number + 10 AS j FROM numbers(4000)) AS t2
USING (n)
SETTINGS max_rows_in_join = 1000, join_overflow_mode = 'throw'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- Test 4: max_bytes_in_join with throw mode should raise an error (concurrent).
SELECT 'test 4: parallel_hash throw on max_bytes_in_join';
SELECT count()
FROM (SELECT number AS n FROM numbers(4)) AS t1
ANY LEFT JOIN (SELECT number * 2 AS n, number + 10 AS j FROM numbers(4000)) AS t2
USING (n)
SETTINGS max_bytes_in_join = 1000, join_overflow_mode = 'throw'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
