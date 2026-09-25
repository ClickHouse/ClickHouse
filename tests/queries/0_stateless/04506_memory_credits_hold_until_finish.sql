-- Regression test for the MemoryCredits accounting of idle-held memory (the "tail" at query finish).
-- MemoryCredits is the time integral of memory usage (byte-microseconds). It is advanced only on
-- allocation/free transitions of the query's memory tracker, so the interval between the last
-- allocation/free and the moment the query finishes was previously lost: a query that allocates
-- memory and then holds it idle (without allocating or freeing) must still account that held interval.
--
-- The query below builds a large IN-set (2000 strings of 16 KiB, at least 32 MiB of keys) and then holds
-- it alive while it sleeps for 6 seconds (the set of an `IN (subquery)` lives until the query finishes).
-- The held interval alone is worth more than 16 MiB x 5 s, which is the lower bound checked below. This
-- bound does not depend on how long it takes to build the set (under sanitizers or on a loaded runner the
-- build is slow, which only increases the value), so it is robust. Without the tail accounting, only the
-- set-building phase is charged, which is far below the bound in a normal run.
--
-- An earlier version compared this query with an identical one that slept for a shorter time, but the
-- variation of the set-building time between the two queries on a loaded runner could exceed the
-- difference of the idle intervals, so the comparison was flaky.
--
-- The server caps every sleep call at 3 seconds per block (for sleepEachRow the cap applies to the
-- per-block total), so a single sleep(6) is rejected with TOO_SLOW. To hold for longer, the query
-- reads 2 rows with max_block_size = 1, so sleepEachRow runs once per single-row block and each call
-- stays within the cap. The set is made large by the width of its rows rather than by their number,
-- so that building it stays cheap even with max_block_size = 1, which applies to the subquery as well.

SET log_queries = 1;

SELECT count()
FROM numbers(2)
WHERE sleepEachRow(3) = 0
  AND concat(repeat('x', 16384), toString(number)) IN (SELECT concat(repeat('x', 16384), toString(number)) FROM numbers(2000))
FORMAT Null
SETTINGS max_threads = 1, max_block_size = 1, log_comment = '04506_memory_credits_hold_long';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['MemoryCredits'] >= 16 * 1048576 * 5000000
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '04506_memory_credits_hold_long'
  AND type = 'QueryFinish';
