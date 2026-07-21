-- Regression test for the MemoryCredits accounting of idle-held memory (the "tail" at query finish).
-- MemoryCredits is the time integral of memory usage (byte-microseconds). It is advanced only on
-- allocation/free transitions of the query's memory tracker, so the interval between the last
-- allocation/free and the moment the query finishes was previously lost: a query that allocates
-- memory and then holds it idle (without allocating or freeing) must still account that held interval.
--
-- Both queries below build the identical large IN-set and hold it alive for their whole lifetime
-- (the set of an `IN (subquery)` lives until the query finishes). They differ only in how long they
-- then hold it idle: one sleeps for a short interval, the other for a much longer one. Because their
-- structure and their set-building phase are identical, the only systematic difference between their
-- MemoryCredits values is the extra idle interval of the longer query, which is charged only if the
-- held (idle) time is accounted. The longer-holding query must therefore strictly exceed the shorter
-- one. Without the fix the idle interval is not charged at all, so the two values are about equal and
-- the strict inequality fails.
--
-- We compare two queries that differ only in the idle-hold duration (rather than a fixed multiple or
-- a sleep-vs-no-sleep pair) so the result is robust to the unbounded, noisy set-building time: under
-- sanitizers or on a loaded runner the build can take a variable number of seconds, but that noise is
-- common to both queries and is dominated by the several-second difference in the idle-hold interval.

SET log_queries = 1;

-- Build the set and hold it idle for a short interval before finishing.
SELECT count()
FROM numbers(1)
WHERE sleep(1) = 0 AND number IN (SELECT number FROM numbers(4000000))
FORMAT Null
SETTINGS max_threads = 1, log_comment = '04506_memory_credits_hold_short';

-- Build the identical set and hold it idle for a much longer interval before finishing.
SELECT count()
FROM numbers(1)
WHERE sleep(6) = 0 AND number IN (SELECT number FROM numbers(4000000))
FORMAT Null
SETTINGS max_threads = 1, log_comment = '04506_memory_credits_hold_long';

SYSTEM FLUSH LOGS query_log;

SELECT
    held_short > 0,
    held_long > held_short
FROM
(
    SELECT
        maxIf(ProfileEvents['MemoryCredits'], log_comment = '04506_memory_credits_hold_short') AS held_short,
        maxIf(ProfileEvents['MemoryCredits'], log_comment = '04506_memory_credits_hold_long') AS held_long
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND log_comment IN ('04506_memory_credits_hold_short', '04506_memory_credits_hold_long')
      AND type = 'QueryFinish'
);
