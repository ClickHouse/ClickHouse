-- Regression test for the MemoryCredits final flush at query finish.
-- MemoryCredits is the time integral of memory usage (byte-microseconds). It is advanced only on
-- allocation/free transitions of the query's memory tracker, so the interval between the last
-- allocation/free and the moment the query finishes (the "tail") was previously lost. A query that
-- allocates memory and then holds it idle until it finishes must still account that held interval.
--
-- Both queries below build the same large IN-set, so the memory footprint and the accounting during
-- the set build are identical. The second query additionally holds that memory for a few seconds (an
-- idle `sleep`, during which no allocation or free happens) before it finishes. Only the final flush
-- at QueryFinish attributes that held interval, so the held query must accumulate substantially more
-- MemoryCredits than the one that finishes right after building the set. Without the flush the two
-- values are about equal, because the idle interval is not charged at all.

SET log_queries = 1;

-- Build the set and finish immediately.
SELECT count()
FROM numbers(1)
WHERE number IN (SELECT number FROM numbers(10000000))
FORMAT Null
SETTINGS max_threads = 1, log_comment = '04506_memory_credits_build';

-- Build the same set, then hold it idle until the query finishes.
SELECT count()
FROM numbers(1)
WHERE sleep(3) = 0 AND number IN (SELECT number FROM numbers(10000000))
FORMAT Null
SETTINGS max_threads = 1, log_comment = '04506_memory_credits_hold';

SYSTEM FLUSH LOGS query_log;

SELECT
    held > 0,
    held >= build * 2
FROM
(
    SELECT
        maxIf(ProfileEvents['MemoryCredits'], log_comment = '04506_memory_credits_build') AS build,
        maxIf(ProfileEvents['MemoryCredits'], log_comment = '04506_memory_credits_hold') AS held
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND log_comment IN ('04506_memory_credits_build', '04506_memory_credits_hold')
      AND type = 'QueryFinish'
);
