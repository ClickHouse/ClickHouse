-- In BREAK mode, `waitUntil` must stop polling once the execution-time limit is reached instead of
-- continuing through the remaining retries. Without the fix the first query would keep polling for
-- about `max_tries * sleep_seconds` = 30 seconds; with the fix it stops after the first timed-out
-- sleep. The row it produces is then dropped by the limit-checking transform (BREAK means partial
-- results), so the query returns no rows. The timing check uses a wide margin (10 seconds against
-- a 30-second unfixed run) to stay robust on loaded CI machines.
CREATE TEMPORARY TABLE start_time AS SELECT now64() AS start;

SELECT result FROM waitUntil(false, 30, 1) SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break';

SELECT if(dateDiff('second', (SELECT start FROM start_time), now64()) < 10, 'fast', 'slow');

-- In THROW mode the same time limit raises `TIMEOUT_EXCEEDED` from the sleep loop.
SELECT result FROM waitUntil(false, 30, 1) SETTINGS max_execution_time = 1, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }
