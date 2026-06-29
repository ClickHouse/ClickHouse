-- The MemoryCredits profile event is the time integral of memory usage (byte-microseconds).
-- A memory-using query must accumulate a strictly positive value, attributed to that query.

SET log_queries = 1;

SELECT count() FROM (SELECT number FROM numbers(10000000) GROUP BY number % 100000) FORMAT Null
SETTINGS max_threads = 4, log_comment = '03204_memory_credits';

SYSTEM FLUSH LOGS;

SELECT ProfileEvents['MemoryCredits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '03204_memory_credits'
  AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;
