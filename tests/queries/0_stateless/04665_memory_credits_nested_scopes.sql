-- Tags: no-async-insert
-- MemoryCredits is query-scoped: nested Process groups (materialized views) and per-thread
-- counters must not report a nonzero value; the enclosing INSERT's query_log row must.

SET log_queries = 1;
SET log_query_threads = 1;
SET log_query_views = 1;
SET log_profile_events = 1;

DROP TABLE IF EXISTS 04665_src;
DROP TABLE IF EXISTS 04665_dst;
DROP TABLE IF EXISTS 04665_mv;

CREATE TABLE 04665_src (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE 04665_dst (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW 04665_mv TO 04665_dst AS SELECT k, s FROM 04665_src;

-- Allocate enough memory (wide strings) that the parent query integral is strictly positive.
INSERT INTO 04665_src
SELECT
    number,
    concat(repeat('x', 4096), toString(number))
FROM numbers(2000)
SETTINGS max_threads = 2, log_comment = '04665_memory_credits_nested_scopes';

SYSTEM FLUSH LOGS query_log, query_thread_log, query_views_log;

SELECT ProfileEvents['MemoryCredits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '04665_memory_credits_nested_scopes'
  AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT (count() > 0) AND (countIf(ProfileEvents['MemoryCredits'] > 0) = 0)
FROM system.query_views_log
WHERE initial_query_id = (
    SELECT query_id
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND log_comment = '04665_memory_credits_nested_scopes'
      AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
);

SELECT (count() > 0) AND (countIf(ProfileEvents['MemoryCredits'] > 0) = 0)
FROM system.query_thread_log
WHERE current_database = currentDatabase()
  AND query_id = (
    SELECT query_id
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND log_comment = '04665_memory_credits_nested_scopes'
      AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
);

DROP TABLE IF EXISTS 04665_mv;
DROP TABLE IF EXISTS 04665_dst;
DROP TABLE IF EXISTS 04665_src;
