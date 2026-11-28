-- Tags: memory-engine

-- With both backoff settings at INT64_MAX the retry instant overflowed into the past and all 11 attempts
-- ran at once; SYSTEM WAIT VIEW waits through them. It must instead stop on attempt 1, scheduled.
CREATE MATERIALIZED VIEW rmv REFRESH AFTER 1 YEAR
    SETTINGS refresh_retries = 10,
             refresh_retry_initial_backoff_ms = 9223372036854775807,
             refresh_retry_max_backoff_ms = 9223372036854775807,
             all_replicas = 1
    APPEND (x Int64) ENGINE = Memory AS SELECT throwIf(number = 0) AS x FROM numbers(1);

SYSTEM WAIT VIEW rmv; -- { serverError REFRESH_FAILED }
SELECT status, retry FROM system.view_refreshes WHERE view = 'rmv' AND database = currentDatabase();

DROP TABLE rmv;
