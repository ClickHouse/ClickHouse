-- A huge Millisecond-typed wait timeout must saturate, not wrap. 9223372036854775 = INT64_MAX / 1000.
SELECT toUInt64(value) <= 9223372036854775
FROM system.settings WHERE name = 'queue_max_wait_ms'
SETTINGS queue_max_wait_ms = 18446744073709551615;

SELECT toUInt64(value) <= 9223372036854775
FROM system.settings WHERE name = 'replace_running_query_max_wait_ms'
SETTINGS replace_running_query_max_wait_ms = 18446744073709551615;

SELECT toUInt64(value) <= 9223372036854775
FROM system.settings WHERE name = 'low_priority_query_wait_time_ms'
SETTINGS low_priority_query_wait_time_ms = 18446744073709551615;

SELECT toUInt64(value) <= 9223372036854775
FROM system.settings WHERE name = 'connection_pool_max_wait_ms'
SETTINGS connection_pool_max_wait_ms = 18446744073709551615;

SELECT toUInt64(value) <= 9223372036854775
FROM system.settings WHERE name = 'kafka_max_wait_ms'
SETTINGS kafka_max_wait_ms = 18446744073709551615;

-- A query carrying a huge interactive_delay must still complete (lazy-output queue wait is clamped).
SELECT 1 SETTINGS interactive_delay = 100000000000000000;

-- A huge lock_acquire_timeout must not overflow the table-lock acquire deadline (now() + timeout);
-- the query just acquires the lock and completes.
SELECT count() > 0 FROM numbers(1000) SETTINGS lock_acquire_timeout = 100000000000;

-- A negative lock_acquire_timeout must saturate to an immediate deadline, not underflow now() + timeout.
SELECT count() > 0 FROM numbers(1000) SETTINGS lock_acquire_timeout = -100000000000;

-- SYSTEM SYNC MERGES builds its deadline as now() + max_execution_time; a huge value must not overflow.
-- SYSTEM commands take no trailing SETTINGS clause, so the timeout is set on the session. With all
-- scheduled parts already covered the command returns immediately, so this just exercises the clamp.
DROP TABLE IF EXISTS t_04327_sync_merges;
CREATE TABLE t_04327_sync_merges (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS merge_selector_algorithm = 'Manual';
INSERT INTO t_04327_sync_merges VALUES (1);
SET max_execution_time = 100000000000;
SYSTEM SYNC MERGES t_04327_sync_merges;
SET max_execution_time = DEFAULT;
SELECT 1;
DROP TABLE t_04327_sync_merges;
