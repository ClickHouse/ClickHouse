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

-- Writing to the query result cache builds the entry expiry as now() + seconds(query_cache_ttl); even the
-- largest ttl must not overflow that time_point addition, and the entry must stay non-stale (a wrapped
-- deadline would land in the past). The top-level write goes through executeQuery; the inner subquery
-- write goes through the Planner path (query_cache_for_subqueries). Both must be cached and non-stale.
SELECT 'qc_04327_top' SETTINGS use_query_cache = 1, query_cache_ttl = 9223372036854;
SELECT stale FROM system.query_cache WHERE query LIKE '%qc\_04327\_top%' AND query NOT LIKE '%system.query_cache%';

SELECT sum(number) FROM (SELECT number FROM numbers(5) WHERE number > 41000000) SETTINGS use_query_cache = 1, query_cache_for_subqueries = 1, query_cache_ttl = 9223372036854;
SELECT stale FROM system.query_cache WHERE query LIKE '%41000000%' AND query NOT LIKE '%system.query_cache%' ORDER BY query;
