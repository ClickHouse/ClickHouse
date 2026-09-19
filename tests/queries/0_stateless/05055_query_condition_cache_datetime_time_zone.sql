-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: asserts QueryConditionCacheHits on the instance-wide query condition cache, which a
-- sibling test's SYSTEM CLEAR QUERY CONDITION CACHE would invalidate.
-- Tag no-parallel-replicas: the query condition cache is populated per replica.

-- The query condition cache remembers, per part, which marks a filter matched no row in, keyed by a
-- hash of the filter. A time zone left out of a `DateTime` is the session time zone, so a filter over
-- one answers differently in each session and the two sessions must not share an entry. Epoch 0 is
-- hour 0 in UTC and hour 9 in Asia/Tokyo.

-- Does additional QCC lookups that the test doesn't expect
SET automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 0;

-- w/o local plan for parallel replicas the test will fail in ParallelReplicas CI run since filter steps will be executed as part of remote queries
SET parallel_replicas_local_plan = 1;

SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;

DROP TABLE IF EXISTS qcc_tz;

-- Every row holds the same value, so each filter below matches either every row or none and a
-- per-mark verdict is unambiguous. No primary key, no minmax index and no statistics leave the query
-- condition cache as the only thing that can drop a mark.
CREATE TABLE qcc_tz (x UInt32) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS add_minmax_index_for_numeric_columns = 0, auto_statistics_types = '';
INSERT INTO qcc_tz SELECT 0 FROM numbers(1_000_000); -- 1 mio rows sounds like a lot but the QCC doesn't cache anything for less data

SELECT '--- an omitted time zone, across sessions';

SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT count() FROM qcc_tz WHERE toHour(toDateTime(0) + x) = 0 SETTINGS use_query_condition_cache = true, session_timezone = 'Asia/Tokyo';

SELECT count() FROM qcc_tz WHERE toHour(toDateTime(0) + x) = 0 SETTINGS use_query_condition_cache = true, session_timezone = 'UTC';

SELECT '--- an omitted time zone, one session';

SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT count() FROM qcc_tz WHERE toHour(toDateTime(0) + x) = 5 SETTINGS use_query_condition_cache = true, session_timezone = 'UTC';

SELECT count() FROM qcc_tz WHERE toHour(toDateTime(0) + x) = 5 SETTINGS use_query_condition_cache = true, session_timezone = 'UTC';

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT count() FROM qcc_tz WHERE toHour(toDateTime(0) + x) = 5 SETTINGS use_query_condition_cache = true, session_timezone = ''UTC'';'
ORDER BY
    event_time_microseconds;

SELECT '--- a time zone omitted from a declared name, across sessions';

SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT count() FROM qcc_tz WHERE toHour(CAST(x, 'SimpleAggregateFunction(any, DateTime)')) = 0 SETTINGS use_query_condition_cache = true, session_timezone = 'Asia/Tokyo';

SELECT count() FROM qcc_tz WHERE toHour(CAST(x, 'SimpleAggregateFunction(any, DateTime)')) = 0 SETTINGS use_query_condition_cache = true, session_timezone = 'UTC';

SELECT '--- a time zone omitted from a declared name, one session';

SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT count() FROM qcc_tz WHERE toHour(CAST(x, 'SimpleAggregateFunction(any, DateTime)')) = 5 SETTINGS use_query_condition_cache = true, session_timezone = 'UTC';

SELECT count() FROM qcc_tz WHERE toHour(CAST(x, 'SimpleAggregateFunction(any, DateTime)')) = 5 SETTINGS use_query_condition_cache = true, session_timezone = 'UTC';

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT count() FROM qcc_tz WHERE toHour(CAST(x, ''SimpleAggregateFunction(any, DateTime)'')) = 5 SETTINGS use_query_condition_cache = true, session_timezone = ''UTC'';'
ORDER BY
    event_time_microseconds;

SELECT '--- a time zone omitted under a wrapper, across sessions';

SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT count() FROM qcc_tz WHERE toHour(CAST(x, 'Nullable(DateTime)')) = 0 SETTINGS use_query_condition_cache = true, session_timezone = 'Asia/Tokyo';

SELECT count() FROM qcc_tz WHERE toHour(CAST(x, 'Nullable(DateTime)')) = 0 SETTINGS use_query_condition_cache = true, session_timezone = 'UTC';

SELECT '--- a time zone omitted under a wrapper, one session';

SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT count() FROM qcc_tz WHERE toHour(CAST(x, 'Nullable(DateTime)')) = 5 SETTINGS use_query_condition_cache = true, session_timezone = 'UTC';

SELECT count() FROM qcc_tz WHERE toHour(CAST(x, 'Nullable(DateTime)')) = 5 SETTINGS use_query_condition_cache = true, session_timezone = 'UTC';

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT count() FROM qcc_tz WHERE toHour(CAST(x, ''Nullable(DateTime)'')) = 5 SETTINGS use_query_condition_cache = true, session_timezone = ''UTC'';'
ORDER BY
    event_time_microseconds;

DROP TABLE qcc_tz;
