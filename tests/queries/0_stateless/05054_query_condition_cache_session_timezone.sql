-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache

-- Tests that the query condition cache does not serve one session's verdict to a session with a
-- different `session_timezone`. The cache records, per mark, whether a condition matched any row,
-- and a date or time function reads the time zone while the condition runs, so the same condition
-- can match different rows in two sessions. Each block below warms the cache in `Asia/Tokyo`, where
-- the hour of the epoch is 9 and nothing matches, then repeats the query in `UTC`, where the hour
-- is 0 and every row matches.

-- The query condition cache is analyzer-only, so the old-analyzer lanes would write no entry at all.
SET allow_experimental_analyzer = 1;
SET use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
-- Parallel replicas do extra cache lookups and run filter steps remotely, which the hit and miss
-- counts at the end of this test do not expect.
SET automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 0, parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab;

-- A `Dynamic` value only resolves its subtype against the reading session from serialization
-- version v3 on; under v1 and v2 the time zone is fixed when the part is written, which would make
-- the `Dynamic` block below independent of `session_timezone` and therefore prove nothing.
CREATE TABLE tab (arr Array(UInt32), d Dynamic, x UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 0, auto_statistics_types = '', dynamic_serialization_version = 'v3';

INSERT INTO tab SELECT [toUInt32(0)], toDateTime(0)::Dynamic, toUInt32(0) FROM numbers(1_000_000);

-- Each block prints: the warm-up count (legitimately 0), whether the cache really holds an entry (a
-- block that cached nothing would pass on any build), the count in the other time zone, and the same
-- count with the cache emptied first.

-- The time zone is read inside a lambda body, which is not part of the lambda's type.
SELECT '-- lambda body';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM tab WHERE arrayExists(y -> toHour(toDateTime(0) + y) = 0, arr) SETTINGS session_timezone = 'Asia/Tokyo', use_query_condition_cache = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SELECT count() > 0 FROM system.query_condition_cache;
SELECT count() FROM tab WHERE arrayExists(y -> toHour(toDateTime(0) + y) = 0, arr) SETTINGS session_timezone = 'UTC', use_query_condition_cache = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM tab WHERE arrayExists(y -> toHour(toDateTime(0) + y) = 0, arr) SETTINGS session_timezone = 'UTC', use_query_condition_cache = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

-- The concrete type of a `Dynamic` value exists only at read time, so the declared type is just
-- `Dynamic` and names no time zone.
SELECT '-- Dynamic subtype';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM tab WHERE toString(d) = '1970-01-01 00:00:00' SETTINGS session_timezone = 'Asia/Tokyo', use_query_condition_cache = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SELECT count() > 0 FROM system.query_condition_cache;
SELECT count() FROM tab WHERE toString(d) = '1970-01-01 00:00:00' SETTINGS session_timezone = 'UTC', use_query_condition_cache = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM tab WHERE toString(d) = '1970-01-01 00:00:00' SETTINGS session_timezone = 'UTC', use_query_condition_cache = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

-- Neither an argument nor a result of this condition is temporal, so no type in it can carry a time
-- zone at all.
SELECT '-- function with no temporal type';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM tab WHERE fromUnixTimestamp(x, '%H') = '00' SETTINGS session_timezone = 'Asia/Tokyo', use_query_condition_cache = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SELECT count() > 0 FROM system.query_condition_cache;
SELECT count() FROM tab WHERE fromUnixTimestamp(x, '%H') = '00' SETTINGS session_timezone = 'UTC', use_query_condition_cache = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM tab WHERE fromUnixTimestamp(x, '%H') = '00' SETTINGS session_timezone = 'UTC', use_query_condition_cache = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

-- A wrapper hides the time zone from the type name: `Nullable(DateTime)` with the zone omitted is
-- spelled the same in every session. This is the shape reported in issue #117186.
SELECT '-- omitted time zone inside a wrapper type';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM tab WHERE toHour(CAST(x, 'Nullable(DateTime)')) = 0 SETTINGS session_timezone = 'Asia/Tokyo', use_query_condition_cache = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SELECT count() > 0 FROM system.query_condition_cache;
SELECT count() FROM tab WHERE toHour(CAST(x, 'Nullable(DateTime)')) = 0 SETTINGS session_timezone = 'UTC', use_query_condition_cache = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM tab WHERE toHour(CAST(x, 'Nullable(DateTime)')) = 0 SETTINGS session_timezone = 'UTC', use_query_condition_cache = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

-- PREWHERE and WHERE populate the cache from different places, so both must be covered. Either
-- setting being 0 disables PREWHERE, so both are pinned.
SELECT '-- without PREWHERE, lambda body';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM tab WHERE arrayExists(y -> toHour(toDateTime(0) + y) = 0, arr) SETTINGS session_timezone = 'Asia/Tokyo', use_query_condition_cache = 1, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;
SELECT count() > 0 FROM system.query_condition_cache;
SELECT count() FROM tab WHERE arrayExists(y -> toHour(toDateTime(0) + y) = 0, arr) SETTINGS session_timezone = 'UTC', use_query_condition_cache = 1, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;

SELECT '-- without PREWHERE, function with no temporal type';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM tab WHERE fromUnixTimestamp(x, '%H') = '00' SETTINGS session_timezone = 'Asia/Tokyo', use_query_condition_cache = 1, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;
SELECT count() > 0 FROM system.query_condition_cache;
SELECT count() FROM tab WHERE fromUnixTimestamp(x, '%H') = '00' SETTINGS session_timezone = 'UTC', use_query_condition_cache = 1, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;

-- Entries for two time zones coexist rather than one evicting the other.
SELECT '-- both time zones cached at once';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM tab WHERE toString(d) = 'no such value' SETTINGS session_timezone = 'Asia/Tokyo', use_query_condition_cache = 1;
SELECT count() FROM tab WHERE toString(d) = 'no such value' SETTINGS session_timezone = 'UTC', use_query_condition_cache = 1;
SELECT count() FROM system.query_condition_cache;

-- The cache must still be used within one session under a non-default time zone: keying on the zone
-- must partition the cache, not disable it. The condition has to drop marks, because a repeated
-- query on a condition that matches everything reads everything again on any build.
SELECT '-- repeated query in one session still hits';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM tab WHERE arrayExists(y -> toHour(toDateTime(0) + y) = 5, arr) FORMAT Null SETTINGS session_timezone = 'Asia/Tokyo', use_query_condition_cache = 1, log_comment = '05054_first';
SELECT count() FROM tab WHERE arrayExists(y -> toHour(toDateTime(0) + y) = 5, arr) FORMAT Null SETTINGS session_timezone = 'Asia/Tokyo', use_query_condition_cache = 1, log_comment = '05054_second';
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, read_rows, ProfileEvents['QueryConditionCacheHits'], ProfileEvents['QueryConditionCacheMisses']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05054_first', '05054_second')
ORDER BY log_comment, event_time_microseconds DESC
LIMIT 1 BY log_comment;

-- Index analysis writes entries of its own, under a different key than the row-level filter, and
-- `use_skip_indexes_on_data_read = 0` is what puts a `set` index's evaluation there. That evaluation
-- runs over the values the index stored, so it follows the session time zone. The warm-up reading no
-- row is what pins its entry on index analysis: the row-level filter has to read a granule to find
-- nothing in it. The repeat in the same zone must hit, or nothing would be there for `UTC` to miss.
SELECT '-- index analysis';
DROP TABLE IF EXISTS tab_skip_index;
CREATE TABLE tab_skip_index (x UInt32, INDEX idx x TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 0, auto_statistics_types = '', index_granularity = 1024;
INSERT INTO tab_skip_index SELECT number FROM numbers(3600);
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM tab_skip_index WHERE fromUnixTimestamp(x, '%H') = '00' SETTINGS session_timezone = 'Asia/Tokyo', use_query_condition_cache = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, log_comment = '05054_index_analysis_warm';
SELECT count() FROM tab_skip_index WHERE fromUnixTimestamp(x, '%H') = '00' SETTINGS session_timezone = 'Asia/Tokyo', use_query_condition_cache = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, log_comment = '05054_index_analysis_reuse';
SELECT count() FROM tab_skip_index WHERE fromUnixTimestamp(x, '%H') = '00' SETTINGS session_timezone = 'UTC', use_query_condition_cache = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM tab_skip_index WHERE fromUnixTimestamp(x, '%H') = '00' SETTINGS session_timezone = 'UTC', use_query_condition_cache = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, read_rows, ProfileEvents['QueryConditionCacheHits']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05054_index_analysis_warm', '05054_index_analysis_reuse')
ORDER BY log_comment, event_time_microseconds DESC
LIMIT 1 BY log_comment;

DROP TABLE tab_skip_index;
DROP TABLE tab;
