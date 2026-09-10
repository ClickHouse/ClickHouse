-- Tags: no-parallel
-- no-parallel: the query condition cache is server-wide and this test drops it.

-- A type that spells no time zone out, such as the `DateTime` inside `Nullable(DateTime)`, means the
-- session's zone, and the query condition cache keyed a granule verdict by the type's name, which
-- carries none. A session that primed the cache under one `session_timezone` therefore served its
-- "no matching rows" verdict to a session in another zone, where the same filter matches every row.

DROP TABLE IF EXISTS t_qcc_session_timezone;
CREATE TABLE t_qcc_session_timezone (x UInt32) ENGINE = MergeTree ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 0, auto_statistics_types = '';
INSERT INTO t_qcc_session_timezone SELECT 0 FROM numbers(100000);

SYSTEM DROP QUERY CONDITION CACHE;

-- The epoch is 09:00 in Tokyo and 00:00 in UTC, so the filter matches nothing there and everything here.
SELECT 'nothing matches in Tokyo', count() FROM t_qcc_session_timezone
WHERE toHour(CAST(x, 'Nullable(DateTime)')) = 0
SETTINGS use_query_condition_cache = 1, session_timezone = 'Asia/Tokyo';

SELECT 'and everything in UTC', count() FROM t_qcc_session_timezone
WHERE toHour(CAST(x, 'Nullable(DateTime)')) = 0
SETTINGS use_query_condition_cache = 1, session_timezone = 'UTC';

SELECT 'the same, the other way round';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_session_timezone WHERE toHour(CAST(x, 'Nullable(DateTime)')) = 0
SETTINGS use_query_condition_cache = 1, session_timezone = 'UTC';
SELECT count() FROM t_qcc_session_timezone WHERE toHour(CAST(x, 'Nullable(DateTime)')) = 0
SETTINGS use_query_condition_cache = 1, session_timezone = 'Asia/Tokyo';

SELECT 'a zone spelled out in the type is unaffected';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_session_timezone WHERE toHour(CAST(x, 'Nullable(DateTime(\'Asia/Tokyo\'))')) = 0
SETTINGS use_query_condition_cache = 1, session_timezone = 'UTC';
SELECT count() FROM t_qcc_session_timezone WHERE toHour(CAST(x, 'Nullable(DateTime(\'UTC\'))')) = 0
SETTINGS use_query_condition_cache = 1, session_timezone = 'Asia/Tokyo';

SELECT 'and two sessions in one zone still share the entry';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_session_timezone WHERE toHour(CAST(x, 'Nullable(DateTime)')) = 0
SETTINGS use_query_condition_cache = 1, session_timezone = 'Asia/Tokyo';
SELECT 'entries', count() FROM system.query_condition_cache;
SELECT count() FROM t_qcc_session_timezone WHERE toHour(CAST(x, 'Nullable(DateTime)')) = 0
SETTINGS use_query_condition_cache = 1, session_timezone = 'Asia/Tokyo';
SELECT 'entries after the same query again', count() FROM system.query_condition_cache;

DROP TABLE t_qcc_session_timezone;
