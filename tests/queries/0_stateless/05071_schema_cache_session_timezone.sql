-- An inferred `DateTime` without an explicit time zone latches the effective time zone of the session
-- that inferred it, and the schema inference cache stores the type objects themselves. A session with
-- another `session_timezone` must not be served that schema: it would parse the values in its own zone
-- but format and compute them in the inferring session's zone.

INSERT INTO FUNCTION file('05071_schema_cache_session_timezone.csv', CSV, 'c1 String')
SELECT arrayJoin(['2020-01-01 00:00:00', '2020-06-01 12:00:00'])
SETTINGS engine_file_truncate_on_insert = 1;

-- Infers the schema first, so the cache entry is latched to this zone.
SELECT toString(c1), toUnixTimestamp(c1), toHour(c1) FROM file('05071_schema_cache_session_timezone.csv') ORDER BY c1
SETTINGS session_timezone = 'Asia/Tokyo';

-- Another zone: must agree with the same query with the cache switched off.
SELECT toString(c1), toUnixTimestamp(c1), toHour(c1) FROM file('05071_schema_cache_session_timezone.csv') ORDER BY c1
SETTINGS session_timezone = 'Europe/Berlin';

SELECT toString(c1), toUnixTimestamp(c1), toHour(c1) FROM file('05071_schema_cache_session_timezone.csv') ORDER BY c1
SETTINGS session_timezone = 'Europe/Berlin', schema_inference_use_cache_for_file = 0;

-- And the first zone is still served its own schema.
SELECT toString(c1), toUnixTimestamp(c1), toHour(c1) FROM file('05071_schema_cache_session_timezone.csv') ORDER BY c1
SETTINGS session_timezone = 'Asia/Tokyo';
