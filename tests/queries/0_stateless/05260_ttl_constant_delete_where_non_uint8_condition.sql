-- A constant `TTL ... DELETE WHERE` whose condition is not a plain `UInt8` column: an INSERT records the TTL
-- for a part only if one of its rows matches the condition, and rejects a condition that cannot be a filter.

SET allow_suspicious_ttl_expressions = 1;
SET session_timezone = 'UTC';
SET async_insert = 0;

DROP TABLE IF EXISTS t_ttl_where_nullable;
DROP TABLE IF EXISTS t_ttl_where_array;

CREATE TABLE t_ttl_where_nullable (x Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple()
TTL toDateTime('2100-01-01 00:00:00', 'UTC') DELETE WHERE x;

SYSTEM STOP MERGES t_ttl_where_nullable;

-- One row per INSERT, so each INSERT is exactly one part.
INSERT INTO t_ttl_where_nullable VALUES (0);
INSERT INTO t_ttl_where_nullable VALUES (NULL);
INSERT INTO t_ttl_where_nullable VALUES (1);

SELECT rows_where_ttl_info.min, rows_where_ttl_info.max
FROM system.parts
WHERE database = currentDatabase() AND table = 't_ttl_where_nullable' AND active
ORDER BY name;

CREATE TABLE t_ttl_where_array (a Array(UInt8)) ENGINE = MergeTree ORDER BY tuple()
TTL toDateTime('2100-01-01 00:00:00', 'UTC') DELETE WHERE a;

INSERT INTO t_ttl_where_array VALUES ([1]); -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_ttl_where_nullable;
DROP TABLE t_ttl_where_array;
