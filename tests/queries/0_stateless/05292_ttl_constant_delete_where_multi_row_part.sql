-- A constant `TTL ... DELETE WHERE` on a part with several rows: the TTL is recorded when any row of
-- the part matches, not only the first one, so the expired TTL deletes exactly the matching rows.

SET allow_suspicious_ttl_expressions = 1;
SET session_timezone = 'UTC';
SET async_insert = 0;

DROP TABLE IF EXISTS t_ttl_const_where;
DROP TABLE IF EXISTS t_ttl_const_where_nullable;

CREATE TABLE t_ttl_const_where (x Int32) ENGINE = MergeTree ORDER BY tuple()
TTL toDateTime('2000-01-01 00:00:00', 'UTC') DELETE WHERE x > 10;

CREATE TABLE t_ttl_const_where_nullable (x Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple()
TTL toDateTime('2000-01-01 00:00:00', 'UTC') DELETE WHERE x > 10;

SYSTEM STOP MERGES t_ttl_const_where;
SYSTEM STOP MERGES t_ttl_const_where_nullable;

INSERT INTO t_ttl_const_where VALUES (1), (5), (20), (7);
INSERT INTO t_ttl_const_where_nullable VALUES (NULL), (5), (20), (NULL);

SELECT table, rows, rows_where_ttl_info.min
FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_ttl_const_where', 't_ttl_const_where_nullable') AND active
ORDER BY table, name;

SYSTEM START MERGES t_ttl_const_where;
SYSTEM START MERGES t_ttl_const_where_nullable;
OPTIMIZE TABLE t_ttl_const_where FINAL;
OPTIMIZE TABLE t_ttl_const_where_nullable FINAL;

SELECT 't_ttl_const_where', x FROM t_ttl_const_where ORDER BY x;
SELECT 't_ttl_const_where_nullable', x FROM t_ttl_const_where_nullable ORDER BY x;

DROP TABLE t_ttl_const_where;
DROP TABLE t_ttl_const_where_nullable;
