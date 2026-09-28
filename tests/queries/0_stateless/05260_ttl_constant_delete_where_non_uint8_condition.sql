-- A constant `TTL ... DELETE WHERE` whose condition is not a plain `UInt8` column: an INSERT records the TTL
-- for a part only if one of its rows matches the condition, and rejects a condition that cannot be a filter.

SET allow_suspicious_ttl_expressions = 1;
SET session_timezone = 'UTC';
SET async_insert = 0;

DROP TABLE IF EXISTS t_bool;
DROP TABLE IF EXISTS t_int8;
DROP TABLE IF EXISTS t_int32;
DROP TABLE IF EXISTS t_nullable_bool;
DROP TABLE IF EXISTS t_nullable_int8;
DROP TABLE IF EXISTS t_nullable_uint8;
DROP TABLE IF EXISTS t_array;

-- One row per INSERT, so each INSERT is exactly one part.

CREATE TABLE t_bool (x Bool) ENGINE = MergeTree ORDER BY tuple()
TTL toDateTime('2100-01-01 00:00:00', 'UTC') DELETE WHERE x;
SYSTEM STOP MERGES t_bool;
INSERT INTO t_bool VALUES (false);
INSERT INTO t_bool VALUES (true);

CREATE TABLE t_int8 (x Int8) ENGINE = MergeTree ORDER BY tuple()
TTL toDateTime('2100-01-01 00:00:00', 'UTC') DELETE WHERE x;
SYSTEM STOP MERGES t_int8;
INSERT INTO t_int8 VALUES (0);
INSERT INTO t_int8 VALUES (-1);

CREATE TABLE t_int32 (x Int32) ENGINE = MergeTree ORDER BY tuple()
TTL toDateTime('2100-01-01 00:00:00', 'UTC') DELETE WHERE x;
SYSTEM STOP MERGES t_int32;
INSERT INTO t_int32 VALUES (0);
INSERT INTO t_int32 VALUES (256);

CREATE TABLE t_nullable_bool (x Nullable(Bool)) ENGINE = MergeTree ORDER BY tuple()
TTL toDateTime('2100-01-01 00:00:00', 'UTC') DELETE WHERE x;
SYSTEM STOP MERGES t_nullable_bool;
INSERT INTO t_nullable_bool VALUES (NULL);
INSERT INTO t_nullable_bool VALUES (false);
INSERT INTO t_nullable_bool VALUES (true);

CREATE TABLE t_nullable_int8 (x Nullable(Int8)) ENGINE = MergeTree ORDER BY tuple()
TTL toDateTime('2100-01-01 00:00:00', 'UTC') DELETE WHERE x;
SYSTEM STOP MERGES t_nullable_int8;
INSERT INTO t_nullable_int8 VALUES (NULL);
INSERT INTO t_nullable_int8 VALUES (0);
INSERT INTO t_nullable_int8 VALUES (-1);

CREATE TABLE t_nullable_uint8 (x Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple()
TTL toDateTime('2100-01-01 00:00:00', 'UTC') DELETE WHERE x;
SYSTEM STOP MERGES t_nullable_uint8;
INSERT INTO t_nullable_uint8 VALUES (NULL);
INSERT INTO t_nullable_uint8 VALUES (0);
INSERT INTO t_nullable_uint8 VALUES (1);

SELECT table, rows_where_ttl_info.min
FROM system.parts
WHERE database = currentDatabase() AND active
ORDER BY table, name;

CREATE TABLE t_array (a Array(UInt8)) ENGINE = MergeTree ORDER BY tuple()
TTL toDateTime('2100-01-01 00:00:00', 'UTC') DELETE WHERE a;

INSERT INTO t_array VALUES ([1]); -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_bool;
DROP TABLE t_int8;
DROP TABLE t_int32;
DROP TABLE t_nullable_bool;
DROP TABLE t_nullable_int8;
DROP TABLE t_nullable_uint8;
DROP TABLE t_array;
