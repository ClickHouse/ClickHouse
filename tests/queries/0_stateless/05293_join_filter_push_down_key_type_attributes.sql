-- A `WHERE` predicate on a `JOIN` key is also pushed down to the other side of the `JOIN`, with that side's
-- key put in its place. `IDataType::equals` treats `DateTime` types with different time zones, and `Bool`
-- and `UInt8`, as the same type, but functions over them return different results, so such a key must not
-- stand in for the other one. Every result below is the one with `query_plan_filter_push_down = 0`.
--
-- All `DateTime` values are the same instant, 2024-01-01 00:00:00 UTC, which is 09:00 in Tokyo.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;

DROP TABLE IF EXISTS t_utc;
DROP TABLE IF EXISTS t_utc_2;
DROP TABLE IF EXISTS t_tokyo;
DROP TABLE IF EXISTS t_utc_nullable;
DROP TABLE IF EXISTS t_tokyo_nullable;
DROP TABLE IF EXISTS t_utc64;
DROP TABLE IF EXISTS t_tokyo64;
DROP TABLE IF EXISTS t_uint8;
DROP TABLE IF EXISTS t_bool;
DROP TABLE IF EXISTS t_bool_nullable;

CREATE TABLE t_utc (k DateTime('UTC')) ENGINE = Memory;
CREATE TABLE t_tokyo (k DateTime('Asia/Tokyo')) ENGINE = Memory;
CREATE TABLE t_utc_nullable (k Nullable(DateTime('UTC'))) ENGINE = Memory;
CREATE TABLE t_tokyo_nullable (k Nullable(DateTime('Asia/Tokyo'))) ENGINE = Memory;
CREATE TABLE t_utc64 (k DateTime64(3, 'UTC')) ENGINE = Memory;
CREATE TABLE t_tokyo64 (k DateTime64(3, 'Asia/Tokyo')) ENGINE = Memory;
CREATE TABLE t_uint8 (k UInt8) ENGINE = Memory;
CREATE TABLE t_bool (k Bool) ENGINE = Memory;
CREATE TABLE t_bool_nullable (k Nullable(Bool)) ENGINE = Memory;

INSERT INTO t_utc VALUES ('2024-01-01 00:00:00');
INSERT INTO t_tokyo SELECT k FROM t_utc;
INSERT INTO t_utc_nullable SELECT k FROM t_utc;
INSERT INTO t_tokyo_nullable SELECT k FROM t_utc;
INSERT INTO t_utc64 SELECT k FROM t_utc;
INSERT INTO t_tokyo64 SELECT k FROM t_utc;
INSERT INTO t_uint8 VALUES (1);
INSERT INTO t_bool VALUES (true);
INSERT INTO t_bool_nullable VALUES (true);

SELECT 'DateTime keys in different time zones';
SELECT count() FROM t_utc AS l INNER JOIN t_tokyo AS r ON l.k = r.k WHERE toHour(r.k) = 9;
SELECT count() FROM t_utc AS l INNER JOIN t_tokyo AS r ON l.k = r.k WHERE toHour(l.k) = 0;
SELECT count() FROM t_tokyo AS l INNER JOIN t_utc AS r ON l.k = r.k WHERE toDate(r.k) = '2024-01-01' AND toHour(l.k) = 9;
-- A string literal compared with a `DateTime` is read in the time zone of that `DateTime`.
SELECT count() FROM t_utc AS l INNER JOIN t_tokyo AS r ON l.k = r.k WHERE r.k >= '2024-01-01 09:00:00' AND r.k < '2024-01-01 10:00:00';
SELECT r.k, toTypeName(r.k) FROM t_utc AS l LEFT JOIN t_tokyo AS r ON l.k = r.k WHERE toHour(l.k) = 0;
SELECT l.k, toTypeName(l.k) FROM t_utc AS l RIGHT JOIN t_tokyo AS r ON l.k = r.k WHERE toHour(r.k) = 9;

SELECT 'DateTime64 keys in different time zones';
SELECT count() FROM t_utc64 AS l INNER JOIN t_tokyo64 AS r ON l.k = r.k WHERE toHour(r.k) = 9;
SELECT count() FROM t_utc64 AS l INNER JOIN t_tokyo64 AS r ON l.k = r.k WHERE toHour(l.k) = 0;

SELECT 'Bool and UInt8 keys';
SELECT count() FROM t_uint8 AS l INNER JOIN t_bool AS r ON l.k = r.k WHERE toString(r.k) = 'true';
SELECT count() FROM t_bool AS l INNER JOIN t_uint8 AS r ON l.k = r.k WHERE toString(r.k) = '1';

-- The keys below have different types, so the other side's key is cast to their least supertype, which
-- takes the time zone or the name of the left key. That cast must not stand in for a right key without it.
SELECT 'Cross-type keys whose supertype differs from a key only in the time zone or the name';
SELECT count() FROM t_tokyo AS l INNER JOIN t_utc_nullable AS r ON l.k = r.k WHERE toHour(r.k) = 0;
SELECT count() FROM t_utc AS l INNER JOIN t_tokyo_nullable AS r ON l.k = r.k WHERE toHour(r.k) = 9;
SELECT count() FROM t_utc_nullable AS l INNER JOIN t_tokyo AS r ON l.k = r.k WHERE toHour(l.k) = 0;
SELECT count() FROM t_uint8 AS l INNER JOIN t_bool_nullable AS r ON l.k = r.k WHERE toString(r.k) = 'true';

-- Keys of identical types are still substituted, so the predicate on `r.k` filters both tables, while
-- keys that differ in the time zone leave it on the right table only.
SELECT 'Number of tables the predicate filters: identical key types, then different time zones';
CREATE TABLE t_utc_2 (k DateTime('UTC')) ENGINE = Memory;
INSERT INTO t_utc_2 SELECT k FROM t_utc;
SELECT count() FROM t_utc AS l INNER JOIN t_utc_2 AS r ON l.k = r.k WHERE toHour(r.k) = 0;
SELECT countIf(explain LIKE '%Filter column: toHour(k) = 0%') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_utc AS l INNER JOIN t_utc_2 AS r ON l.k = r.k WHERE toHour(r.k) = 0
    SETTINGS query_plan_join_swap_table = 'false', enable_join_runtime_filters = 0
);
SELECT countIf(explain LIKE '%Filter column: toHour(k) = 9%') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_utc AS l INNER JOIN t_tokyo AS r ON l.k = r.k WHERE toHour(r.k) = 9
    SETTINGS query_plan_join_swap_table = 'false', enable_join_runtime_filters = 0
);

DROP TABLE t_utc;
DROP TABLE t_utc_2;
DROP TABLE t_tokyo;
DROP TABLE t_utc_nullable;
DROP TABLE t_tokyo_nullable;
DROP TABLE t_utc64;
DROP TABLE t_tokyo64;
DROP TABLE t_uint8;
DROP TABLE t_bool;
DROP TABLE t_bool_nullable;
