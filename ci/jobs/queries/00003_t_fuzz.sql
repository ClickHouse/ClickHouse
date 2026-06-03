-- FIXME
-- Tags: xfail
DROP TABLE IF EXISTS tj;
DROP TABLE IF EXISTS t__fuzz_0;
CREATE TABLE tj (`key2` UInt64, `key1` Int64, `a` UInt64, `b` UInt64, `x` UInt64, `y` UInt64) ENGINE = Join(`ALL`, RIGHT, key1, key2);
CREATE TABLE t__fuzz_0 (`key2` UInt64, `key1` Nullable(Int64), `b` Date, `x` Nullable(UInt64), `val` UInt64) ENGINE = Memory;
INSERT INTO t__fuzz_0 FORMAT Values (1, -1, 11, 111, 1111), (2, -2, 22, 222, 2222), (3, -3, 33, 333, 2222), (4, -4, 44, 444, 4444), (5, -5, 55, 555, 5555);
SELECT tj.x FROM t__fuzz_0 ALL RIGHT JOIN tj USING (key1, key2) QUALIFY equals(key1, and(and(1, isZeroOrNull(65535)), equals(and(not(assumeNotNull(materialize(31))), materialize(1) * 1, 65535), and(not(65535), 1)))) ORDER BY key1 ASC NULLS LAST FORMAT TSVWithNames;
DROP TABLE tj;
DROP TABLE t__fuzz_0;
