SELECT '...const maps...';

WITH map(1, 2, 3, 4) AS m SELECT m[number] FROM numbers(5);
WITH map('1', 2, '3', 4) AS m SELECT m[toString(number)] FROM numbers(5);

WITH map(1, 2, 3, 4) AS m SELECT m[3];
WITH map('1', 2, '3', 4) AS m SELECT m['3'];

DROP TABLE IF EXISTS t_map_02014;

CREATE TABLE t_map_02014(i1 UInt64, i2 Int32, m1 Map(UInt32, String), m2 Map(Int8, String), m3 Map(Int128, String)) ENGINE = Memory;
INSERT INTO t_map_02014 VALUES (1, -1, map(1, 'foo', 2, 'bar'), map(-1, 'foo', 1, 'bar'), map(-1, 'foo', 1, 'bar'));

SELECT '...int keys...';

SELECT m1[i1], m2[i1], m3[i1] FROM t_map_02014;
SELECT m1[i2], m2[i2], m3[i2] FROM t_map_02014;

DROP TABLE IF EXISTS t_map_02014;

CREATE TABLE t_map_02014(s String, fs FixedString(3), m1 Map(String, String), m2 Map(FixedString(3), String)) ENGINE = Memory;
INSERT INTO t_map_02014 VALUES ('aaa', 'bbb', map('aaa', 'foo', 'bbb', 'bar'), map('aaa', 'foo', 'bbb', 'bar'));

SELECT '...string keys...';

SELECT m1['aaa'], m2['aaa'] FROM t_map_02014;
SELECT m1['aaa'::FixedString(3)], m2['aaa'::FixedString(3)] FROM t_map_02014;
SELECT m1[s], m2[s] FROM t_map_02014;
SELECT m1[fs], m2[fs] FROM t_map_02014;
SELECT length(m2['aaa'::FixedString(4)]) FROM t_map_02014;

DROP TABLE IF EXISTS t_map_02014;
