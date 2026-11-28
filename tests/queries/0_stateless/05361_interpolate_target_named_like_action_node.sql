DROP TABLE IF EXISTS t_interpolate_named_like_expression;
-- `-x` without an alias makes a column named `negate(x)`
CREATE TABLE t_interpolate_named_like_expression ENGINE = Memory AS SELECT toUInt16(number * 3) AS g, toInt64(number + 1) AS x, -x FROM numbers(2);
SELECT g, x, `negate(x)` FROM t_interpolate_named_like_expression ORDER BY g WITH FILL INTERPOLATE (x AS -x, `negate(x)` AS `negate(x)` - 1);
DROP TABLE t_interpolate_named_like_expression;

SELECT g, y, `2_UInt8` FROM (SELECT toUInt16(number * 2) AS g, toUInt64(number * 10) AS y, toUInt64(number * 100 + 5) AS `2_UInt8` FROM numbers(3))
ORDER BY g WITH FILL INTERPOLATE (y AS y + 2, `2_UInt8` AS `2_UInt8` + 1);
SELECT g, y, `2_UInt8` FROM (SELECT toUInt16(number * 2) AS g, toUInt64(number * 10) AS y, toUInt64(number * 100 + 5) AS `2_UInt8` FROM numbers(3))
ORDER BY g WITH FILL INTERPOLATE (`2_UInt8` AS `2_UInt8` + 1, y AS y + 2);
SELECT g, `2_UInt8` FROM (SELECT toUInt16(number * 2) AS g, toUInt64(number * 100 + 5) AS `2_UInt8` FROM numbers(3))
ORDER BY g WITH FILL INTERPOLATE (`2_UInt8` AS `2_UInt8` + 2);

SELECT g, x, `negate(__INTERPOLATE.x)` FROM (SELECT toUInt16(number * 3) AS g, toInt64(number + 1) AS x, toInt64(100 + number) AS `negate(__INTERPOLATE.x)` FROM numbers(2))
ORDER BY g WITH FILL INTERPOLATE (x AS -x, `negate(__INTERPOLATE.x)` AS `negate(__INTERPOLATE.x)` - 1);
SELECT g, y, z, `plus(__table1.z, 1_UInt8)` FROM (SELECT toUInt16(number * 2) AS g, toUInt64(number * 10) AS y, toUInt64(number) AS z, toUInt64(number * 100 + 5) AS `plus(__table1.z, 1_UInt8)` FROM numbers(3))
ORDER BY g WITH FILL INTERPOLATE (y AS z + 1, `plus(__table1.z, 1_UInt8)` AS `plus(__table1.z, 1_UInt8)` + 2);
