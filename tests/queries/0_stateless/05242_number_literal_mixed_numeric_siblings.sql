-- A number literal takes the type of its numeric siblings only when that keeps its value exactly,
-- and with several siblings it is typed by their common supertype. The result of a mixed-type call
-- therefore does not depend on the order of the arguments.

SELECT 'float', greatest(toFloat32(number), toFloat64(number), 1.123456789012345678) AS r, toTypeName(r) FROM numbers(1);
SELECT 'float', greatest(toFloat64(number), toFloat32(number), 1.123456789012345678) AS r, toTypeName(r) FROM numbers(1);
SELECT 'float', greatest(toFloat32(number), 1.123456789012345678) AS r, toTypeName(r) FROM numbers(1);
SELECT 'float', greatest(toFloat32(number), toFloat64(number), 1.5) AS r, toTypeName(r) FROM numbers(1);
SELECT 'float', greatest(toFloat64(number), toFloat32(number), 1.5) AS r, toTypeName(r) FROM numbers(1);

-- An exactly representable literal narrows to the sibling type, an inexact one keeps Float64.
SELECT 'narrow', toTypeName(greatest(toFloat32(number), 1.5)), toTypeName(greatest(toFloat32(number), 1.1)) FROM numbers(1);

SELECT 'int', greatest(toInt8(number), toInt64(number), 300) AS r, toTypeName(r) FROM numbers(1);
SELECT 'int', greatest(toInt64(number), toInt8(number), 300) AS r, toTypeName(r) FROM numbers(1);
SELECT 'int', toTypeName(toInt8(number) + 100), toTypeName(toInt8(number) + 1000) FROM numbers(1);

SELECT 'wide', greatest(toInt8(number), toUInt128(number), 100000000000000000000000) AS r, toTypeName(r) FROM numbers(1);
SELECT 'wide', greatest(toUInt128(number), toInt8(number), 100000000000000000000000) AS r, toTypeName(r) FROM numbers(1);
