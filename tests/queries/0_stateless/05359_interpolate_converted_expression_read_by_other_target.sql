-- An INTERPOLATE expression converted to its target's type must not replace the expression it converts for the other INTERPOLATE items.
-- Every gap has one filled row, so each filled row is computed from a source row.

SELECT 'swap of targets of different types';
SELECT g, a, b FROM (SELECT toUInt16(number * 2) AS g, toUInt8(number + 1) AS a, toUInt32(number * 1000 + 300) AS b FROM numbers(3))
ORDER BY g WITH FILL INTERPOLATE (a AS b, b AS a);

SELECT 'swap with a Nullable target';
SELECT g, a, b FROM (SELECT toUInt16(number * 2) AS g, toNullable(toUInt32(number + 1)) AS a, toUInt32(number * 1000 + 300) AS b FROM numbers(3))
ORDER BY g WITH FILL INTERPOLATE (a AS b, b AS a);

SELECT 'swap with a String target';
SELECT g, a, b FROM (SELECT toUInt16(number * 2) AS g, toString(number + 1) AS a, toUInt32(number * 1000 + 300) AS b FROM numbers(3))
ORDER BY g WITH FILL INTERPOLATE (b AS a, a AS b);

SELECT 'a column converted for an earlier target';
SELECT g, a, b, c FROM (SELECT toUInt16(number * 2) AS g, toUInt8(number + 1) AS a, toUInt32(number * 1000 + 300) AS b, toUInt32(7) AS c FROM numbers(3))
ORDER BY g WITH FILL INTERPOLATE (a AS b, c AS b + 1);

SELECT 'a literal converted for an earlier target';
SELECT g, s, y FROM (SELECT toUInt16(number * 2) AS g, toString(number) AS s, toUInt64(number * 10) AS y FROM numbers(3))
ORDER BY g WITH FILL INTERPOLATE (s AS 1, y AS y + 1);
