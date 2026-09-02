SELECT toTypeName(sum(n)) FROM (SELECT toUInt16(number) AS n FROM system.numbers LIMIT 100);
SELECT toTypeName(sumWithOverflow(n)) FROM (SELECT toUInt16(number) AS n FROM system.numbers LIMIT 100);
SELECT toTypeName(sum(n)) FROM (SELECT toFloat32(number) AS n FROM system.numbers LIMIT 100);
SELECT toTypeName(sumWithOverflow(n)) FROM (SELECT toFloat32(number) AS n FROM system.numbers LIMIT 100);

SELECT sum(n) FROM (SELECT toUInt16(number) AS n FROM system.numbers LIMIT 100);
SELECT sumWithOverflow(n) FROM (SELECT toUInt16(number) AS n FROM system.numbers LIMIT 100);
