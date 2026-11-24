-- Common type of Bool and UInt8 must not depend on argument order
SELECT toTypeName(x) FROM (SELECT true AS x UNION ALL SELECT toUInt8(2) AS x) LIMIT 1;
SELECT toTypeName(x) FROM (SELECT toUInt8(2) AS x UNION ALL SELECT true AS x) LIMIT 1;
SELECT toTypeName(if(number = 0, true, toUInt8(2))) FROM numbers(1);
SELECT toTypeName(if(number = 0, toUInt8(2), true)) FROM numbers(1);
SELECT toTypeName(x), x FROM (SELECT true AS x UNION ALL SELECT toUInt8(2) AS x) ORDER BY x;
