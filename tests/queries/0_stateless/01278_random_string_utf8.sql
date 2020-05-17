SELECT randomStringUTF8('string'); -- { serverError 43 }
SELECT randomStringUTF8(-10); -- { serverError 43 }
SELECT lengthUTF8(randomStringUTF8(100));
SELECT toTypeName(randomStringUTF8(10));
SELECT isValidUTF8(randomStringUTF8(100000));
SELECT randomStringUTF8(0);
-- SELECT DISTINCT c > 30000 FROM (SELECT arrayJoin(arrayMap(x -> reinterpretAsUInt8(substring(randomStringUTF8(100), x + 1, 1)), range(100))) AS byte, count() AS c FROM numbers(100000) GROUP BY byte ORDER BY byte);

