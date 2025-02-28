SELECT randomFixedString('string'); -- { serverError 43 }
SELECT randomFixedString(0); -- { serverError 69 }
SELECT randomFixedString(rand() % 10); -- { serverError 44 }
SELECT toTypeName(randomFixedString(10));
SELECT DISTINCT c > 30000 FROM (SELECT arrayJoin(arrayMap(x -> reinterpretAsUInt8(substring(randomFixedString(100), x + 1, 1)), range(100))) AS byte, count() AS c FROM numbers(100000) GROUP BY byte ORDER BY byte);
