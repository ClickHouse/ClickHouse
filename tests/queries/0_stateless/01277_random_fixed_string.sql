SELECT randomFixedString('string'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT randomFixedString(0); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT randomFixedString(rand() % 10); -- { serverError ILLEGAL_COLUMN }
SELECT toTypeName(randomFixedString(10));
SELECT DISTINCT c > 30000 FROM (SELECT arrayJoin(reinterpret(randomFixedString(100), 'Array(UInt8)')) AS byte, count() AS c FROM numbers(100000) GROUP BY byte ORDER BY byte);
