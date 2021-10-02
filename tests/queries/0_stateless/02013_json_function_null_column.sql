-- Tags: no-fasttest

SELECT JSONExtract('{"string_value":null}', 'string_value', 'Nullable(String)') as x, toTypeName(x);
SELECT JSONExtract('{"string_value":null}', 'string_value', 'String') as x, toTypeName(x);
SELECT JSONExtract(toNullable('{"string_value":null}'), 'string_value', 'Nullable(String)') as x, toTypeName(x);
SELECT JSONExtract(toNullable('{"string_value":null}'), 'string_value', 'String') as x, toTypeName(x);
SELECT JSONExtract(NULL, 'string_value', 'Nullable(String)') as x, toTypeName(x);
SELECT JSONExtract(NULL, 'string_value', 'String') as x, toTypeName(x);
SELECT JSONExtractString('["a", "b", "c", "d", "e"]', idx) FROM (SELECT arrayJoin([2, NULL, 2147483646, 65535, 65535, 3]) AS idx);
