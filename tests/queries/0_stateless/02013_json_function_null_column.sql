
SELECT JSONExtract('{"string_value":null}', 'string_value', 'Nullable(String)') as x, toTypeName(x);
SELECT JSONExtract('{"string_value":null}', 'string_value', 'LowCardinality(Nullable(String))') as x, toTypeName(x);
SELECT JSONExtract('{"string_value":null}', 'string_value', 'String') as x, toTypeName(x);
SELECT JSONExtract(toNullable('{"string_value":null}'), 'string_value', 'Nullable(String)') as x, toTypeName(x);
SELECT JSONExtract(toNullable('{"string_value":null}'), 'string_value', 'LowCardinality(Nullable(String))') as x, toTypeName(x);  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONExtract(toNullable('{"string_value":null}'), 'string_value', 'String') as x, toTypeName(x);
SELECT JSONExtract(NULL, 'string_value', 'Nullable(String)') as x, toTypeName(x);
SELECT JSONExtract(NULL, 'string_value', 'LowCardinality(Nullable(String))') as x, toTypeName(x);
SELECT JSONExtract(NULL, 'string_value', 'String') as x, toTypeName(x);
SELECT JSONExtractString('["a", "b", "c", "d", "e"]', idx) FROM (SELECT arrayJoin([2, NULL, 2147483646, 65535, 65535, 3]) AS idx);

SELECT JSONExtractInt('[1]', toNullable(1));
SELECT JSONExtractBool('[1]', toNullable(1));
SELECT JSONExtractFloat('[1]', toNullable(1));
SELECT JSONExtractString('["a"]', toNullable(1));
SELECT JSONExtractInt('[1]', toLowCardinality(toNullable(1))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT JSONExtractArrayRaw('["1"]', toNullable(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONExtractKeysAndValuesRaw('["1"]', toNullable(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSONExtractKeysAndValues('["1"]', toNullable(1)); -- { serverError ILLEGAL_COLUMN }

SELECT JSONExtract('[1]', toNullable(1), 'Nullable(Int)');
SELECT JSONExtract('[1]', toNullable(1), 'Nullable(UInt8)');
SELECT JSONExtract('[1]', toNullable(1), 'Nullable(Bool)');
SELECT JSONExtract('[1]', toNullable(1), 'Nullable(Float)');
SELECT JSONExtract('["a"]', toNullable(1), 'Nullable(String)');
SELECT JSONExtract('["a"]', toNullable(1), 'Nullable(Int)');
SELECT JSONExtract('["-a"]', toNullable(1), 'Nullable(Int)');

SELECT JSONExtract(materialize('{"key":"value"}'), 'Tuple(key LowCardinality(Nullable(String)))');
SELECT JSONExtract(materialize('{"key":null}'), 'Tuple(key LowCardinality(Nullable(String)))');
SELECT JSONExtract(materialize('{"not_a_key":"value"}'), 'Tuple(key LowCardinality(Nullable(String)))');
