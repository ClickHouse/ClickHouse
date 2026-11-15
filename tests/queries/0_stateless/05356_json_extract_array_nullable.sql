-- JSON functions returning a type that can't be inside Nullable (e.g. Array) must accept
-- Nullable inputs and return their non-Nullable result type unchanged.
-- See https://github.com/ClickHouse/ClickHouse/issues/68290

-- The functions from the issue
SELECT JSONExtractKeysAndValuesRaw(toNullable('{"a":"b"}'));
SELECT JSONExtractKeysAndValues(toNullable('{"a":"b"}'), 'String');

-- Other functions returning Array
SELECT JSONExtractKeys(toNullable('{"a":"b"}'));
SELECT JSONExtractArrayRaw(toNullable('[1,"x"]'));
SELECT JSONExtract(toNullable('[1,2]'), 'Array(Int64)');
SELECT JSONExtractKeysAndValuesRawCaseInsensitive(toNullable('{"A":{"b":1}}'), 'a');

-- The result type must remain non-Nullable
SELECT toTypeName(JSONExtractKeysAndValuesRaw(toNullable('{}')));
SELECT toTypeName(JSONExtractKeysAndValues(toNullable('{}'), 'String'));
SELECT toTypeName(JSONExtractKeys(toNullable('{}')));
SELECT toTypeName(JSONExtractArrayRaw(toNullable('[]')));
SELECT toTypeName(JSONExtract(toNullable('[]'), 'Array(Int64)'));

-- Functions whose result type can be inside Nullable still return Nullable
SELECT toTypeName(JSONExtractRaw(toNullable('{}')));
SELECT toTypeName(JSONExtract(toNullable('{}'), 'Tuple(a Int64)'));

-- NULL rows return an empty array
DROP TABLE IF EXISTS t_json_nullable;
CREATE TABLE t_json_nullable (id UInt8, j Nullable(String)) ENGINE = Memory;
INSERT INTO t_json_nullable VALUES (1, '{"a":"b","c":[1,2]}'), (2, NULL), (3, '[1,2]');
SELECT
    id,
    JSONExtractKeysAndValuesRaw(j),
    JSONExtractKeysAndValues(j, 'String'),
    JSONExtractKeys(j),
    JSONExtractArrayRaw(j),
    JSONExtract(j, 'Array(Int64)')
FROM t_json_nullable ORDER BY id;
DROP TABLE t_json_nullable;

-- A Nullable path argument
SELECT JSONExtractKeys('{"a":{"b":1}}', toNullable('a'));
SELECT JSONExtractKeys('{"a":{"b":1}}', x) FROM (SELECT arrayJoin([NULL, 'a']::Array(Nullable(String))) AS x);

-- A NULL row whose nested value is not empty must still return an empty array
SELECT JSONExtractKeys(nullIf(materialize('{"a":1}'), materialize('{"a":1}')));
SELECT JSONExtractKeysAndValuesRaw(nullIf(materialize('{"a":1}'), materialize('{"a":1}')));
SELECT JSONExtractKeysAndValues(nullIf(materialize('{"a":"b"}'), materialize('{"a":"b"}')), 'String');
SELECT JSONExtract(nullIf(materialize('[1,2]'), materialize('[1,2]')), 'Array(Int64)');

-- A NULL key must not be looked up as an empty string, which is a valid key
SELECT JSONExtractKeys('{"":{"x":1}}', k) FROM values('k Nullable(String)', NULL, '');
SELECT JSONExtractArrayRaw('{"":[1]}', k) FROM values('k Nullable(String)', NULL, '');

-- Both the JSON and the key are Nullable
SELECT j, k, JSONExtractKeys(j, k)
FROM values('j Nullable(String), k Nullable(String)', ('{"a":{"b":1}}', 'a'), (NULL, 'a'), ('{"a":{"b":1}}', NULL), (NULL, NULL));

-- LowCardinality cannot be inside Nullable either
SELECT JSONExtract(j, 'LowCardinality(Nullable(String))') AS r, toTypeName(r) FROM values('j Nullable(String)', '"x"', NULL);
SELECT JSONExtract(j, 'LowCardinality(String)') AS r, toTypeName(r) FROM values('j Nullable(String)', '"x"', NULL);
SELECT JSONExtract(j, 'Array(LowCardinality(String))') AS r, toTypeName(r) FROM values('j Nullable(String)', '["x"]', NULL);

-- A NULL constant still returns NULL
SELECT JSONExtractKeys(NULL), toTypeName(JSONExtractKeys(NULL));
