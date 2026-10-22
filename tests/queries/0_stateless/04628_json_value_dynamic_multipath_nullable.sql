-- Test JSON_VALUE multi-path over Dynamic input with function_json_value_return_type_allow_nullable=1:
-- the per-leaf makeNullable branch in getReturnTypeForDynamic must produce Nullable(String) leaves.
SET function_json_value_return_type_allow_nullable = 1;

SELECT toTypeName(JSON_VALUE('{"a": 1, "b": 2}'::Dynamic, tuple('$.a', '$.b')));
SELECT toTypeName(JSON_VALUE('{"a": 1}'::Dynamic, array('$.a', '$.b')));
SELECT JSON_VALUE('{"a": 1, "b": 2}'::Dynamic, tuple('$.a', '$.missing'));
SELECT JSON_VALUE('{"a": 1}'::Dynamic, array('$.a', '$.missing'));
SELECT toTypeName(JSON_VALUE('{"a": 1, "b": 2}'::Dynamic, tuple('$.a', toLowCardinality('$.b'))));
