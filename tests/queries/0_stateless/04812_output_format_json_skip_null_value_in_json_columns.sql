SET enable_json_type = 1;

SELECT 'serialization functions, disabled';
WITH '{"a":{"b":null,"c":1},"d":null,"e":2}'::JSON(a.b Nullable(UInt32), a.c UInt32, d Nullable(String), e UInt32) AS jsn
SELECT toString(jsn), jsn::String, toJSONString(jsn)
SETTINGS output_format_json_skip_null_value_in_json_columns = 0;

SELECT 'serialization functions, enabled';
WITH '{"a":{"b":null,"c":1},"d":null,"e":2}'::JSON(a.b Nullable(UInt32), a.c UInt32, d Nullable(String), e UInt32) AS jsn
SELECT toString(jsn), jsn::String, toJSONString(jsn)
SETTINGS output_format_json_skip_null_value_in_json_columns = 1;

SELECT 'all null and missing typed paths';
SELECT toString(jsn)
FROM values('jsn JSON(a Nullable(UInt32), b Nullable(String))', ('{"a":null}'), ('{}'))
SETTINGS output_format_json_skip_null_value_in_json_columns = 1;

SELECT 'typed Dynamic path';
SELECT toString('{"a":null,"b":1}'::JSON(a Dynamic, b UInt32))
SETTINGS output_format_json_skip_null_value_in_json_columns = 1;

SELECT 'nested JSON and named Tuple';
WITH '{"n":{"x":null,"y":1},"t":{"x":null,"y":2}}'::JSON(
    n JSON(x Nullable(UInt32), y UInt32),
    t Tuple(x Nullable(UInt32), y UInt32)) AS jsn
SELECT toString(jsn)
SETTINGS
    output_format_json_skip_null_value_in_json_columns = 1,
    output_format_json_named_tuples_as_objects = 1;

SELECT '{"a":{"b":null,"c":1},"d":null,"e":2}'::JSON(a.b Nullable(UInt32), a.c UInt32, d Nullable(String), e UInt32) AS jsn
FORMAT JSONEachRow
SETTINGS output_format_json_skip_null_value_in_json_columns = 1;
