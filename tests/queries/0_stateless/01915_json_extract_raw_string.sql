-- Tags: no-fasttest

select JSONExtract('{"a": "123", "b": 456, "c": [7, 8, 9]}', 'Tuple(a String, b String, c String)');

with '{"string_value":null}' as json select JSONExtract(json, 'string_value', 'Nullable(String)');

select JSONExtractString('{"a": 123}', 'a');
select JSONExtractString('{"a": "123"}', 'a');
select JSONExtractString('{"a": null}', 'a');
