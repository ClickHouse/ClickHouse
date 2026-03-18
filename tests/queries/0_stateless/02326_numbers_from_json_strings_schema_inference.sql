-- Tags: no-fasttest

set input_format_json_try_infer_numbers_from_strings=1;
set input_format_json_read_numbers_as_strings=0;
set allow_experimental_object_type=1;

desc format(JSONEachRow, '{"x" : "123"}');
desc format(JSONEachRow, '{"x" : ["123", 123, 12.3]}');
desc format(JSONEachRow, '{"x" : {"k1" : "123", "k2" : 123}}');
desc format(JSONEachRow, '{"x" : {"k1" : ["123", "123"], "k2" : [123, 123]}}');
desc format(JSONEachRow, '{"x" : "123"}\n{"x" : 123}');
desc format(JSONEachRow, '{"x" : ["123", "456"]}\n{"x" : [123, 456]}');
desc format(JSONEachRow, '{"x" : {"k1" : "123"}}\n{"x" : {"k2" : 123}}');
desc format(JSONEachRow, '{"x" : {"k1" : ["123", "123"]}}\n{"x": {"k2" : [123, 123]}}');
desc format(JSONEachRow, '{"x" : ["123", "Some string"]}');
desc format(JSONEachRow, '{"x" : {"k1" : "123", "k2" : "Some string"}}');
desc format(JSONEachRow, '{"x" : {"k1" : ["123", "123"], "k2" : ["Some string"]}}');
desc format(JSONEachRow, '{"x" : "123"}\n{"x" : "Some string"}');
desc format(JSONEachRow, '{"x" : ["123", "456"]}\n{"x" : ["Some string"]}');
desc format(JSONEachRow, '{"x" : {"k1" : "123"}}\n{"x" : {"k2" : "Some string"}}');
desc format(JSONEachRow, '{"x" : {"k1" : ["123", "123"]}}\n{"x": {"k2" : ["Some string"]}}');
desc format(JSONEachRow, '{"x" : [123, "Some string"]}');
desc format(JSONEachRow, '{"x" : {"a" : 123, "b" : "Some string"}}');
