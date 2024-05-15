SET input_format_json_infer_variant_from_multi_type_array=1;
SELECT arr, toTypeName(arr) FROM format('JSONEachRow', '{"arr" : [1, "Hello", {"a" : 32}]}');
SELECT x, toTypeName(x) FROM format('JSONEachRow', '{"x" : 42}, {"x" : "Hello"}');
SELECT x, toTypeName(x) FROM format('JSONEachRow', '{"x" : [1, 2, 3]}, {"x" : {"a" : 42}}');
