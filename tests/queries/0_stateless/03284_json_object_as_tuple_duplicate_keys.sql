select * from format(JSONEachRow, 'a Tuple(b UInt32)', '{"a" : {"b" : 1, "b" : 2}}'); -- {serverError INCORRECT_DATA}
select * from format(JSONEachRow, '{"a" : {"b" : 1, "b" : 2}}'); -- {serverError INCORRECT_DATA}
select * from format(JSONEachRow, '{"a" : {"b" : 1, "b" : 2, "b" : 3}, "c" : 42}'); -- {serverError INCORRECT_DATA}
set input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects=1;
desc format(JSONEachRow, '{"a" : {"b" : 1, "b" : "Hello"}}');
select * from format(JSONEachRow, '{"a" : {"b" : 1, "b" : "Hello"}}'); -- {serverError INCORRECT_DATA}
desc format(JSONEachRow, '{"a" : {"b" : 1, "b" : {"c" : "Hello"}}}');
select * from format(JSONEachRow, '{"a" : {"b" : 1, "b" : {"c" : "Hello"}}}'); -- {serverError INCORRECT_DATA}

