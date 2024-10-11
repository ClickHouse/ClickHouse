set input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects=1;
desc format(JSONEachRow, '{"obj" : {"a" : 42}}, {"obj" : {"a" : {"b" : 42}}}');
select * from format(JSONEachRow, '{"obj" : {"a" : 42}}, {"obj" : {"a" : {"b" : 42}}}');

