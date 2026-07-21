-- Tags: no-fasttest
set allow_experimental_object_type=1;
desc format(JSONEachRow, '{"a" : {"b" : {"c" : 1, "d" : "str"}}}');
set allow_experimental_object_type=0, input_format_json_read_objects_as_strings=0, input_format_json_try_infer_named_tuples_from_objects=0, input_format_json_read_numbers_as_strings=0;
desc format(JSONEachRow, '{"a" : {"b" : {"c" : 1, "d" : "str"}}}'); -- {serverError CANNOT_EXTRACT_TABLE_STRUCTURE}

