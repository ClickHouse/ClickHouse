-- Tags: no-fasttest
set input_format_json_try_infer_named_tuples_from_objects=0;
set input_format_json_read_objects_as_strings=0;
select * from format(JSONEachRow, '{"a" : {}}, {"a" : {"b" : 1}}')
