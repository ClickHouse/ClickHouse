set input_format_json_read_arrays_as_strings = 1;
select * from format(JSONEachRow, 'arr String', '{"arr" : [1, "Hello", [1,2,3]]}');
