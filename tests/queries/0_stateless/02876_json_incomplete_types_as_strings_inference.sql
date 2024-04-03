set input_format_json_infer_incomplete_types_as_strings=1;
desc format(JSONEachRow, '{"a" : null, "b" : {}, "c" : []}');
select * from format(JSONEachRow, '{"a" : null, "b" : {}, "c" : []}');
desc format(JSONEachRow, '{"a" : {"b" : null, "c" : [[], []]}, "d" : {"e" : [{}, {}], "f" : null}}');
select * from format(JSONEachRow, '{"a" : {"b" : null, "c" : [[], []]}, "d" : {"e" : [{}, {}], "f" : null}}');

