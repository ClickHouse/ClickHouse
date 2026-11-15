-- Tags: no-fasttest

set input_format_json_read_numbers_as_strings=1;
set input_format_json_infer_array_of_dynamic_from_array_of_different_types=0;
select * from format(JSONEachRow, '{"x" : 123}\n{"x" : "str"}');
select * from format(JSONEachRow, '{"x" : [123, "str"]}');
select * from format(JSONEachRow, '{"x" : [123, "456"]}\n{"x" : ["str", "rts"]}');

