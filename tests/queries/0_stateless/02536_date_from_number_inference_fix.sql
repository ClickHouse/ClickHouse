set input_format_json_try_infer_numbers_from_strings=1;
desc format(JSONEachRow, '{"x" : "20000101"}');
select * from format(JSONEachRow, '{"x" : "20000101"}');
select * from format(JSONEachRow, '{"x" : "19000101"}');

