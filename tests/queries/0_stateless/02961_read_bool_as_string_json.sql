set input_format_json_read_bools_as_strings=1;
select * from format(JSONEachRow, 'x String', '{"x" : true}, {"x" : false}, {"x" : "str"}');
select * from format(JSONEachRow, '{"x" : true}, {"x" : false}, {"x" : "str"}');
select * from format(JSONEachRow, 'x String', '{"x" : tru}'); -- {serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED}
select * from format(JSONEachRow, 'x String', '{"x" : fals}'); -- {serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED}
select * from format(JSONEachRow, 'x String', '{"x" : atru}'); -- {serverError INCORRECT_DATA}
select * from format(JSONEachRow, 'x Array(String)', '{"x" : [true, false]}, {"x" : [false, true]}, {"x" : ["str1", "str2"]}');
select * from format(JSONEachRow, '{"x" : [true, false]}, {"x" : [false, true]}, {"x" : ["str1", "str2"]}');

