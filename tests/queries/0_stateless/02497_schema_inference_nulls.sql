select 'JSONEachRow';
set schema_inference_make_columns_nullable=1;
set input_format_json_try_infer_named_tuples_from_objects=0;
set input_format_json_read_objects_as_strings=0;
set input_format_json_infer_incomplete_types_as_strings=0;
set input_format_json_read_numbers_as_strings=0;
set input_format_json_infer_array_of_dynamic_from_array_of_different_types=0;

desc format(JSONEachRow, '{"x" : 1234}, {"x" : "String"}') settings input_format_json_try_infer_numbers_from_strings=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
desc format(JSONEachRow, '{"x" : [null, 1]}');
desc format(JSONEachRow, '{"x" : [null, 1]}, {"x" : []}');
desc format(JSONEachRow, '{"x" : [null, 1]}, {"x" : [null]}');
desc format(JSONEachRow, '{"x" : [null, 1]}, {"x" : [1, null]}');
desc format(JSONEachRow, '{"x" : [null, 1]}, {"x" : ["abc", 1]}');
desc format(JSONEachRow, '{"x" : [null, 1]}, {"x" : ["abc", null]}');
desc format(JSONEachRow, '{"x" : {}}, {"x" : {"a" : 1}}');
desc format(JSONEachRow, '{"x" : {"a" : null}}, {"x" : {"b" : 1}}');
desc format(JSONEachRow, '{"x" : null}, {"x" : [1, 2]}');
desc format(JSONEachRow, '{"x" : [[], [null], [1, 2, 3]]}');
desc format(JSONEachRow, '{"x" : [{"a" : null}, {"b" : 1}]}');
desc format(JSONEachRow, '{"x" : [["2020-01-01", null, "1234"], ["abcd"]]}');

set schema_inference_make_columns_nullable='auto';
desc format(JSONEachRow, '{"x" : [1, 2]}');
desc format(JSONEachRow, '{"x" : [null, 1]}');
desc format(JSONEachRow, '{"x" : [1, 2]}, {"x" : [3]}');
desc format(JSONEachRow, '{"x" : [1, 2]}, {"x" : [null]}');

select 'JSONCompactEachRow';
set schema_inference_make_columns_nullable=1;
desc format(JSONCompactEachRow, '[1234], ["String"]') settings input_format_json_try_infer_numbers_from_strings=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
desc format(JSONCompactEachRow, '[[null, 1]]');
desc format(JSONCompactEachRow, '[[null, 1]], [[]]');
desc format(JSONCompactEachRow, '[[null, 1]], [[null]]');
desc format(JSONCompactEachRow, '[[null, 1]], [[1, null]]');
desc format(JSONCompactEachRow, '[[null, 1]], [["abc", 1]]');
desc format(JSONCompactEachRow, '[[null, 1]], [["abc", null]]');
desc format(JSONCompactEachRow, '[{}], [{"a" : 1}]');
desc format(JSONCompactEachRow, '[{"a" : null}], [{"b" : 1}]');
desc format(JSONCompactEachRow, '[null], [[1, 2]]');
desc format(JSONCompactEachRow, '[[[], [null], [1, 2, 3]]]');
desc format(JSONCompactEachRow, '[[{"a" : null}, {"b" : 1}]]');
desc format(JSONCompactEachRow, '[[["2020-01-01", null, "1234"], ["abcd"]]]');

set schema_inference_make_columns_nullable='auto';
desc format(JSONCompactEachRow, '[[1, 2]]');
desc format(JSONCompactEachRow, '[[null, 1]]');
desc format(JSONCompactEachRow, '[[1, 2]], [[3]]');
desc format(JSONCompactEachRow, '[[1, 2]], [[null]]');


select 'CSV';
set schema_inference_make_columns_nullable=1;
desc format(CSV, '"[null, 1]"');
desc format(CSV, '"[null, 1]"\n"[]"');
desc format(CSV, '"[null, 1]"\n"[null]"');
desc format(CSV, '"[null, 1]"\n"[1, null]"');
desc format(CSV, '"{}"\n"{\'a\' : 1}"');
desc format(CSV, '"{\'a\' : null}"\n"{\'b\' : 1}"');
desc format(CSV, '"[[], [null], [1, 2, 3]]"');
desc format(CSV, '"[{\'a\' : null}, {\'b\' : 1}]"');
desc format(CSV, '"[[\'2020-01-01\', null, \'1234\'], [\'abcd\']]"');

set schema_inference_make_columns_nullable='auto';
desc format(CSV, '"[1,2]"');
desc format(CSV, '"[NULL, 1]"');
desc format(CSV, '"[1, 2]"\n"[3]"');
desc format(CSV, '"[1, 2]"\n"[null]"');

set schema_inference_make_columns_nullable=0;
desc format(CSV, '\\N,\\N,1\nb,\\N,1');
set schema_inference_make_columns_nullable='auto';
desc format(CSV, '\\N,\\N,1\nb,\\N,1');
set schema_inference_make_columns_nullable=3;
desc format(CSV, '\\N,\\N,1\nb,\\N,1');
