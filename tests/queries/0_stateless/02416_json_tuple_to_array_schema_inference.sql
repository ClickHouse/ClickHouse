set input_format_json_infer_array_of_dynamic_from_array_of_different_types=0;
desc format(JSONEachRow, '{"x" : [[42, null], [24, null]]}');
desc format(JSONEachRow, '{"x" : [[[42, null], []], 24]}');
desc format(JSONEachRow, '{"x" : {"key" : [42, null]}}');

