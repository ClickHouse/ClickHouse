set input_format_json_infer_array_of_dynamic_from_array_of_different_types=0;
desc format(JSONEachRow, '{"data" : [[1, null, 3, null], [null, {"a" : 12, "b" : 12}, null, "string"], [null, null, 4, "string"]]}');

