select ['{}', '{"c" : [1, {"b" : []}]}']::Array(JSON) settings input_format_json_infer_incomplete_types_as_strings=0; -- {serverError INCORRECT_DATA}

