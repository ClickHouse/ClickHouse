set input_format_max_rows_to_read_for_schema_inference=2;
set input_format_json_infer_incomplete_types_as_strings=0;
desc format('JSONEachRow', '{"a" : null}, {"a" : 42}') settings input_format_max_bytes_to_read_for_schema_inference=10; -- {serverError CANNOT_EXTRACT_TABLE_STRUCTURE}
desc format('JSONEachRow', '{"a" : null}, {"a" : 42}') settings input_format_max_bytes_to_read_for_schema_inference=20;

