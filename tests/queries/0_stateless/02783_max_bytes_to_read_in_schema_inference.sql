set input_format_max_rows_to_read_for_schema_inference=2;
desc format('JSONEachRow', '{"a" : null}, {"a" : 42}') settings input_format_max_bytes_to_read_for_schema_inference=10; -- {serverError ONLY_NULLS_WHILE_READING_SCHEMA}
desc format('JSONEachRow', '{"a" : null}, {"a" : 42}') settings input_format_max_bytes_to_read_for_schema_inference=20;

