desc format(JSONEachRow, '{"x" : null}, {"x" : 42}') settings schema_inference_make_columns_nullable=1;
select * from format(JSONEachRow, '{"x" : null}, {"x" : 42}') settings schema_inference_make_columns_nullable=1;
desc format(JSONEachRow, '{"x" : null}, {"x" : 42}') settings schema_inference_make_columns_nullable='auto', input_format_null_as_default=0;
select * from format(JSONEachRow, '{"x" : null}, {"x" : 42}') settings schema_inference_make_columns_nullable='auto', input_format_null_as_default=0;
desc format(JSONEachRow, '{"x" : null}, {"x" : 42}') settings schema_inference_make_columns_nullable=0, input_format_null_as_default=1;
select * from format(JSONEachRow, '{"x" : null}, {"x" : 42}') settings schema_inference_make_columns_nullable=0, input_format_null_as_default=1;

