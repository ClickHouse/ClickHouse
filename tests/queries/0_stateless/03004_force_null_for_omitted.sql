set allow_suspicious_low_cardinality_types = 1;
insert into function file('03004_data.bsonEachRow', auto, 'null Nullable(UInt32)') select number % 2 ? NULL : number from numbers(5) settings engine_file_truncate_on_insert=1;
select * from file('03004_data.bsonEachRow', auto, 'null UInt32, foo UInt32');
select * from file('03004_data.bsonEachRow', auto, 'null UInt32, foo UInt32') settings input_format_force_null_for_omitted_fields = 1; -- { serverError TYPE_MISMATCH }
select * from file('03004_data.bsonEachRow', auto, 'null UInt32, foo Nullable(UInt32)');
select * from file('03004_data.bsonEachRow', auto, 'null UInt32, foo Nullable(UInt32)') settings input_format_force_null_for_omitted_fields = 1;
select * from file('03004_data.bsonEachRow', auto, 'null UInt32, foo LowCardinality(Nullable(UInt32))');
select * from file('03004_data.bsonEachRow', auto, 'null UInt32, foo LowCardinality(Nullable(UInt32))') settings input_format_force_null_for_omitted_fields = 1;

select * from format(Values, 'foo UInt32', '()');
select * from format(Values, 'foo UInt32', '()') settings input_format_force_null_for_omitted_fields = 1; -- { serverError TYPE_MISMATCH }

select * from format(JSONEachRow, 'foo UInt32', '{}');
select * from format(JSONEachRow, 'foo UInt32', '{}') settings input_format_force_null_for_omitted_fields = 1;  -- { serverError TYPE_MISMATCH }
select * from format(JSONEachRow, 'foo UInt32, bar Nullable(UInt32)', '{}');
select * from format(JSONEachRow, 'foo UInt32, bar Nullable(UInt32)', '{\"foo\":1}');
select * from format(JSONEachRow, 'foo UInt32, bar Nullable(UInt32)', '{}') settings input_format_force_null_for_omitted_fields = 1;  -- { serverError TYPE_MISMATCH }
select * from format(JSONEachRow, 'foo UInt32, bar Nullable(UInt32)', '{\"foo\":1}') settings input_format_force_null_for_omitted_fields = 1;
select * from format(JSONEachRow, 'foo UInt32, bar LowCardinality(Nullable(UInt32))', '{\"foo\":1}');
select * from format(JSONEachRow, 'foo UInt32, bar LowCardinality(Nullable(UInt32))', '{\"foo\":1}') settings input_format_force_null_for_omitted_fields = 1;
