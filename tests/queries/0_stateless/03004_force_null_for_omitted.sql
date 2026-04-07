set allow_suspicious_low_cardinality_types = 1;
insert into function file(concat(currentDatabase(), '.03004_data.bsonEachRow'), auto, 'null Nullable(UInt32)') select number % 2 ? NULL : number from numbers(5) settings engine_file_truncate_on_insert=1;
select * from file(concat(currentDatabase(), '.03004_data.bsonEachRow'), auto, 'null UInt32, foo UInt32');
select * from file(concat(currentDatabase(), '.03004_data.bsonEachRow'), auto, 'null UInt32, foo UInt32') settings input_format_force_null_for_omitted_fields = 1; -- { serverError TYPE_MISMATCH }
select * from file(concat(currentDatabase(), '.03004_data.bsonEachRow'), auto, 'null UInt32, foo Nullable(UInt32)');
select * from file(concat(currentDatabase(), '.03004_data.bsonEachRow'), auto, 'null UInt32, foo Nullable(UInt32)') settings input_format_force_null_for_omitted_fields = 1;
select * from file(concat(currentDatabase(), '.03004_data.bsonEachRow'), auto, 'null UInt32, foo LowCardinality(Nullable(UInt32))');
select * from file(concat(currentDatabase(), '.03004_data.bsonEachRow'), auto, 'null UInt32, foo LowCardinality(Nullable(UInt32))') settings input_format_force_null_for_omitted_fields = 1;

select * from format(JSONEachRow, 'foo UInt32', '{}');
select * from format(JSONEachRow, 'foo UInt32', '{}') settings input_format_force_null_for_omitted_fields = 1;  -- { serverError TYPE_MISMATCH }
select * from format(JSONEachRow, 'foo UInt32, bar Nullable(UInt32)', '{}');
select * from format(JSONEachRow, 'foo UInt32, bar Nullable(UInt32)', '{\"foo\":1}');
select * from format(JSONEachRow, 'foo UInt32, bar Nullable(UInt32)', '{}') settings input_format_force_null_for_omitted_fields = 1;  -- { serverError TYPE_MISMATCH }
select * from format(JSONEachRow, 'foo UInt32, bar Nullable(UInt32)', '{\"foo\":1}') settings input_format_force_null_for_omitted_fields = 1;
select * from format(JSONEachRow, 'foo UInt32, bar LowCardinality(Nullable(UInt32))', '{\"foo\":1}');
select * from format(JSONEachRow, 'foo UInt32, bar LowCardinality(Nullable(UInt32))', '{\"foo\":1}') settings input_format_force_null_for_omitted_fields = 1;

select * from format(CSVWithNamesAndTypes, 'foo UInt32, bar UInt32', 'foo\nUInt32\n1');
select * from format(CSVWithNamesAndTypes, 'foo UInt32, bar UInt32', 'foo\nUInt32\n1') settings input_format_force_null_for_omitted_fields = 1;  -- { serverError TYPE_MISMATCH }
select * from format(CSVWithNamesAndTypes, 'foo UInt32, bar Nullable(UInt32)', 'foo\nUInt32\n1') settings input_format_force_null_for_omitted_fields = 1;
select * from format(CSVWithNamesAndTypes, 'foo UInt32, bar LowCardinality(Nullable(UInt32))', 'foo\nUInt32\n1') settings input_format_force_null_for_omitted_fields = 1;
select * from format(CSVWithNamesAndTypes, 'foo UInt32, bar UInt32', 'foo,bar\nUInt32,UInt32\n1,2\n3\n') settings input_format_csv_allow_variable_number_of_columns = 1;
select * from format(CSVWithNamesAndTypes, 'foo UInt32, bar UInt32', 'foo,bar\nUInt32,UInt32\n1,2\n3\n') settings input_format_csv_allow_variable_number_of_columns = 1, input_format_force_null_for_omitted_fields = 1;  -- { serverError TYPE_MISMATCH }

select * from format(TSVWithNamesAndTypes, 'foo UInt32, bar UInt32', 'foo\nUInt32\n1');
select * from format(TSVWithNamesAndTypes, 'foo UInt32, bar UInt32', 'foo\nUInt32\n1') settings input_format_force_null_for_omitted_fields = 1;  -- { serverError TYPE_MISMATCH }
select * from format(TSVWithNamesAndTypes, 'foo UInt32, bar Nullable(UInt32)', 'foo\nUInt32\n1') settings input_format_force_null_for_omitted_fields = 1;
select * from format(TSVWithNamesAndTypes, 'foo UInt32, bar LowCardinality(Nullable(UInt32))', 'foo\nUInt32\n1') settings input_format_force_null_for_omitted_fields = 1;
select * from format(TSVWithNamesAndTypes, 'foo UInt32, bar UInt32', 'foo\tbar\nUInt32\tUInt32\n1\t2\n3\n') settings input_format_tsv_allow_variable_number_of_columns = 1;
select * from format(TSVWithNamesAndTypes, 'foo UInt32, bar UInt32', 'foo\tbar\nUInt32\tUInt32\n1\t2\n3\n') settings input_format_tsv_allow_variable_number_of_columns = 1, input_format_force_null_for_omitted_fields = 1;  -- { serverError TYPE_MISMATCH }

select * from format(TSKV, 'foo UInt32, bar UInt32', 'foo=1\n');
select * from format(TSKV, 'foo UInt32, bar UInt32', 'foo=1\n') settings input_format_force_null_for_omitted_fields = 1;  -- { serverError TYPE_MISMATCH }
select * from format(TSKV, 'foo UInt32, bar Nullable(UInt32)', 'foo=1\n') settings input_format_force_null_for_omitted_fields = 1;
select * from format(TSKV, 'foo UInt32, bar LowCardinality(Nullable(UInt32))', 'foo=1\n') settings input_format_force_null_for_omitted_fields = 1;
