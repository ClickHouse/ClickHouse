insert into function file(02453_data.jsonl, TSV) select 1 settings engine_file_truncate_on_insert=1;
select * from file(02453_data.jsonl, auto, 'x UInt32') settings input_format_allow_errors_num=1, input_format_record_errors_file_path='../error_file'; -- {serverError DATABASE_ACCESS_DENIED}

