-- Tags: no-fasttest

insert into function file(current_database() ||'03626_parquet_bool.parquet') select true as x settings engine_file_truncate_on_insert=1;
select * from file(current_database() ||'03626_parquet_bool.parquet') where x=1 settings input_format_parquet_use_native_reader_v3=1;
