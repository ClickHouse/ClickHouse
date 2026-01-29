-- Tags: no-parallel, no-fasttest

insert into function file('03626_parquet_bool.parquet') select true as x settings engine_file_truncate_on_insert=1;
select * from file('03626_parquet_bool.parquet') where x=1 settings input_format_parquet_use_native_reader_v3=1;
