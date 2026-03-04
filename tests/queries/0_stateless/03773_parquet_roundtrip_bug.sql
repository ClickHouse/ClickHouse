-- Tags: no-fasttest

insert into function file(currentDatabase() || '_03773.parquet') select '{"a": 42}'::JSON as j1, tuple(42, '{}') as t, '{"b": 1337}'::JSON as j2 settings engine_file_truncate_on_insert=1;
select toTypeName(j1), toTypeName(t), toTypeName(j2) from file(currentDatabase() || '_03773.parquet') settings input_format_parquet_use_native_reader_v3=0;
select toTypeName(j1), toTypeName(t), toTypeName(j2) from file(currentDatabase() || '_03773.parquet') settings input_format_parquet_use_native_reader_v3=1;
