-- Tags: no-fasttest, no-parallel

insert into function file(data_02304.parquet) select 'hello' as s from numbers(3) settings engine_file_truncate_on_insert=1, output_format_parquet_string_as_string=1;
desc file(data_02304.parquet);
insert into function file(data_02304.orc) select 'hello' as s from numbers(3) settings engine_file_truncate_on_insert=1, output_format_orc_string_as_string=1;
desc file(data_02304.orc);
insert into function file(data_02304.arrow) select 'hello' as s from numbers(3) settings engine_file_truncate_on_insert=1, output_format_arrow_string_as_string=1;
desc file(data_02304.arrow);
