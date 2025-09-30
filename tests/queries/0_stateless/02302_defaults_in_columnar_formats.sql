-- Tags: no-fasttest

insert into function file(currentDatabase() || '_data_02302.parquet') select 1 as x, null::Nullable(UInt8) as xx settings engine_file_truncate_on_insert=1;
select * from file(currentDatabase() || '_data_02302.parquet', auto, 'x UInt8, xx UInt8 default 10, y default 42, z default x + xx + y') settings input_format_parquet_allow_missing_columns=1;
insert into function file(currentDatabase() || '_data_02302.orc') select 1 as x, null::Nullable(UInt8) as xx settings engine_file_truncate_on_insert=1;
select * from file(currentDatabase() || '_data_02302.orc', auto, 'x UInt8, xx UInt8 default 10, y default 42, z default x + xx + y') settings input_format_orc_allow_missing_columns=1;
insert into function file(currentDatabase() || '_data_02302.arrow') select 1 as x, null::Nullable(UInt8) as xx settings engine_file_truncate_on_insert=1;
select * from file(currentDatabase() || '_data_02302.arrow', auto, 'x UInt8, xx UInt8 default 10, y default 42, z default x + xx + y') settings input_format_arrow_allow_missing_columns=1;
