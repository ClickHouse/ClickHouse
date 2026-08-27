-- Tags: no-fasttest

insert into function file(currentDatabase() || '_data_02302.parquet') select 1 as x, null::Nullable(UInt8) as xx settings engine_file_truncate_on_insert=1;
select * from file(currentDatabase() || '_data_02302.parquet', auto, 'x UInt8, xx UInt8 default 10, y default 42, z default x + xx + y') settings input_format_parquet_allow_missing_columns=1;
insert into function file(currentDatabase() || '_data_02302.orc') select 1 as x, null::Nullable(UInt8) as xx settings engine_file_truncate_on_insert=1;
select * from file(currentDatabase() || '_data_02302.orc', auto, 'x UInt8, xx UInt8 default 10, y default 42, z default x + xx + y') settings input_format_orc_allow_missing_columns=1;
insert into function file(currentDatabase() || '_data_02302.arrow') select 1 as x, null::Nullable(UInt8) as xx settings engine_file_truncate_on_insert=1;
select * from file(currentDatabase() || '_data_02302.arrow', auto, 'x UInt8, xx UInt8 default 10, y default 42, z default x + xx + y') settings input_format_arrow_allow_missing_columns=1;

-- A count-only read reads just the single smallest declared column, so `d` must stay strictly
-- narrower than every other column for this to exercise a defaulted one. The count cache would
-- answer before a reader exists, and the count-only optimization is randomized off in some runs, so both are pinned.
select count() from file(currentDatabase() || '_data_02302.parquet', auto, 'x UInt64, d UInt8 default 10') settings use_cache_for_count_from_files = 0, optimize_count_from_files = 1;
-- With the setting off no `default` expression is applied, so the columns keep their type defaults.
select * from file(currentDatabase() || '_data_02302.parquet', auto, 'x UInt8, xx UInt8 default 10, y default 42, z default x + xx + y') settings input_format_parquet_allow_missing_columns=1, input_format_defaults_for_omitted_fields=0;
