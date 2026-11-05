-- Tags: no-fasttest
-- Regression test: with `input_format_null_as_default = 0`, reading a NULL value from
-- a nullable Parquet column into a non-Nullable column used to throw LOGICAL_ERROR
-- "Unexpected number of rows in column subchunk" instead of the expected
-- CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN.
-- The bug lives in the v3 native Parquet reader, so it must be enabled explicitly
-- (it is the default in 25.11+, but bugfix validation runs against an older stable).

insert into function file(currentDatabase() || '_data_04104.parquet') select 1 as x, null::Nullable(UInt8) as xx settings engine_file_truncate_on_insert=1;

-- Column `xx` is read as a non-Nullable UInt8 while the file stores a NULL.
-- With null_as_default disabled, this must raise CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN.
select * from file(currentDatabase() || '_data_04104.parquet', auto, 'x UInt8, xx UInt8') settings input_format_null_as_default = 0, input_format_parquet_use_native_reader_v3 = 1; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

-- Sanity check: with null_as_default enabled, the same query succeeds and replaces NULL with the default.
select * from file(currentDatabase() || '_data_04104.parquet', auto, 'x UInt8, xx UInt8') settings input_format_null_as_default = 1, input_format_parquet_use_native_reader_v3 = 1;

-- Sanity check: reading into a Nullable column works either way.
select * from file(currentDatabase() || '_data_04104.parquet', auto, 'x UInt8, xx Nullable(UInt8)') settings input_format_null_as_default = 0, input_format_parquet_use_native_reader_v3 = 1;

-- Sanity check: a file with only non-null values reads fine even with null_as_default disabled.
insert into function file(currentDatabase() || '_data_04104_nonull.parquet') select 1 as x, 2::Nullable(UInt8) as xx settings engine_file_truncate_on_insert=1;
select * from file(currentDatabase() || '_data_04104_nonull.parquet', auto, 'x UInt8, xx UInt8') settings input_format_null_as_default = 0, input_format_parquet_use_native_reader_v3 = 1;
