-- Tags: no-fasttest

-- Parquet sorts `uuid` statistics by unsigned big-endian byte comparison, while ClickHouse sorts the
-- `UUID` data type by its second half, so a min/max pair read out of a Parquet file is not an interval
-- in the column's own order. Using it as a bound silently dropped rows, and a pair that inverts under
-- ClickHouse's order was reported as corrupt metadata.
-- https://github.com/ClickHouse/ClickHouse/issues/118371

set engine_file_truncate_on_insert = 1;
set max_threads = 1;
set max_insert_threads = 1;

-- A, C, B in one row group. Byte-wise A < C < B, so the file's statistics are min = A, max = B; as
-- ClickHouse UUIDs the order is A < B < C, so C lies outside [A, B].
insert into function file(currentDatabase() || '_05099_u3.parquet', Parquet, 'u UUID')
    select toUUID(x) from values('x String',
        '00000000-0000-0000-0000-000000000001',
        '00000000-0000-0000-ffff-ffffffffffff',
        '00000001-0000-0000-0000-000000000002');

-- Only C and B, so the byte-ordered statistics pair (min = C, max = B) inverts as ClickHouse UUIDs.
insert into function file(currentDatabase() || '_05099_u2inv.parquet', Parquet, 'u UUID')
    select toUUID(x) from values('x String',
        '00000000-0000-0000-ffff-ffffffffffff',
        '00000001-0000-0000-0000-000000000002');

-- The same three values as a bare FIXED_LEN_BYTE_ARRAY(16) column with no logical type, for the
-- explicit `UUID` type hint.
insert into function file(currentDatabase() || '_05099_flba3.parquet', Parquet, 'u FixedString(16)')
    select toFixedString(unhex(x), 16) from values('x String',
        '00000000000000000000000000000001',
        '0000000000000000FFFFFFFFFFFFFFFF',
        '00000001000000000000000000000002');

insert into function file(currentDatabase() || '_05099_n.parquet', Parquet, 'n UInt64')
    select number from numbers(2000) settings output_format_parquet_row_group_size = 1000;

-- The statistics really are byte-lexicographic, and the column really carries the UUID logical type.
select trimBoth(row_groups[1].columns[1].statistics.min), trimBoth(row_groups[1].columns[1].statistics.max), columns[1].logical_type
    from file(currentDatabase() || '_05099_u3.parquet', ParquetMetadata);

-- C is in the file, so all three must return 1. The row group leg (default), the page index leg
-- (a different setting, so `input_format_parquet_filter_push_down = 0` alone does not avoid it), and
-- with both legs off.
select count() from file(currentDatabase() || '_05099_u3.parquet', Parquet, 'u UUID')
    where u = toUUID('00000000-0000-0000-ffff-ffffffffffff');
select count() from file(currentDatabase() || '_05099_u3.parquet', Parquet, 'u UUID')
    where u = toUUID('00000000-0000-0000-ffff-ffffffffffff')
    settings input_format_parquet_filter_push_down = 0;
select count() from file(currentDatabase() || '_05099_u3.parquet', Parquet, 'u UUID')
    where u = toUUID('00000000-0000-0000-ffff-ffffffffffff')
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;

-- The same read with no type hint at all, which is the form the defect was reported in; the column
-- is inferred as `Nullable(UUID)`.
select count() from file(currentDatabase() || '_05099_u3.parquet')
    where u = toUUID('00000000-0000-0000-ffff-ffffffffffff');

-- The inverted pair: a valid file that must be read, not rejected.
select count() from file(currentDatabase() || '_05099_u2inv.parquet', Parquet, 'u UUID')
    where u = toUUID('00000001-0000-0000-0000-000000000002');
select count() from file(currentDatabase() || '_05099_u2inv.parquet', Parquet, 'u UUID')
    where u = toUUID('00000001-0000-0000-0000-000000000002')
    settings input_format_parquet_filter_push_down = 0;

-- The explicit type hint reaches a second site with the same defect.
select count() from file(currentDatabase() || '_05099_flba3.parquet', Parquet, 'u UUID')
    where u = toUUID('00000000-0000-0000-ffff-ffffffffffff');
select count() from file(currentDatabase() || '_05099_flba3.parquet', Parquet, 'u UUID')
    where u = toUUID('00000000-0000-0000-ffff-ffffffffffff')
    settings input_format_parquet_filter_push_down = 0;

-- Unfiltered reads and the two endpoints were never affected.
select count() from file(currentDatabase() || '_05099_u3.parquet', Parquet, 'u UUID');
select count() from file(currentDatabase() || '_05099_u3.parquet', Parquet, 'u UUID')
    where u in (toUUID('00000000-0000-0000-0000-000000000001'), toUUID('00000001-0000-0000-0000-000000000002'));

-- Controls: the other consumers of Parquet statistics keep pruning. The same
-- FIXED_LEN_BYTE_ARRAY(16) column read as `String` (Parquet's byte order and ClickHouse's `String`
-- order agree), and a plain `UInt64` column.
select count() from file(currentDatabase() || '_05099_flba3.parquet', Parquet, 'u String')
    where u = unhex('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF') settings log_comment = '05099prune_string';
select count() from file(currentDatabase() || '_05099_n.parquet', Parquet, 'n UInt64')
    where n > 10000 settings log_comment = '05099prune_uint';

system flush logs query_log;
select distinct log_comment, ProfileEvents['ParquetReadRowGroups'], ProfileEvents['ParquetPrunedRowGroups']
    from system.query_log
    where current_database = currentDatabase() and type = 'QueryFinish' and log_comment like '05099prune%'
    order by log_comment;
