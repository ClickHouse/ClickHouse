-- Tags: no-fasttest
-- no-fasttest: the Parquet format is not built in the fast-test image.

-- A Parquet integer column's min/max statistics are ordered as the stored type. Read with a type hint
-- that reinterprets the sign or narrows the width, the pair is no longer an interval in the requested
-- type's order, so using it as a bound silently dropped rows: for a row group holding {-1, 5} the signed
-- max 5 is not an upper bound in unsigned order, and the row group (and page) holding 4294967295 was
-- pruned.
-- https://github.com/ClickHouse/ClickHouse/issues/118376

set engine_file_truncate_on_insert = 1;
set max_threads = 1;
set max_insert_threads = 1;
set max_block_size = 1000000;
set output_format_parquet_row_group_size = 1000000;

-- {-1, 5} in one row group. Written as plain INT32/INT64 with no logical annotation, so the statistics
-- are computed in signed order.
insert into function file(currentDatabase() || '_05230_i32.parquet', Parquet, 'x Int32')
    select arrayJoin([toInt32(-1), toInt32(5)]) as x;
insert into function file(currentDatabase() || '_05230_i64.parquet', Parquet, 'x Int64')
    select arrayJoin([toInt64(-1), toInt64(5)]) as x;

-- The mirror direction: an unsigned column read as the same-width signed type, where the unsigned max
-- reinterprets to -1.
insert into function file(currentDatabase() || '_05230_u64.parquet', Parquet, 'x UInt64')
    select arrayJoin([toUInt64(1), toUInt64(18446744073709551615)]) as x;

insert into function file(currentDatabase() || '_05230_i32bloom.parquet', Parquet, 'x Int32')
    select arrayJoin([toInt32(-1), toInt32(5)]) as x settings output_format_parquet_write_bloom_filter = 1;

-- The statistics really are the signed-order pair, and the column really carries no logical type.
select trimBoth(row_groups[1].columns[1].statistics.min), trimBoth(row_groups[1].columns[1].statistics.max),
       columns[1].physical_type, columns[1].logical_type
    from file(currentDatabase() || '_05230_i32.parquet', ParquetMetadata);

-- 4294967295 is in the file, so all four must return 1: the row group leg (default), the page index leg
-- (a different setting, so disabling either one alone does not avoid the other), and with both legs off.
select count() from file(currentDatabase() || '_05230_i32.parquet', Parquet, 'x UInt32')
    where x = 4294967295;
select count() from file(currentDatabase() || '_05230_i32.parquet', Parquet, 'x UInt32')
    where x = 4294967295 settings input_format_parquet_filter_push_down = 0;
select count() from file(currentDatabase() || '_05230_i32.parquet', Parquet, 'x UInt32')
    where x = 4294967295 settings input_format_parquet_page_filter_push_down = 0;
select count() from file(currentDatabase() || '_05230_i32.parquet', Parquet, 'x UInt32')
    where x = 4294967295
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;

-- The 64-bit arm, and the mirror direction.
select count() from file(currentDatabase() || '_05230_i64.parquet', Parquet, 'x UInt64')
    where x = 18446744073709551615;
select count() from file(currentDatabase() || '_05230_u64.parquet', Parquet, 'x Int64')
    where x = -1;
-- A widening flip, where the width comparison cannot help: the same INT32 file read as UInt64. The kept
-- signed max 5 is not an upper bound in 64-bit unsigned order either.
select count() from file(currentDatabase() || '_05230_i32.parquet', Parquet, 'x UInt64')
    where x = 18446744073709551615;

-- A narrowing hint reorders the endpoints without changing the signedness: a UINT_32 column
-- {65535, 65536} read as UInt16 produces {65535, 0}, so the stored max is not an upper bound either.
-- The hash filters lose these rows through a separate mechanism, so pin them off to leave only the
-- min/max legs under test.
insert into function file(currentDatabase() || '_05230_u32n.parquet', Parquet, 'x UInt32')
    select arrayJoin([toUInt32(65535), toUInt32(65536)]) as x;
insert into function file(currentDatabase() || '_05230_i32n.parquet', Parquet, 'x Int32')
    select arrayJoin([toInt32(65536), toInt32(65537)]) as x;
select count() from file(currentDatabase() || '_05230_u32n.parquet', Parquet, 'x UInt16') where x = 0
    settings input_format_parquet_dictionary_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0;
select count() from file(currentDatabase() || '_05230_i32n.parquet', Parquet, 'x Int16') where x = 0
    settings input_format_parquet_dictionary_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0;

-- An Enum orders by its underlying signed integer, so an Enum hint reorders exactly like the native
-- integer of that width: a UINT_8 column {5, 200} read 8-bit signed gives {5, -56}, and the stored max 200
-- is not an upper bound. A range predicate is served by neither hash filter, so these four need no pinning
-- and all four returned 0 at completely default settings.
insert into function file(currentDatabase() || '_05230_u8e.parquet', Parquet, 'x UInt8')
    select arrayJoin([toUInt8(5), toUInt8(200)]) as x;
insert into function file(currentDatabase() || '_05230_u16e.parquet', Parquet, 'x UInt16')
    select arrayJoin([toUInt16(5), toUInt16(40000)]) as x;
select count() from file(currentDatabase() || '_05230_u8e.parquet', Parquet, 'x Int8') where x < 0;
select count() from file(currentDatabase() || '_05230_u8e.parquet', Parquet, 'x Enum8(''lo'' = 5, ''hi'' = -56)')
    where x < 'lo';
select count() from file(currentDatabase() || '_05230_u16e.parquet', Parquet, 'x Int16') where x < 0;
select count() from file(currentDatabase() || '_05230_u16e.parquet', Parquet, 'x Enum16(''lo'' = 5, ''hi'' = -25536)')
    where x < 'lo';

-- A day number outside the requested date type's window is reinterpreted just like a narrowed integer,
-- so `Date` and `Date32` hints reorder the same statistics. A plain INT32/INT64 column {65536, 65537}
-- read as `Date` holds two 1970-01-01 values under statistics that stay [65536, 65537]; an INT64 column
-- {100000, 2^32} read as `Date32` holds a 2106 day under a kept min of 2243-10-17; and an INT32 column
-- {2932896, 2932897} read as `Date32` straddles the day 2932896 limit above which the cast reads the
-- number as seconds, so 9999-12-31 and 1970-02-03 arrive in that order.
-- The hash filters lose these rows too, so pin them off to leave only the min/max legs under test. The
-- `Date32` arms ask for a range rather than a day, because a value that large is read as a timestamp and
-- its date then depends on the session timezone, while the bounds are plain day numbers.
insert into function file(currentDatabase() || '_05230_i64d.parquet', Parquet, 'x Int64')
    select arrayJoin([toInt64(65536), toInt64(65537)]) as x;
insert into function file(currentDatabase() || '_05230_i64d32.parquet', Parquet, 'x Int64')
    select arrayJoin([toInt64(100000), toInt64(4294967296)]) as x;
insert into function file(currentDatabase() || '_05230_i32d32.parquet', Parquet, 'x Int32')
    select arrayJoin([toInt32(2932896), toInt32(2932897)]) as x;
select count() from file(currentDatabase() || '_05230_i32n.parquet', Parquet, 'x Date')
    where x = toDate('1970-01-01')
    settings input_format_parquet_dictionary_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0;
select count() from file(currentDatabase() || '_05230_i64d.parquet', Parquet, 'x Date')
    where x = toDate('1970-01-01')
    settings input_format_parquet_dictionary_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0;
select count() from file(currentDatabase() || '_05230_i64d32.parquet', Parquet, 'x Date32')
    where x < toDate32('2243-10-17')
    settings input_format_parquet_dictionary_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0;
select count() from file(currentDatabase() || '_05230_i32d32.parquet', Parquet, 'x Date32')
    where x < toDate32('2000-01-01')
    settings input_format_parquet_dictionary_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0;

-- A parquet `DATE` column does carry a day-range check, but `convertField` only applied it to a signed
-- Field, and a `Date` target asks for an unsigned one. So days past 2149-06-06 read as `Date` with
-- `date_time_overflow_behavior = 'saturate'` arrived as 2149-06-06 while their raw bounds survived.
insert into function file(currentDatabase() || '_05230_dsat.parquet', Parquet, 'x Date32')
    select arrayJoin([toDate32('2149-06-07'), toDate32('2149-06-08')]) as x;
select count() from file(currentDatabase() || '_05230_dsat.parquet', Parquet, 'x Date')
    where x = toDate('2149-06-06')
    settings date_time_overflow_behavior = 'saturate',
             input_format_parquet_dictionary_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 0;

-- The dictionary and bloom filters hash both sides after casting to the parquet physical type, so they
-- agree for a same-width signedness flip. Pin that: with both min/max legs off and each hash filter on
-- in turn, the row is still found.
select count() from file(currentDatabase() || '_05230_i32.parquet', Parquet, 'x UInt32')
    where x = 4294967295
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05230_i32bloom.parquet', Parquet, 'x UInt32')
    where x = 4294967295
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0,
             input_format_parquet_bloom_filter_push_down = 1;

-- An unfiltered read and a predicate inside the kept interval were never affected.
select count() from file(currentDatabase() || '_05230_i32.parquet', Parquet, 'x UInt32');
select count() from file(currentDatabase() || '_05230_i32.parquet', Parquet, 'x UInt32') where x = 5;

-- Controls: pruning is preserved wherever the statistics are still an interval in the requested order.
-- Matching hints, ClickHouse's own round-trip, the always-monotonic widening of an unsigned column to a
-- wider signed type (at the physical width and at an 8-bit annotated one), a BOOLEAN column read as
-- Int8, and a DATE column read as Date.
insert into function file(currentDatabase() || '_05230_c_i32.parquet', Parquet, 'x Int32')
    select toInt32(number) as x from numbers(4000) settings output_format_parquet_row_group_size = 1000;
insert into function file(currentDatabase() || '_05230_c_u32.parquet', Parquet, 'x UInt32')
    select toUInt32(number) as x from numbers(4000) settings output_format_parquet_row_group_size = 1000;
insert into function file(currentDatabase() || '_05230_c_u32big.parquet', Parquet, 'x UInt32')
    select toUInt32(4294963296 + number) as x from numbers(4000) settings output_format_parquet_row_group_size = 1000;
insert into function file(currentDatabase() || '_05230_c_u8.parquet', Parquet, 'x UInt8')
    select toUInt8(number) as x from numbers(256) settings output_format_parquet_row_group_size = 128;
insert into function file(currentDatabase() || '_05230_c_bool.parquet', Parquet, 'x Bool')
    select number >= 1000 as x from numbers(2000) settings output_format_parquet_row_group_size = 1000;
insert into function file(currentDatabase() || '_05230_c_d32.parquet', Parquet, 'x Date32')
    select toDate32('2001-01-01') + number as x from numbers(2000) settings output_format_parquet_row_group_size = 1000;
-- The two order-preserving Enum hints: an INT_8 column read as Enum8 is the identity, and a UINT_8 column
-- read as Enum16 is a widening flip. Two row groups each, so a pruned one is visible in the counters.
insert into function file(currentDatabase() || '_05230_c_i8e.parquet', Parquet, 'x Int8')
    select toInt8(if(number < 1000, -100, 100)) as x from numbers(2000) settings output_format_parquet_row_group_size = 1000;
insert into function file(currentDatabase() || '_05230_c_u8e.parquet', Parquet, 'x UInt8')
    select toUInt8(if(number < 1000, 5, 200)) as x from numbers(2000) settings output_format_parquet_row_group_size = 1000;
insert into function file(currentDatabase() || '_05230_c_u16d.parquet', Parquet, 'x UInt16')
    select toUInt16(number) as x from numbers(4000) settings output_format_parquet_row_group_size = 1000;

select count() from file(currentDatabase() || '_05230_c_i32.parquet', Parquet, 'x Int32') where x > 3500
    settings log_comment = '05230prune_i32', input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05230_c_u32.parquet', Parquet, 'x UInt32') where x > 3500
    settings log_comment = '05230prune_u32', input_format_parquet_dictionary_filter_push_down = 1048576;
-- All values exceed INT32_MAX, so reading them as Int64 really does change the endpoints' signedness;
-- the cast is an exact widening, so the pruning must survive.
select count() from file(currentDatabase() || '_05230_c_u32big.parquet', Parquet, 'x Int64') where x > 4294966796
    settings log_comment = '05230prune_u32big', input_format_parquet_dictionary_filter_push_down = 1048576;
-- A UINT_8 column read as Int16: the annotation declares 8 bits, not the physical 32, so this widening
-- is exact and the statistics must still prune.
select count() from file(currentDatabase() || '_05230_c_u8.parquet', Parquet, 'x Int16') where x > 200
    settings log_comment = '05230prune_u8', input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05230_c_bool.parquet', Parquet, 'x Int8') where x = 1
    settings log_comment = '05230prune_bool', input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05230_c_d32.parquet', Parquet, 'x Date') where x > toDate('2003-10-01')
    settings log_comment = '05230prune_date', input_format_parquet_dictionary_filter_push_down = 1048576;
-- The date directions that stay sound. A DATE column carries a day-number range check that drops any
-- endpoint the requested type cannot represent, so it prunes under both date hints. `Date` needs no such
-- check when the stored domain is at most an unsigned 16 bits, since that is exactly its own window.
select count() from file(currentDatabase() || '_05230_c_d32.parquet', Parquet, 'x Date32') where x > toDate32('2003-10-01')
    settings log_comment = '05230prune_d32_from_date', input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05230_c_u16d.parquet', Parquet, 'x Date')
    where x > toDate('1970-01-01') + 3500
    settings log_comment = '05230prune_date_u16', input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05230_c_i8e.parquet', Parquet, 'x Enum8(''lo'' = -100, ''hi'' = 100)')
    where x > 'lo'
    settings log_comment = '05230prune_i8e8', input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05230_c_u8e.parquet', Parquet, 'x Enum16(''lo'' = 5, ''hi'' = 200)')
    where x > 'lo'
    settings log_comment = '05230prune_u8e16', input_format_parquet_dictionary_filter_push_down = 1048576;

system flush logs query_log;
select distinct log_comment, ProfileEvents['ParquetReadRowGroups'], ProfileEvents['ParquetPrunedRowGroups']
    from system.query_log
    where current_database = currentDatabase() and type = 'QueryFinish' and log_comment like '05230prune%'
    order by log_comment;
