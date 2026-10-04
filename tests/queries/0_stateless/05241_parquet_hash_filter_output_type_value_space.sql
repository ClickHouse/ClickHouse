-- Tags: no-fasttest
-- no-fasttest: the Parquet format is not built in the fast-test image.

-- The Parquet bloom-filter and dictionary-filter row-group legs hash the stored side in physical space
-- (the dictionary is materialized as the decoded type and cast to the physical type, and the file's own
-- bloom filter holds digests of the physical values) but hash query constants of the requested type.
-- Read with a type hint whose value space differs from the stored one, a matching constant hashed to a
-- digest the filter had never seen, and the row group holding the match was pruned.
-- https://github.com/ClickHouse/ClickHouse/issues/118376

set engine_file_truncate_on_insert = 1;
set max_threads = 1;
set max_insert_threads = 1;
set max_block_size = 1000000;
set output_format_parquet_row_group_size = 1000000;

insert into function file(currentDatabase() || '_05241_u32.parquet', Parquet, 'x UInt32')
    select toUInt32(70000) as x;
insert into function file(currentDatabase() || '_05241_i64.parquet', Parquet, 'x Int64')
    select toInt64(4294967301) as x;
insert into function file(currentDatabase() || '_05241_u8.parquet', Parquet, 'x UInt8')
    select toUInt8(200) as x;
insert into function file(currentDatabase() || '_05241_i8.parquet', Parquet, 'x Int8')
    select toInt8(-1) as x;
insert into function file(currentDatabase() || '_05241_u32big.parquet', Parquet, 'x UInt32')
    select toUInt32(3000000000) as x;
insert into function file(currentDatabase() || '_05241_i32neg.parquet', Parquet, 'x Int32')
    select toInt32(number - 4000) as x from numbers(4000)
    settings output_format_parquet_row_group_size = 1000;
insert into function file(currentDatabase() || '_05241_i32ts.parquet', Parquet, 'x Int32')
    select toInt32(2000000000) as x;
insert into function file(currentDatabase() || '_05241_str.parquet', Parquet, 'x String')
    select '3500' as x;
insert into function file(currentDatabase() || '_05241_fs4.parquet', Parquet, 'x FixedString(4)')
    select toFixedString('ab', 4) as x;

-- Part 1: shapes whose matching row the hash legs pruned away. Every select must print the number of
-- rows in the file that really match.

-- An 8-bit signedness flip, where only the hash legs ever prune: the stored value sign-extends to
-- 0xffffffff while the constant 255 casts to 255. Run it four times to pin that neither min/max leg is
-- what hides the row: at the defaults, with the row-group leg off, with the page leg off, and with both.
select count() from file(currentDatabase() || '_05241_i8.parquet', Parquet, 'x UInt8') where x = 255;
select count() from file(currentDatabase() || '_05241_i8.parquet', Parquet, 'x UInt8') where x = 255
    settings input_format_parquet_filter_push_down = 0;
select count() from file(currentDatabase() || '_05241_i8.parquet', Parquet, 'x UInt8') where x = 255
    settings input_format_parquet_page_filter_push_down = 0;
select count() from file(currentDatabase() || '_05241_i8.parquet', Parquet, 'x UInt8') where x = 255
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;

-- A signed source widened into an unsigned target, where comparing the widths alone would allow it.
select count() from file(currentDatabase() || '_05241_i8.parquet', Parquet, 'x UInt16') where x = 65535;

-- A signed source read as DateTime, which saturates every negative value to the epoch.
select count() from file(currentDatabase() || '_05241_i32neg.parquet', Parquet, 'x DateTime')
    where x = toDateTime(0);

-- A String column read as a wider FixedString, where the constant is padded to the requested width and
-- so hashes more bytes than the stored value.
select count() from file(currentDatabase() || '_05241_str.parquet', Parquet, 'x FixedString(8)')
    where x = '3500';

-- The remaining shapes are pruned by the min/max statistics legs as well, through the separate defect
-- that reads statistics in an order the requested type does not share (issue #118376). Those legs are
-- turned off below so that what is asserted here is the hash legs alone.

-- Narrowing hints: only the low 16 bits of 70000, and the low 32 bits of 4294967301, survive the cast.
select count() from file(currentDatabase() || '_05241_u32.parquet', Parquet, 'x UInt16') where x = 4464
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;
select count() from file(currentDatabase() || '_05241_i64.parquet', Parquet, 'x Int32') where x = 5
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;

-- The mirror of the 8-bit signedness flip above, unsigned stored and signed requested.
select count() from file(currentDatabase() || '_05241_u8.parquet', Parquet, 'x Int8') where x = -56
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;

-- Enum hints: narrowing into the enum's Int16, and a signedness flip through an Enum8.
select count() from file(currentDatabase() || '_05241_u32big.parquet', Parquet, 'x Enum16(\'a\' = 24064)')
    where x = 'a'
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;
select count() from file(currentDatabase() || '_05241_u8.parquet', Parquet, 'x Enum8(\'a\' = -56)')
    where x = 'a'
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;

-- A unit change, which no width comparison can detect: the integer -> Date32 cast reads a value wider
-- than 16 bits as a Unix timestamp, so 3000000000 seconds becomes a day number. Which day that is
-- depends on the session time zone, so the constant is written as the same conversion rather than as a
-- date literal.
select count() from file(currentDatabase() || '_05241_u32big.parquet', Parquet, 'x Date32')
    where x = toDate32(toDateTime(3000000000))
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;
-- The same unit change over a signed column, which the signedness rule alone would let through: Int32
-- and Date32 have the same width and the same sign, so only the source's declared width rules it out.
select count() from file(currentDatabase() || '_05241_i32ts.parquet', Parquet, 'x Date32')
    where x = toDate32(toDateTime(2000000000))
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;

-- The Nullable wrapper must not hide the narrowing underneath it.
select count() from file(currentDatabase() || '_05241_u32.parquet', Parquet, 'x Nullable(UInt16)')
    where x = 4464
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;

-- A FixedString column read as String: the stored value carries its own padding, which the cast strips
-- and the constant never had.
select count() from file(currentDatabase() || '_05241_fs4.parquet', Parquet, 'x String') where x = 'ab'
    settings input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;

-- Part 2: pruning that must survive, one control per clause of the predicate. Each fixture has four row
-- groups with the match in the last one.

insert into function file(currentDatabase() || '_05241_c_u8.parquet', Parquet, 'x UInt8')
    select toUInt8(number) as x from numbers(256)
    settings output_format_parquet_row_group_size = 64, output_format_parquet_write_bloom_filter = 1;
insert into function file(currentDatabase() || '_05241_c_i8.parquet', Parquet, 'x Int8')
    select toInt8(number - 128) as x from numbers(256)
    settings output_format_parquet_row_group_size = 64, output_format_parquet_write_bloom_filter = 1;
insert into function file(currentDatabase() || '_05241_c_u32.parquet', Parquet, 'x UInt32')
    select toUInt32(number) as x from numbers(4000)
    settings output_format_parquet_row_group_size = 1000, output_format_parquet_write_bloom_filter = 1;
insert into function file(currentDatabase() || '_05241_c_i32.parquet', Parquet, 'x Int32')
    select toInt32(number) as x from numbers(4000)
    settings output_format_parquet_row_group_size = 1000, output_format_parquet_write_bloom_filter = 1;
insert into function file(currentDatabase() || '_05241_c_u64.parquet', Parquet, 'x UInt64')
    select toUInt64(18446744073709547616 + number) as x from numbers(4000)
    settings output_format_parquet_row_group_size = 1000, output_format_parquet_write_bloom_filter = 1;
insert into function file(currentDatabase() || '_05241_c_u32ts.parquet', Parquet, 'x UInt32')
    select toUInt32(3000000000 + number) as x from numbers(4000)
    settings output_format_parquet_row_group_size = 1000, output_format_parquet_write_bloom_filter = 1;
insert into function file(currentDatabase() || '_05241_c_str.parquet', Parquet, 'x String')
    select leftPad(toString(number), 4, '0') as x from numbers(4000)
    settings output_format_parquet_row_group_size = 1000, output_format_parquet_write_bloom_filter = 1;
insert into function file(currentDatabase() || '_05241_c_ipv6.parquet', Parquet, 'x IPv6')
    select toIPv6('2001:db8::' || lower(hex(toUInt16(number)))) as x from numbers(4000)
    settings output_format_parquet_row_group_size = 1000, output_format_parquet_write_bloom_filter = 1;
insert into function file(currentDatabase() || '_05241_c_i64.parquet', Parquet, 'x Int64')
    select toInt64(number) as x from numbers(4000)
    settings output_format_parquet_row_group_size = 1000, output_format_parquet_write_bloom_filter = 1;
insert into function file(currentDatabase() || '_05241_c_fs8.parquet', Parquet, 'x FixedString(8)')
    select leftPad(toString(number), 8, '0')::FixedString(8) as x from numbers(256)
    settings output_format_parquet_row_group_size = 64, output_format_parquet_write_bloom_filter = 1;

-- value_preserving with the requested type equal to the decoded one, at the declared width and at the
-- physical width.
select count() from file(currentDatabase() || '_05241_c_u8.parquet', Parquet, 'x UInt8') where x = 200
    settings log_comment = 'hfilter_d_01_u8_u8', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05241_c_u8.parquet', Parquet, 'x UInt8') where x = 200
    settings log_comment = 'hfilter_b_01_u8_u8', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 1,
             input_format_parquet_dictionary_filter_push_down = 0;
select count() from file(currentDatabase() || '_05241_c_u32.parquet', Parquet, 'x UInt32') where x = 3500
    settings log_comment = 'hfilter_d_02_u32_u32', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05241_c_u32.parquet', Parquet, 'x UInt32') where x = 3500
    settings log_comment = 'hfilter_b_02_u32_u32', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 1,
             input_format_parquet_dictionary_filter_push_down = 0;

-- value_preserving with an unsigned source widening, and with a signed source widening to signed.
select count() from file(currentDatabase() || '_05241_c_u8.parquet', Parquet, 'x Int16') where x = 200
    settings log_comment = 'hfilter_d_03_u8_i16', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05241_c_u8.parquet', Parquet, 'x Int16') where x = 200
    settings log_comment = 'hfilter_b_03_u8_i16', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 1,
             input_format_parquet_dictionary_filter_push_down = 0;
select count() from file(currentDatabase() || '_05241_c_i8.parquet', Parquet, 'x Int16') where x = 100
    settings log_comment = 'hfilter_d_04_i8_i16', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05241_c_i8.parquet', Parquet, 'x Int16') where x = 100
    settings log_comment = 'hfilter_b_04_i8_i16', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 1,
             input_format_parquet_dictionary_filter_push_down = 0;

-- reinterpretation: the cast wraps at or above the physical width, so the hashed low bits are the same.
-- Int8 -> UInt32 is the arm a rule keyed on the declared width alone would have withheld.
select count() from file(currentDatabase() || '_05241_c_i32.parquet', Parquet, 'x UInt32') where x = 3500
    settings log_comment = 'hfilter_d_05_i32_u32', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05241_c_i32.parquet', Parquet, 'x UInt32') where x = 3500
    settings log_comment = 'hfilter_b_05_i32_u32', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 1,
             input_format_parquet_dictionary_filter_push_down = 0;
select count() from file(currentDatabase() || '_05241_c_u64.parquet', Parquet, 'x Int64') where x = -500
    settings log_comment = 'hfilter_d_06_u64_i64', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05241_c_u64.parquet', Parquet, 'x Int64') where x = -500
    settings log_comment = 'hfilter_b_06_u64_i64', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 1,
             input_format_parquet_dictionary_filter_push_down = 0;
select count() from file(currentDatabase() || '_05241_c_i8.parquet', Parquet, 'x UInt32') where x = 100
    settings log_comment = 'hfilter_d_07_i8_u32', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05241_c_i8.parquet', Parquet, 'x UInt32') where x = 100
    settings log_comment = 'hfilter_b_07_i8_u32', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 1,
             input_format_parquet_dictionary_filter_push_down = 0;

-- Non-integer targets that are numerically the identity on the stored space.
select count() from file(currentDatabase() || '_05241_c_u32ts.parquet', Parquet, 'x DateTime')
    where x = toDateTime(3000003500)
    settings log_comment = 'hfilter_d_08_u32_dt', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05241_c_u32ts.parquet', Parquet, 'x DateTime')
    where x = toDateTime(3000003500)
    settings log_comment = 'hfilter_b_08_u32_dt', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 1,
             input_format_parquet_dictionary_filter_push_down = 0;
select count() from file(currentDatabase() || '_05241_c_u32.parquet', Parquet, 'x IPv4')
    where x = toIPv4(3500)
    settings log_comment = 'hfilter_d_09_u32_ipv4', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05241_c_u32.parquet', Parquet, 'x IPv4')
    where x = toIPv4(3500)
    settings log_comment = 'hfilter_b_09_u32_ipv4', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 1,
             input_format_parquet_dictionary_filter_push_down = 0;

-- The date family with a declared width of at most 16 bits, where the cast really is the day-number
-- overload and so numerically the identity.
select count() from file(currentDatabase() || '_05241_c_u8.parquet', Parquet, 'x Date')
    where x = toDate(200)
    settings log_comment = 'hfilter_d_10_u8_date', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05241_c_u8.parquet', Parquet, 'x Date')
    where x = toDate(200)
    settings log_comment = 'hfilter_b_10_u8_date', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 1,
             input_format_parquet_dictionary_filter_push_down = 0;
select count() from file(currentDatabase() || '_05241_c_i8.parquet', Parquet, 'x Date32')
    where x = toDate32(100)
    settings log_comment = 'hfilter_d_11_i8_date32', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05241_c_i8.parquet', Parquet, 'x Date32')
    where x = toDate32(100)
    settings log_comment = 'hfilter_b_11_i8_date32', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 1,
             input_format_parquet_dictionary_filter_push_down = 0;

-- The Nullable and LowCardinality wrappers, which the predicate has to look through rather than refuse.
select count() from file(currentDatabase() || '_05241_c_u32.parquet', Parquet, 'x Nullable(UInt32)')
    where x = 3500
    settings log_comment = 'hfilter_d_12_u32_nu32', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05241_c_u32.parquet', Parquet, 'x Nullable(UInt32)')
    where x = 3500
    settings log_comment = 'hfilter_b_12_u32_nu32', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 1,
             input_format_parquet_dictionary_filter_push_down = 0;
select count() from file(currentDatabase() || '_05241_c_str.parquet', Parquet, 'x LowCardinality(String)')
    where x = '3500'
    settings log_comment = 'hfilter_d_13_str_lcstr', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05241_c_str.parquet', Parquet, 'x LowCardinality(String)')
    where x = '3500'
    settings log_comment = 'hfilter_b_13_str_lcstr', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 1,
             input_format_parquet_dictionary_filter_push_down = 0;

-- The string family: equality of the requested and decoded types, and the one pair that is not equality,
-- an IPv6 read from the FixedString(16) a 16-byte fixed array decodes to.
select count() from file(currentDatabase() || '_05241_c_str.parquet', Parquet, 'x String')
    where x = '3500'
    settings log_comment = 'hfilter_d_14_str_str', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05241_c_str.parquet', Parquet, 'x String')
    where x = '3500'
    settings log_comment = 'hfilter_b_14_str_str', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 1,
             input_format_parquet_dictionary_filter_push_down = 0;
select count() from file(currentDatabase() || '_05241_c_ipv6.parquet', Parquet, 'x IPv6')
    where x = toIPv6('2001:db8::0dac')
    settings log_comment = 'hfilter_d_15_ipv6_ipv6', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05241_c_ipv6.parquet', Parquet, 'x IPv6')
    where x = toIPv6('2001:db8::0dac')
    settings log_comment = 'hfilter_b_15_ipv6_ipv6', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 1,
             input_format_parquet_dictionary_filter_push_down = 0;

-- Int64 and FixedString are the two types ClickHouse writes without any parquet annotation, so these are
-- the only controls that read a column whose width the file itself never declares.
select count() from file(currentDatabase() || '_05241_c_i64.parquet', Parquet, 'x Int64') where x = 3500
    settings log_comment = 'hfilter_d_16_i64_i64', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;
select count() from file(currentDatabase() || '_05241_c_i64.parquet', Parquet, 'x Int64') where x = 3500
    settings log_comment = 'hfilter_b_16_i64_i64', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 1,
             input_format_parquet_dictionary_filter_push_down = 0;
select count() from file(currentDatabase() || '_05241_c_fs8.parquet', Parquet, 'x FixedString(8)')
    where x = '00000200'
    settings log_comment = 'hfilter_d_17_fs8_fs8', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;

-- The same file read with no structure hint at all, where the requested type is the inferred one.
select count() from file(currentDatabase() || '_05241_c_fs8.parquet', Parquet) where x = '00000200'
    settings log_comment = 'hfilter_d_18_fs8_inferred', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 1048576;

-- The same two controls with both hash legs off as well, so that the rows below have a no-pruning
-- baseline to be read against.
select count() from file(currentDatabase() || '_05241_c_u8.parquet', Parquet, 'x UInt8') where x = 200
    settings log_comment = 'hfilter_n_01_u8_u8', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 0;
select count() from file(currentDatabase() || '_05241_c_u32.parquet', Parquet, 'x UInt32') where x = 3500
    settings log_comment = 'hfilter_n_02_u32_u32', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 0;
select count() from file(currentDatabase() || '_05241_c_i64.parquet', Parquet, 'x Int64') where x = 3500
    settings log_comment = 'hfilter_n_03_i64_i64', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 0;
select count() from file(currentDatabase() || '_05241_c_fs8.parquet', Parquet, 'x FixedString(8)')
    where x = '00000200'
    settings log_comment = 'hfilter_n_04_fs8_fs8', input_format_parquet_filter_push_down = 0,
             input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
             input_format_parquet_dictionary_filter_push_down = 0;

-- A correct count alone cannot tell a preserved optimization from a disabled one, so assert that the
-- legs really did skip row groups. `ParquetPrunedRowGroups` counts only the min/max legs, which are off
-- here, so it stays 0 and `read_rows` is the whole evidence: 64 of 256 rows and 1000 of 4000 mean three
-- of the four row groups were skipped, and the `hfilter_n_` rows are the same reads with no hash leg at
-- all. `hfilter_d_` is the dictionary leg, `hfilter_b_` the bloom filter leg.
system flush logs query_log;
select distinct log_comment, ProfileEvents['ParquetPrunedRowGroups'], read_rows
    from system.query_log
    where current_database = currentDatabase() and type = 'QueryFinish' and log_comment like 'hfilter\_%'
    order by log_comment;
