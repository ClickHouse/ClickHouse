-- Tags: long, no-fasttest, no-parallel, no-tsan, no-msan, no-asan

set output_format_parquet_use_custom_encoder = 1;
set output_format_parquet_row_group_size = 1000;
set output_format_parquet_data_page_size = 800;
set output_format_parquet_batch_size = 100;
set output_format_parquet_row_group_size_bytes = 1000000000;
set engine_file_truncate_on_insert = 1;
set allow_suspicious_low_cardinality_types = 1;
set output_format_parquet_enum_as_byte_array=0;

-- Write random data to parquet file, then read from it and check that it matches what we wrote.
-- Do this for all kinds of data types: primitive, Nullable(primitive), Array(primitive),
-- Array(Nullable(primitive)), Array(Array(primitive)), Map(primitive, primitive), etc.

drop table if exists basic_types_02735;
create temporary table basic_types_02735 as select * from generateRandom('
    u8 UInt8,
    u16 UInt16,
    u32 UInt32,
    u64 UInt64,
    i8 Int8,
    i16 Int16,
    i32 Int32,
    i64 Int64,
    date Date,
    date32 Date32,
    datetime DateTime,
    datetime64 DateTime64,
    enum8 Enum8(''x'' = 1, ''y'' = 2, ''z'' = 3),
    enum16 Enum16(''xx'' = 1000, ''yy'' = 2000, ''zz'' = 3000),
    float32 Float32,
    float64 Float64,
    str String,
    fstr FixedString(12),
    u128 UInt128,
    u256 UInt256,
    i128 Int128,
    i256 Int256,
    decimal32 Decimal32(3),
    decimal64 Decimal64(10),
    decimal128 Decimal128(20),
    decimal256 Decimal256(40),
    ipv4 IPv4,
    ipv6 IPv6') limit 1011;
insert into function file(basic_types_02735.parquet) select * from basic_types_02735 settings output_format_parquet_datetime_as_uint32 = 1;
desc file(basic_types_02735.parquet);
select (select sum(cityHash64(*)) from basic_types_02735) - (select sum(cityHash64(*)) from file(basic_types_02735.parquet));
drop table basic_types_02735;

-- DateTime values don't roundtrip (without output_format_parquet_datetime_as_uint32) because we
-- write them as DateTime64(3) (the closest type supported by Parquet).
drop table if exists datetime_02735;
create temporary table datetime_02735 as select * from generateRandom('datetime DateTime') limit 1011;
insert into function file(datetime_02735.parquet) select * from datetime_02735;
desc file(datetime_02735.parquet);
select (select sum(cityHash64(toDateTime64(datetime, 3))) from datetime_02735) - (select sum(cityHash64(*)) from file(datetime_02735.parquet));
select (select sum(cityHash64(*)) from datetime_02735) - (select sum(cityHash64(*)) from file(datetime_02735.parquet, Parquet, 'datetime DateTime'));
drop table datetime_02735;

drop table if exists nullables_02735;
create temporary table nullables_02735 as select * from generateRandom('
    u16 Nullable(UInt16),
    i64 Nullable(Int64),
    datetime64 Nullable(DateTime64),
    enum8 Nullable(Enum8(''x'' = 1, ''y'' = 2, ''z'' = 3)),
    float64 Nullable(Float64),
    str Nullable(String),
    fstr Nullable(FixedString(12)),
    i256 Nullable(Int256),
    decimal256 Nullable(Decimal256(40)),
    ipv6 Nullable(IPv6)') limit 1000;
insert into function file(nullables_02735.parquet) select * from nullables_02735;
select (select sum(cityHash64(*)) from nullables_02735) - (select sum(cityHash64(*)) from file(nullables_02735.parquet));
drop table nullables_02735;


-- TODO: When cityHash64() fully supports Nullable: https://github.com/ClickHouse/ClickHouse/pull/58754
--       the next two blocks can be simplified: arrays_out_02735 intermediate table is not needed,
--       a.csv and b.csv are not needed.

drop table if exists arrays_02735;
drop table if exists arrays_out_02735;
create table arrays_02735 engine = Memory as select * from generateRandom('
    u32 Array(UInt32),
    i8 Array(Int8),
    datetime Array(DateTime),
    enum16 Array(Enum16(''xx'' = 1000, ''yy'' = 2000, ''zz'' = 3000)),
    float32 Array(Float32),
    str Array(String),
    fstr Array(FixedString(12)),
    u128 Array(UInt128),
    decimal64 Array(Decimal64(10)),
    ipv4 Array(IPv4),
    msi Map(String, Int16),
    tup Tuple(FixedString(3), Array(String), Map(Int8, Date))') limit 1000;
insert into function file(arrays_02735.parquet) select * from arrays_02735;
create temporary table arrays_out_02735 as arrays_02735;
insert into arrays_out_02735 select * from file(arrays_02735.parquet);
select (select sum(cityHash64(*)) from arrays_02735) - (select sum(cityHash64(*)) from arrays_out_02735);
--select (select sum(cityHash64(*)) from arrays_02735) -
--       (select sum(cityHash64(u32, i8, datetime, enum16, float32, str, fstr, arrayMap(x->reinterpret(x, 'UInt128'), u128), decimal64, ipv4, msi, tup)) from file(arrays_02735.parquet));
drop table arrays_02735;
drop table arrays_out_02735;


drop table if exists madness_02735;
create temporary table madness_02735 as select * from generateRandom('
    aa Array(Array(UInt32)),
    aaa Array(Array(Array(UInt32))),
    an Array(Nullable(String)),
    aan Array(Array(Nullable(FixedString(10)))),
    l LowCardinality(String),
    ln LowCardinality(Nullable(FixedString(11))),
    al Array(LowCardinality(UInt128)),
    aaln Array(Array(LowCardinality(Nullable(String)))),
    mln Map(LowCardinality(String), Nullable(Int8)),
    t Tuple(Map(FixedString(5), Tuple(Array(UInt16), Nullable(UInt16), Array(Tuple(Int8, Decimal64(10))))), Tuple(kitchen UInt64, sink String)),
    n Nested(hello UInt64, world Tuple(first String, second FixedString(1)))
    ') limit 1000;
insert into function file(madness_02735.parquet) select * from madness_02735;
insert into function file(a.csv) select * from madness_02735 order by tuple(*);
insert into function file(b.csv) select aa, aaa, an, aan, l, ln, arrayMap(x->reinterpret(x, 'UInt128'), al) as al_, aaln, mln, t, n.hello, n.world from file(madness_02735.parquet) order by tuple(aa, aaa, an, aan, l, ln, al_, aaln, mln, t, n.hello, n.world);
select (select sum(cityHash64(*)) from file(a.csv, LineAsString)) - (select sum(cityHash64(*)) from file(b.csv, LineAsString));
--select (select sum(cityHash64(*)) from madness_02735) -
--       (select sum(cityHash64(aa, aaa, an, aan, l, ln, map(x->reinterpret(x, 'UInt128'), al), aaln, mln, t, n.hello, n.world)) from file(madness_02735.parquet));
drop table madness_02735;


-- Merging input blocks into bigger row groups.
insert into function file(squash_02735.parquet) select '012345' union all select '543210' settings max_block_size = 1;
select num_columns, num_rows, num_row_groups from file(squash_02735.parquet, ParquetMetadata);

-- Row group size limit in bytes.
insert into function file(row_group_bytes_02735.parquet) select '012345' union all select '543210' settings max_block_size = 1, output_format_parquet_row_group_size_bytes = 5;
select num_columns, num_rows, num_row_groups from file(row_group_bytes_02735.parquet, ParquetMetadata);

-- Row group size limit in rows.
insert into function file(tiny_row_groups_02735.parquet) select * from numbers(3) settings output_format_parquet_row_group_size = 1;
select num_columns, num_rows, num_row_groups from file(tiny_row_groups_02735.parquet, ParquetMetadata);

-- 1M unique 8-byte values should exceed dictionary_size_limit (1 MB).
insert into function file(big_column_chunk_02735.parquet) select number from numbers(1000000) settings output_format_parquet_row_group_size = 1000000;
select num_columns, num_rows, num_row_groups from file(big_column_chunk_02735.parquet, ParquetMetadata);
select sum(cityHash64(number)) from file(big_column_chunk_02735.parquet);

-- Check statistics: signed vs unsigned, null count. Use enough rows to produce multiple pages.
insert into function file(statistics_02735.parquet) select 100 + number%200 as a, toUInt32(number * 3000) as u, toInt32(number * 3000) as i, if(number % 10 == 9, toString(number), null) as s from numbers(1000000) settings output_format_parquet_row_group_size = 1000000;
select num_columns, num_rows, num_row_groups from file(statistics_02735.parquet, ParquetMetadata);
select tupleElement(c, 'statistics') from file(statistics_02735.parquet, ParquetMetadata) array join tupleElement(row_groups[1], 'columns') as c;

-- Statistics string length limit (max_statistics_size).
insert into function file(long_string_02735.parquet) select toString(range(number * 2000)) from numbers(2);
select tupleElement(tupleElement(row_groups[1], 'columns'), 'statistics') from file(long_string_02735.parquet, ParquetMetadata);

-- Compression setting.
insert into function file(compressed_02735.parquet) select concat('aaaaaaaaaaaaaaaa', toString(number)) as s from numbers(1000) settings output_format_parquet_row_group_size = 10000, output_format_parquet_compression_method='zstd';
select total_compressed_size < 10000, total_uncompressed_size > 15000 from file(compressed_02735.parquet, ParquetMetadata);
insert into function file(compressed_02735.parquet) select concat('aaaaaaaaaaaaaaaa', toString(number)) as s from numbers(1000) settings output_format_parquet_row_group_size = 10000, output_format_parquet_compression_method='none';
select total_compressed_size < 10000, total_uncompressed_size > 15000 from file(compressed_02735.parquet, ParquetMetadata);
insert into function file(compressed_02735.parquet) select if(number%3==1, NULL, 42) as x from numbers(70) settings output_format_parquet_compression_method='zstd';
select sum(cityHash64(*)) from file(compressed_02735.parquet);

-- Single-threaded encoding and Arrow encoder.
drop table if exists other_encoders_02735;
create temporary table other_encoders_02735 as select number, number*2 from numbers(10000);
insert into function file(single_thread_02735.parquet) select * from other_encoders_02735 settings max_threads = 1;
select sum(cityHash64(*)) from file(single_thread_02735.parquet);
insert into function file(arrow_02735.parquet) select * from other_encoders_02735 settings output_format_parquet_use_custom_encoder = 0;
select sum(cityHash64(*)) from file(arrow_02735.parquet);

-- String -> binary vs string; FixedString -> fixed-length-binary vs binary vs string.
insert into function file(strings1_02735.parquet) select 'never', toFixedString('gonna', 5) settings output_format_parquet_string_as_string = 1, output_format_parquet_fixed_string_as_fixed_byte_array = 1;
select columns.5, columns.6 from file(strings1_02735.parquet, ParquetMetadata) array join columns;
insert into function file(strings2_02735.parquet) select 'give', toFixedString('you', 3) settings output_format_parquet_string_as_string = 0, output_format_parquet_fixed_string_as_fixed_byte_array = 0;
select columns.5, columns.6 from file(strings2_02735.parquet, ParquetMetadata) array join columns;
insert into function file(strings3_02735.parquet) select toFixedString('up', 2) settings output_format_parquet_string_as_string = 1, output_format_parquet_fixed_string_as_fixed_byte_array = 0;
select columns.5, columns.6 from file(strings3_02735.parquet, ParquetMetadata) array join columns;
select * from file(strings1_02735.parquet);
select * from file(strings2_02735.parquet);
select * from file(strings3_02735.parquet);

-- DateTime64 with different units.
insert into function file(datetime64_02735.parquet) select
    toDateTime64(number / 1e3, 3) as ms,
    toDateTime64(number / 1e6, 6) as us,
    toDateTime64(number / 1e9, 9) as ns,
    toDateTime64(number / 1e2, 2) as cs,
    toDateTime64(number, 0) as s,
    toDateTime64(number / 1e7, 7) as dus
    from numbers(2000);
desc file(datetime64_02735.parquet);
select sum(cityHash64(*)) from file(datetime64_02735.parquet);

insert into function file(date_as_uint16.parquet) select toDate('2025-08-12') as d settings output_format_parquet_date_as_uint16 = 1;
select * from file(date_as_uint16.parquet);
desc file(date_as_uint16.parquet);
