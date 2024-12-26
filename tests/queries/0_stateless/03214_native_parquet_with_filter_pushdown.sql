-- {echoOn}
set output_format_parquet_use_custom_encoder = 0;
set output_format_parquet_row_group_size = 1000;
set output_format_parquet_data_page_size = 800;
set output_format_parquet_batch_size = 100;
set output_format_parquet_row_group_size_bytes = 1000000000;
set engine_file_truncate_on_insert=1;
set allow_suspicious_low_cardinality_types=1;

-- only scan
-- primitive types
drop table if exists test_primitive_scan;
create temporary table test_primitive_scan as select * from generateRandom('
b BOOL, i8 Int8, i16 Int16, i32 Int32, i64 Int64, ui8 UInt8, ui16 UInt16, ui32 UInt32, ui64 UInt64, f Float32, d Float64, date DATE, dt DateTime, d32 DATE32, dt64 DateTime64, s String, fs FixedString(4), dec32 DECIMAL32(9), dec64 DECIMAL64(18), dec128 DECIMAL128(32), dec256 DECIMAL256(64)') limit 2000;
insert into function file(test_primitive_scan.parquet) select * from test_primitive_scan;
--diff:
select * from file(test_primitive_scan.parquet) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) settings input_format_parquet_use_native_reader=false;

--nullable
drop table if exists test_nullable_primitive_scan;
create temporary table test_nullable_primitive_scan as select * from generateRandom('
b Nullable(BOOL), i8 Nullable(Int8), i16 Nullable(Int16), i32 Nullable(Int32), i64 Nullable(Int64), ui8 Nullable(UInt8), ui16 Nullable(UInt16), ui32 Nullable(UInt32), ui64 Nullable(UInt64), f Nullable(Float32), d Nullable(Float64), date Nullable(DATE), dt Nullable(DateTime), d32 Nullable(DATE32), dt64 Nullable(DateTime64), s Nullable(String), fs Nullable(FixedString(4)), dec32 Nullable(DECIMAL32(9)), dec64 Nullable(DECIMAL64(18)), dec128 Nullable(DECIMAL128(32)), dec256 Nullable(DECIMAL256(64))') limit 2000;
insert into function file(test_nullable_primitive_scan.parquet) select * from test_nullable_primitive_scan;
--diff:
select * from file(test_nullable_primitive_scan.parquet) settings input_format_parquet_use_native_reader=true except select * from file(test_nullable_primitive_scan.parquet) settings input_format_parquet_use_native_reader=false;

-- nested types
drop table if exists test_nested_scan;
create temporary table test_nested_scan as select * from generateRandom('
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
    tup Tuple(FixedString(3), Array(String), Map(Int8, Date))') limit 2000;
insert into function file(test_nested_scan.parquet) select * from test_nested_scan;
--diff:
select * from file(test_nested_scan.parquet) settings input_format_parquet_use_native_reader=true except select * from file(test_nested_scan.parquet) settings input_format_parquet_use_native_reader=false;


drop table if exists test_nested_scan_with_nullable;
create temporary table test_nested_scan_with_nullable as select * from generateRandom('
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
    n Nested(hello UInt64, world Tuple(first String, second FixedString(1)))') limit 2000;
insert into function file(test_nested_scan_with_nullable.parquet) select * from test_nested_scan_with_nullable;
--diff:
select * from file(test_nested_scan_with_nullable.parquet) settings input_format_parquet_use_native_reader=true except select * from file(test_nested_scan_with_nullable.parquet) settings input_format_parquet_use_native_reader=false;

-- with filter pushdown
-- primitive types
drop table if exists test_primitive_scan;
create temporary table test_primitive_scan as select * from generateRandom('
b BOOL, i8 Int8, i16 Int16, i32 Int32, i64 Int64, ui8 UInt8, ui16 UInt16, ui32 UInt32, ui64 UInt64, f Float32, d Float64, date DATE, dt DateTime, d32 DATE32, dt64 DateTime64, s String, fs FixedString(4), dec32 DECIMAL32(9), dec64 DECIMAL64(18), dec128 DECIMAL128(32), dec256 DECIMAL256(64)') limit 50000;
insert into function file(test_primitive_scan.parquet) select * from test_primitive_scan;

select * from file(test_primitive_scan.parquet) where b = false settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where b = false settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where b in (false) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where b in (false) settings input_format_parquet_use_native_reader=false;

select * from file(test_primitive_scan.parquet) where i8 > 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i8 > 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i8 >= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i8 >= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i8 < 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i8 < 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i8 <= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i8 <= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i8 = 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i8 = 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i8 in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i8 in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i8 not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i8 not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;

select * from file(test_primitive_scan.parquet) where i16 > 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i16 > 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i16 >= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i16 >= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i16 < 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i16 < 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i16 <= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i16 <= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i16 = 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i16 = 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i16 in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i16 in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i16 not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i16 not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;

select * from file(test_primitive_scan.parquet) where i32 > 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i32 > 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i32 >= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i32 >= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i32 < 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i32 < 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i32 <= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i32 <= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i32 = 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i32 = 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i32 in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i32 in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i32 not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i32 not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;

select * from file(test_primitive_scan.parquet) where i64 > 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i64 > 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i64 >= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i64 >= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i64 < 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i64 < 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i64 <= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i64 <= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i64 = 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i64 = 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i64 in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i64 in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where i64 not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where i64 not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;

select * from file(test_primitive_scan.parquet) where ui8 > 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui8 > 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui8 >= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui8 >= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui8 < 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui8 < 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui8 <= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui8 <= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui8 = 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui8 = 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui8 in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui8 in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui8 not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui8 not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;

select * from file(test_primitive_scan.parquet) where ui16 > 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui16 > 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui16 >= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui16 >= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui16 < 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui16 < 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui16 <= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui16 <= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui16 = 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui16 = 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui16 in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui16 in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui16 not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui16 not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;

select * from file(test_primitive_scan.parquet) where ui32 > 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui32 > 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui32 >= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui32 >= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui32 < 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui32 < 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui32 <= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui32 <= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui32 = 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui32 = 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui32 in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui32 in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui32 not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui32 not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;

select * from file(test_primitive_scan.parquet) where ui64 > 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui64 > 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui64 >= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui64 >= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui64 < 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui64 < 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui64 <= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui64 <= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui64 = 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui64 = 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui64 in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui64 in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where ui64 not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where ui64 not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;

select * from file(test_primitive_scan.parquet) where f > 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where f > 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where f >= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where f >= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where f < 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where f < 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where f <= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where f <= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where f = 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where f = 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where f in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where f in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where f not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where f not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;

select * from file(test_primitive_scan.parquet) where d > 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where d > 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where d >= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where d >= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where d < 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where d < 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where d <= 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where d <= 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where d = 20 settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where d = 20 settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where d in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where d in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;
select * from file(test_primitive_scan.parquet) where d not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=true except select * from file(test_primitive_scan.parquet) where d not in (15,16,17,18,19) settings input_format_parquet_use_native_reader=false;

-- nested types