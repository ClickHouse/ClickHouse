set allow_experimental_variant_type = 1;
set use_variant_as_common_type = 1;
set allow_experimental_dynamic_type = 1;
set allow_suspicious_low_cardinality_types = 1;
set session_timezone = 'UTC';

select accurateCastOrDefault(variant, 'UInt32'), multiIf(number % 4 == 0, NULL, number % 4 == 1, number, number % 4 == 2, 'str_' || toString(number), range(number)) as variant from numbers(8);
select accurateCastOrNull(variant, 'UInt32'), multiIf(number % 4 == 0, NULL, number % 4 == 1, number, number % 4 == 2, 'str_' || toString(number), range(number)) as variant from numbers(8);

select accurateCastOrDefault(dynamic, 'UInt32'), multiIf(number % 4 == 0, NULL, number % 4 == 1, number, number % 4 == 2, 'str_' || toString(number), range(number))::Dynamic as dynamic from numbers(8);
select accurateCastOrNull(dynamic, 'UInt32'), multiIf(number % 4 == 0, NULL, number % 4 == 1, number, number % 4 == 2, 'str_' || toString(number), range(number))::Dynamic as dynamic from numbers(8);

drop table if exists t;
create table t (d Dynamic) engine=MergeTree order by tuple();

-- Integer types: signed and unsigned integers (UInt8, UInt16, UInt32, UInt64, UInt128, UInt256, Int8, Int16, Int32, Int64, Int128, Int256)
INSERT INTO t VALUES (-128::Int8), (-127::Int8), (-1::Int8), (0::Int8), (1::Int8), (126::Int8), (127::Int8);
INSERT INTO t VALUES (-128::Int8), (-127::Int8), (-1::Int8), (0::Int8), (1::Int8), (126::Int8), (127::Int8);
INSERT INTO t VALUES (-128::Int8), (-127::Int8), (-1::Int8), (0::Int8), (1::Int8), (126::Int8), (127::Int8);
INSERT INTO t VALUES (-32768::Int16), (-32767::Int16), (-1::Int16), (0::Int16), (1::Int16), (32766::Int16), (32767::Int16);
INSERT INTO t VALUES (-2147483648::Int32), (-2147483647::Int32), (-1::Int32), (0::Int32), (1::Int32), (2147483646::Int32), (2147483647::Int32);
INSERT INTO t VALUES (-9223372036854775808::Int64), (-9223372036854775807::Int64), (-1::Int64), (0::Int64), (1::Int64), (9223372036854775806::Int64), (9223372036854775807::Int64);
INSERT INTO t VALUES (-170141183460469231731687303715884105728::Int128), (-170141183460469231731687303715884105727::Int128), (-1::Int128), (0::Int128), (1::Int128), (170141183460469231731687303715884105726::Int128), (170141183460469231731687303715884105727::Int128);
INSERT INTO t VALUES (-57896044618658097711785492504343953926634992332820282019728792003956564819968::Int256), (-57896044618658097711785492504343953926634992332820282019728792003956564819967::Int256), (-1::Int256), (0::Int256), (1::Int256), (57896044618658097711785492504343953926634992332820282019728792003956564819966::Int256), (57896044618658097711785492504343953926634992332820282019728792003956564819967::Int256);

INSERT INTO t VALUES (0::UInt8), (1::UInt8), (254::UInt8), (255::UInt8);
INSERT INTO t VALUES (0::UInt16), (1::UInt16), (65534::UInt16), (65535::UInt16);
INSERT INTO t VALUES (0::UInt32), (1::UInt32), (4294967294::UInt32), (4294967295::UInt32);
INSERT INTO t VALUES (0::UInt64), (1::UInt64), (18446744073709551614::UInt64), (18446744073709551615::UInt64);
INSERT INTO t VALUES (0::UInt128), (1::UInt128), (340282366920938463463374607431768211454::UInt128), (340282366920938463463374607431768211455::UInt128);
INSERT INTO t VALUES (0::UInt256), (1::UInt256), (115792089237316195423570985008687907853269984665640564039457584007913129639934::UInt256), (115792089237316195423570985008687907853269984665640564039457584007913129639935::UInt256);

-- Floating-point numbers: floats(Float32 and Float64) values
INSERT INTO t VALUES (1.17549435e-38::Float32), (3.40282347e+38::Float32), (-3.40282347e+38::Float32), (-1.17549435e-38::Float32), (1.4e-45::Float32), (-1.4e-45::Float32);
INSERT INTO t VALUES (inf::Float32), (-inf::Float32), (nan::Float32);
INSERT INTO t VALUES (inf::FLOAT(12)), (-inf::FLOAT(12)), (nan::FLOAT(12));
INSERT INTO t VALUES (inf::FLOAT(15,22)), (-inf::FLOAT(15,22)), (nan::FLOAT(15,22));

INSERT INTO t VALUES (1.17549435e-38::Float64), (3.40282347e+38::Float64), (-3.40282347e+38::Float64), (-1.17549435e-38::Float64), (1.4e-45::Float64), (-1.4e-45::Float64);
INSERT INTO t VALUES (2.2250738585072014e-308::Float64), (1.7976931348623157e+308::Float64), (-1.7976931348623157e+308::Float64), (-2.2250738585072014e-308::Float64);
INSERT INTO t VALUES (inf::Float64), (-inf::Float64), (nan::Float64);
INSERT INTO t VALUES (inf::DOUBLE(12)), (-inf::DOUBLE(12)), (nan::DOUBLE(12));
INSERT INTO t VALUES (inf::DOUBLE(15,22)), (-inf::DOUBLE(15,22)), (nan::DOUBLE(15,22));

-- Strings: String and FixedString
INSERT INTO t VALUES ('string'::String), ('1'::FixedString(1)), ('1'::FixedString(2)), ('1'::FixedString(10)); --(''::String),

-- Boolean
INSERT INTO t VALUES ('1'::Bool), (0::Bool);

-- UUID
INSERT INTO t VALUES ('dededdb6-7835-4ce4-8d11-b5de6f2820e9'::UUID);
INSERT INTO t VALUES ('00000000-0000-0000-0000-000000000000'::UUID);

-- LowCardinality
INSERT INTO t VALUES ('1'::LowCardinality(String)), ('1'::LowCardinality(String)), (0::LowCardinality(UInt16));

-- Arrays
INSERT INTO t VALUES ([]::Array(Dynamic)), ([[]]::Array(Array(Dynamic))), ([[[]]]::Array(Array(Array(Dynamic))));

-- Tuple
INSERT INTO t VALUES (()::Tuple(Dynamic)), ((())::Tuple(Tuple(Dynamic))), (((()))::Tuple(Tuple(Tuple(Dynamic))));

-- Map.
INSERT INTO t VALUES (map(11::Dynamic, 'v1'::Dynamic, '22'::Dynamic, 1::Dynamic));

-- SimpleAggregateFunction
INSERT INTO t VALUES ([1,2]::SimpleAggregateFunction(anyLast, Array(Int16)));

-- IPs
INSERT INTO t VALUES (toIPv4('192.168.0.1')), (toIPv6('::1'));

-- Geo
INSERT INTO t VALUES ((1.23, 4.56)::Point), (([(1.23, 4.56)::Point, (2.34, 5.67)::Point])::Ring);
INSERT INTO t VALUES ([[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]]::MultiPolygon);

-- Interval
INSERT INTO t VALUES (interval '1' day), (interval '2' month), (interval '3' year);

-- Nested
INSERT INTO t VALUES ([(1, 'aa'), (2, 'bb')]::Nested(x UInt32, y String));
INSERT INTO t VALUES ([(1, (2, ['aa', 'bb']), [(3, 'cc'), (4, 'dd')]), (5, (6, ['ee', 'ff']), [(7, 'gg'), (8, 'hh')])]::Nested(x UInt32, y Tuple(y1 UInt32, y2 Array(String)), z Nested(z1 UInt32, z2 String)));

optimize table t final;

select distinct toInt8OrDefault(d) as res from t order by res;
select distinct toUInt8OrDefault(d) as res from t order by res;
select distinct toInt16OrDefault(d) as res from t order by res;
select distinct toUInt16OrDefault(d) as res from t order by res;
select distinct toInt32OrDefault(d) as res from t order by res;
select distinct toUInt32OrDefault(d) as res from t order by res;
select distinct toInt64OrDefault(d) as res from t order by res;
select distinct toUInt64OrDefault(d) as res from t order by res;
select distinct toInt128OrDefault(d) as res from t order by res;
select distinct toUInt128OrDefault(d) as res from t order by res;
select distinct toInt256OrDefault(d) as res from t order by res;
select distinct toUInt256OrDefault(d) as res from t order by res;

select distinct toFloat32OrDefault(d) as res from t order by res;
select distinct toFloat64OrDefault(d) as res from t order by res;

select distinct toDecimal32OrDefault(d, 3) as res from t order by res;
select distinct toDecimal64OrDefault(d, 3) as res from t order by res;
select distinct toDecimal128OrDefault(d, 3) as res from t order by res;
select distinct toDecimal256OrDefault(d, 3) as res from t order by res;

select distinct toDateOrDefault(d) as res from t order by res;
select distinct toDate32OrDefault(d) as res from t order by res;
select distinct toDateTimeOrDefault(d) as res from t order by res;

select distinct toIPv4OrDefault(d) as res from t order by res;
select distinct toIPv6OrDefault(d) as res from t order by res;

select distinct toUUIDOrDefault(d) as res from t order by res;

drop table t;

