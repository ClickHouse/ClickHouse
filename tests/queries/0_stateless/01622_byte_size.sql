--
-- byteSize
--
select '';
select '# byteSize';

-- numbers #0 --
select '';
select 'byteSize for numbers #0';
drop table if exists test_byte_size_number0;
create table test_byte_size_number0
(
    key Int32,
    u8 UInt8,
    u16 UInt16,
    u32 UInt32,
    u64 UInt64,
    u256 UInt256,
    i8 Int8,
    i16 Int16,
    i32 Int32,
    i64 Int64,
    i128 Int128,
    i256 Int256,
    f32 Float32,
    f64 Float64
) engine MergeTree order by key;

insert into test_byte_size_number0 values(1, 8, 16, 32, 64, 256, -8, -16, -32, -64, -128, -256, 32.32, 64.64);
insert into test_byte_size_number0 values(2, 8, 16, 32, 64, 256, -8, -16, -32, -64, -128, -256, 32.32, 64.64);

select key, toTypeName(u8), byteSize(u8), toTypeName(u16), byteSize(u16), toTypeName(u32), byteSize(u32), toTypeName(u64), byteSize(u64), toTypeName(u256), byteSize(u256) from test_byte_size_number0 order by key;
select key, toTypeName(i8), byteSize(i8), toTypeName(i16), byteSize(i16), toTypeName(i32), byteSize(i32), toTypeName(i64), byteSize(i64), toTypeName(i128), byteSize(i128), toTypeName(u256), byteSize(u256) from test_byte_size_number0 order by key;
select key, toTypeName(f32), byteSize(f32), toTypeName(f64), byteSize(f64) from test_byte_size_number0 order by key;

drop table if exists test_byte_size_number0;


-- numbers #1 --
select '';
select 'byteSize for numbers #1';
drop table if exists test_byte_size_number1;
create table test_byte_size_number1
(
    key Int32,
    date Date,
    dt DateTime,
    dt64 DateTime64(3),
    en8 Enum8('a'=1, 'b'=2, 'c'=3, 'd'=4),
    en16 Enum16('c'=100, 'l'=101, 'i'=102, 'ck'=103, 'h'=104, 'o'=105, 'u'=106, 's'=107, 'e'=108),
    dec32 Decimal32(4),
    dec64 Decimal64(8),
    dec128 Decimal128(16),
    dec256 Decimal256(16),
    uuid UUID
) engine MergeTree order by key;

insert into test_byte_size_number1 values(1, '2020-01-01', '2020-01-01 01:02:03', '2020-02-02 01:02:03', 'a', 'ck', 32.32, 64.64, 128.128, 256.256, generateUUIDv4());
insert into test_byte_size_number1 values(2, '2020-01-01', '2020-01-01 01:02:03', '2020-02-02 01:02:03', 'a', 'ck', 32.32, 64.64, 128.128, 256.256, generateUUIDv4());

select key, byteSize(*), toTypeName(date), byteSize(date), toTypeName(dt), byteSize(dt), toTypeName(dt64), byteSize(dt64), toTypeName(uuid), byteSize(uuid) from test_byte_size_number1 order by key;

drop table if exists test_byte_size_number1;


-- constant numbers --
select '';
select 'byteSize for constants';
select 0x1, byteSize(0x1), 0x100, byteSize(0x100), 0x10000, byteSize(0x10000), 0x100000000, byteSize(0x100000000), 0.5, byteSize(0.5), 1e-10, byteSize(1e-10);
select toDate('2020-01-01'), byteSize(toDate('2020-01-01')), toDateTime('2020-01-01 01:02:03'), byteSize(toDateTime('2020-01-01 01:02:03')), toDateTime64('2020-01-01 01:02:03',3), byteSize(toDateTime64('2020-01-01 01:02:03',3));
select toTypeName(generateUUIDv4()), byteSize(generateUUIDv4());


-- strings --
select '';
select 'byteSize for strings';
drop table if exists test_byte_size_string;
create table test_byte_size_string
(
    key Int32,
    str1 String,
    str2 String,
    fstr1 FixedString(8),
    fstr2 FixedString(8)
) engine MergeTree order by key;

insert into test_byte_size_string values(1, '', 'a', '', 'abcde');
insert into test_byte_size_string values(2, 'abced', '', 'abcde', '');

select key, byteSize(*), str1, byteSize(str1), str2, byteSize(str2), fstr1, byteSize(fstr1), fstr2, byteSize(fstr2) from test_byte_size_string order by key;
select 'constants: ', '', byteSize(''), 'a', byteSize('a'), 'abcde', byteSize('abcde');

drop table if exists test_byte_size_string;


-- simple arrays --
drop table if exists test_byte_size_array;
create table test_byte_size_array
(
    key Int32,
    uints8 Array(UInt8),
    ints8 Array(Int8),
    ints32 Array(Int32),
    floats32 Array(Float32),
    decs32 Array(Decimal32(4)),
    dates Array(Date),
    uuids Array(UUID)
) engine MergeTree order by key;

insert into test_byte_size_array values(1, [], [], [], [], [], [], []);
insert into test_byte_size_array values(2, [1], [-1], [256], [1.1], [1.1], ['2020-01-01'], ['61f0c404-5cb3-11e7-907b-a6006ad3dba0']);
insert into test_byte_size_array values(3, [1,1], [-1,-1], [256,256], [1.1,1.1], [1.1,1.1], ['2020-01-01','2020-01-01'], ['61f0c404-5cb3-11e7-907b-a6006ad3dba0','61f0c404-5cb3-11e7-907b-a6006ad3dba0']);
insert into test_byte_size_array values(4, [1,1,1], [-1,-1,-1], [256,256,256], [1.1,1.1,1.1], [1.1,1.1,1.1], ['2020-01-01','2020-01-01','2020-01-01'], ['61f0c404-5cb3-11e7-907b-a6006ad3dba0','61f0c404-5cb3-11e7-907b-a6006ad3dba0','61f0c404-5cb3-11e7-907b-a6006ad3dba0']);

select '';
select 'byteSize for simple array';
select key, byteSize(*), uints8, byteSize(uints8), ints8, byteSize(ints8), ints32, byteSize(ints32), floats32, byteSize(floats32), decs32, byteSize(decs32), dates, byteSize(dates), uuids, byteSize(uuids) from test_byte_size_array order by key;

select 'constants:', [], byteSize([]), [1,1], byteSize([1,1]), [-1,-1], byteSize([-1,-1]), toTypeName([256,256]), byteSize([256,256]), toTypeName([1.1,1.1]), byteSize([1.1,1.1]);
select 'constants:', [toDecimal32(1.1,4),toDecimal32(1.1,4)], byteSize([toDecimal32(1.1,4),toDecimal32(1.1,4)]), [toDate('2020-01-01'),toDate('2020-01-01')], byteSize([toDate('2020-01-01'),toDate('2020-01-01')]);
select 'constants:', [toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'),toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')], byteSize([toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'),toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')]);

drop table if exists test_byte_size_array;


-- complex arrays --
drop table if exists test_byte_size_complex_array;
create table test_byte_size_complex_array
(
    key Int32,
    ints Array(Int32),
    int_ints Array(Array(Int32)),
    strs Array(String),
    str_strs Array(Array(String))
) engine MergeTree order by key;

insert into test_byte_size_complex_array values(1, [], [[]], [], [[]]);
insert into test_byte_size_complex_array values(2, [1,2], [[], [1,2]], [''], [[], ['']]);
insert into test_byte_size_complex_array values(3, [0,256], [[], [1,2], [0,256]], ['','a'], [[], [''], ['','a']]);
insert into test_byte_size_complex_array values(4, [256,65536], [[], [1,2], [0,256], [256,65536]], ['','a','abced'], [[], [''], ['','a'], ['','a','abced']]);

select '';
select 'byteSize for int array of arrays';
select key, byteSize(*), ints, byteSize(ints), int_ints, byteSize(int_ints) from test_byte_size_complex_array order by key;
select 'constants:', [[], [1,2], [0,0x10000]],toTypeName([[], [1,2], [0,0x10000]]), byteSize([[], [1,2], [0,0x10000]]);

select '';
select 'byteSize for string array of arrays';
-- select key, byteSize(*), strs, byteSize(strs), str_strs, byteSize(str_strs) from test_byte_size_complex_array order by key;
select key, byteSize(*), strs, byteSize(strs), str_strs, byteSize(str_strs) from test_byte_size_complex_array order by key;
select 'constants:', [[], [''], ['','a']], byteSize([[], [''], ['','a']]);

drop table if exists test_byte_size_complex_array;


-- others --
drop table if exists test_byte_size_other;
create table test_byte_size_other
(
    key Int32,
    opt_int32 Nullable(Int32),
    opt_str Nullable(String),
    tuple Tuple(Int32, Nullable(String)),
    strings LowCardinality(String)
) engine MergeTree order by key;

insert into test_byte_size_other values(1, NULL, NULL, tuple(1, NULL), '');
insert into test_byte_size_other values(2, 1, 'a', tuple(1, 'a'), 'a');
insert into test_byte_size_other values(3, 256, 'abcde', tuple(256, 'abcde'), 'abcde');

select '';
select 'byteSize for others: Nullable, Tuple, LowCardinality';
select key, byteSize(*), opt_int32, byteSize(opt_int32), opt_str, byteSize(opt_str), tuple, byteSize(tuple), strings, byteSize(strings) from test_byte_size_other order by key;
select 'constants:', NULL, byteSize(NULL), tuple(0x10000, NULL), byteSize(tuple(0x10000, NULL)), tuple(0x10000, toNullable('a')), byteSize(tuple(0x10000, toNullable('a')));
select 'constants:', toLowCardinality('abced'),toTypeName(toLowCardinality('abced')), byteSize(toLowCardinality('abced'));

drop table if exists test_byte_size_other;


-- more complex fields --
drop table if exists test_byte_size_more_complex;
create table test_byte_size_more_complex
(
    key Int32,
    complex1 Array(Tuple(Nullable(FixedString(4)), Array(Tuple(Nullable(String), String))))
) engine MergeTree order by key;

insert into test_byte_size_more_complex values(1, []);
insert into test_byte_size_more_complex values(2, [tuple(NULL, [])]);
insert into test_byte_size_more_complex values(3, [tuple('a', [])]);
insert into test_byte_size_more_complex values(4, [tuple('a', [tuple(NULL, 'a')])]);
insert into test_byte_size_more_complex values(5, [tuple('a', [tuple(NULL, 'a'), tuple(NULL, 'a')])]);
insert into test_byte_size_more_complex values(6, [tuple(NULL, []), tuple('a', []), tuple('a', [tuple(NULL, 'a')]), tuple('a', [tuple(NULL, 'a'), tuple(NULL, 'a')])]);

select '';
select 'byteSize for complex fields';
select key, byteSize(*), complex1, byteSize(complex1) from test_byte_size_more_complex order by key;
select 'constants:', tuple(NULL, []), byteSize(tuple(NULL, [])), tuple(toNullable(toFixedString('a',4)), []), byteSize(tuple(toNullable(toFixedString('a',4)), [])), tuple(toNullable(toFixedString('a',4)), [tuple(NULL, 'a')]), byteSize(tuple(toNullable(toFixedString('a',4)), [tuple(NULL, 'a')])), tuple(toFixedString('a',4), [tuple(NULL, 'a'), tuple(NULL, 'a')]), byteSize(tuple(toNullable(toFixedString('a',4)), [tuple(NULL, 'a'), tuple(NULL, 'a')]));
select 'constants:', [tuple(NULL, []), tuple(toNullable(toFixedString('a',4)), []), tuple(toNullable(toFixedString('a',4)), [tuple(NULL, 'a')]), tuple(toNullable(toFixedString('a',4)), [tuple(NULL, 'a'), tuple(NULL, 'a')])];
select 'constants:', toTypeName([tuple(NULL, []), tuple(toNullable(toFixedString('a',4)), []), tuple(toNullable(toFixedString('a',4)), [tuple(NULL, 'a')]), tuple(toNullable(toFixedString('a',4)), [tuple(NULL, 'a'), tuple(NULL, 'a')])]);
select 'constants:', byteSize([tuple(NULL, []), tuple(toNullable(toFixedString('a',4)), []), tuple(toNullable(toFixedString('a',4)), [tuple(NULL, 'a')]), tuple(toNullable(toFixedString('a',4)), [tuple(NULL, 'a'), tuple(NULL, 'a')])]);

drop table if exists test_byte_size_more_complex;
