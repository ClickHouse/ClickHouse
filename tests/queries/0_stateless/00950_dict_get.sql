-- Tags: no-parallel

-- Must use `system` database and these tables - they're configured in tests/*_dictionary.xml
use system;
drop table if exists ints;
drop table if exists strings;
drop table if exists decimals;

create table ints (key UInt64, i8 Int8, i16 Int16, i32 Int32, i64 Int64, u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64) Engine = Memory;
create table strings (key UInt64, str String) Engine = Memory;
create table decimals (key UInt64, d32 Decimal32(4), d64 Decimal64(6), d128 Decimal128(1)) Engine = Memory;

insert into ints values (1, 1, 1, 1, 1, 1, 1, 1, 1);
insert into strings values (1, '1');
insert into decimals values (1, 1, 1, 1);

select 'dictGet', 'flat_ints' as dict_name, toUInt64(1) as k,
    dictGet(dict_name, 'i8', k),
    dictGet(dict_name, 'i16', k),
    dictGet(dict_name, 'i32', k),
    dictGet(dict_name, 'i64', k),
    dictGet(dict_name, 'u8', k),
    dictGet(dict_name, 'u16', k),
    dictGet(dict_name, 'u32', k),
    dictGet(dict_name, 'u64', k),
    dictGet(dict_name, ('i8', 'i16', 'i32'), k);
select 'dictGetOrDefault', 'flat_ints' as dict_name, toUInt64(1) as k,
    dictGetOrDefault(dict_name, 'i8', k, toInt8(42)),
    dictGetOrDefault(dict_name, 'i16', k, toInt16(42)),
    dictGetOrDefault(dict_name, 'i32', k, toInt32(42)),
    dictGetOrDefault(dict_name, 'i64', k, toInt64(42)),
    dictGetOrDefault(dict_name, 'u8', k, toUInt8(42)),
    dictGetOrDefault(dict_name, 'u16', k, toUInt16(42)),
    dictGetOrDefault(dict_name, 'u32', k, toUInt32(42)),
    dictGetOrDefault(dict_name, 'u64', k, toUInt64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (toInt8(42), toInt16(42), toInt32(42)));
select 'dictGetOrDefault', 'flat_ints' as dict_name, toUInt64(0) as k,
    dictGetOrDefault(dict_name, 'i8', k, toInt8(42)),
    dictGetOrDefault(dict_name, 'i16', k, toInt16(42)),
    dictGetOrDefault(dict_name, 'i32', k, toInt32(42)),
    dictGetOrDefault(dict_name, 'i64', k, toInt64(42)),
    dictGetOrDefault(dict_name, 'u8', k, toUInt8(42)),
    dictGetOrDefault(dict_name, 'u16', k, toUInt16(42)),
    dictGetOrDefault(dict_name, 'u32', k, toUInt32(42)),
    dictGetOrDefault(dict_name, 'u64', k, toUInt64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (toInt8(42), toInt16(42), toInt32(42)));

select 'dictGet', 'hashed_ints' as dict_name, toUInt64(1) as k,
    dictGet(dict_name, 'i8', k),
    dictGet(dict_name, 'i16', k),
    dictGet(dict_name, 'i32', k),
    dictGet(dict_name, 'i64', k),
    dictGet(dict_name, 'u8', k),
    dictGet(dict_name, 'u16', k),
    dictGet(dict_name, 'u32', k),
    dictGet(dict_name, 'u64', k),
    dictGet(dict_name, ('i8', 'i16', 'i32'), k);
select 'dictGetOrDefault', 'hashed_ints' as dict_name, toUInt64(1) as k,
    dictGetOrDefault(dict_name, 'i8', k, toInt8(42)),
    dictGetOrDefault(dict_name, 'i16', k, toInt16(42)),
    dictGetOrDefault(dict_name, 'i32', k, toInt32(42)),
    dictGetOrDefault(dict_name, 'i64', k, toInt64(42)),
    dictGetOrDefault(dict_name, 'u8', k, toUInt8(42)),
    dictGetOrDefault(dict_name, 'u16', k, toUInt16(42)),
    dictGetOrDefault(dict_name, 'u32', k, toUInt32(42)),
    dictGetOrDefault(dict_name, 'u64', k, toUInt64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (toInt8(42), toInt16(42), toInt32(42)));
select 'dictGetOrDefault', 'hashed_ints' as dict_name, toUInt64(0) as k,
    dictGetOrDefault(dict_name, 'i8', k, toInt8(42)),
    dictGetOrDefault(dict_name, 'i16', k, toInt16(42)),
    dictGetOrDefault(dict_name, 'i32', k, toInt32(42)),
    dictGetOrDefault(dict_name, 'i64', k, toInt64(42)),
    dictGetOrDefault(dict_name, 'u8', k, toUInt8(42)),
    dictGetOrDefault(dict_name, 'u16', k, toUInt16(42)),
    dictGetOrDefault(dict_name, 'u32', k, toUInt32(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (toInt8(42), toInt16(42), toInt32(42)));

select 'dictGet', 'hashed_sparse_ints' as dict_name, toUInt64(1) as k,
    dictGet(dict_name, 'i8', k),
    dictGet(dict_name, 'i16', k),
    dictGet(dict_name, 'i32', k),
    dictGet(dict_name, 'i64', k),
    dictGet(dict_name, 'u8', k),
    dictGet(dict_name, 'u16', k),
    dictGet(dict_name, 'u32', k),
    dictGet(dict_name, 'u64', k),
    dictGet(dict_name, ('i8', 'i16', 'i32'), k);
select 'dictGetOrDefault', 'hashed_sparse_ints' as dict_name, toUInt64(1) as k,
    dictGetOrDefault(dict_name, 'i8', k, toInt8(42)),
    dictGetOrDefault(dict_name, 'i16', k, toInt16(42)),
    dictGetOrDefault(dict_name, 'i32', k, toInt32(42)),
    dictGetOrDefault(dict_name, 'i64', k, toInt64(42)),
    dictGetOrDefault(dict_name, 'u8', k, toUInt8(42)),
    dictGetOrDefault(dict_name, 'u16', k, toUInt16(42)),
    dictGetOrDefault(dict_name, 'u32', k, toUInt32(42)),
    dictGetOrDefault(dict_name, 'u64', k, toUInt64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (toInt8(42), toInt16(42), toInt32(42)));
select 'dictGetOrDefault', 'hashed_sparse_ints' as dict_name, toUInt64(0) as k,
    dictGetOrDefault(dict_name, 'i8', k, toInt8(42)),
    dictGetOrDefault(dict_name, 'i16', k, toInt16(42)),
    dictGetOrDefault(dict_name, 'i32', k, toInt32(42)),
    dictGetOrDefault(dict_name, 'i64', k, toInt64(42)),
    dictGetOrDefault(dict_name, 'u8', k, toUInt8(42)),
    dictGetOrDefault(dict_name, 'u16', k, toUInt16(42)),
    dictGetOrDefault(dict_name, 'u32', k, toUInt32(42)),
    dictGetOrDefault(dict_name, 'u64', k, toUInt64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (toInt8(42), toInt16(42), toInt32(42)));

select 'dictGet', 'cache_ints' as dict_name, toUInt64(1) as k,
    dictGet(dict_name, 'i8', k),
    dictGet(dict_name, 'i16', k),
    dictGet(dict_name, 'i32', k),
    dictGet(dict_name, 'i64', k),
    dictGet(dict_name, 'u8', k),
    dictGet(dict_name, 'u16', k),
    dictGet(dict_name, 'u32', k),
    dictGet(dict_name, 'u64', k),
    dictGet(dict_name, ('i8', 'i16', 'i32'), k);
select 'dictGetOrDefault', 'cache_ints' as dict_name, toUInt64(1) as k,
    dictGetOrDefault(dict_name, 'i8', k, toInt8(42)),
    dictGetOrDefault(dict_name, 'i16', k, toInt16(42)),
    dictGetOrDefault(dict_name, 'i32', k, toInt32(42)),
    dictGetOrDefault(dict_name, 'i64', k, toInt64(42)),
    dictGetOrDefault(dict_name, 'u8', k, toUInt8(42)),
    dictGetOrDefault(dict_name, 'u16', k, toUInt16(42)),
    dictGetOrDefault(dict_name, 'u32', k, toUInt32(42)),
    dictGetOrDefault(dict_name, 'u64', k, toUInt64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (toInt8(42), toInt16(42), toInt32(42)));
select 'dictGetOrDefault', 'cache_ints' as dict_name, toUInt64(0) as k,
    dictGetOrDefault(dict_name, 'i8', k, toInt8(42)),
    dictGetOrDefault(dict_name, 'i16', k, toInt16(42)),
    dictGetOrDefault(dict_name, 'i32', k, toInt32(42)),
    dictGetOrDefault(dict_name, 'i64', k, toInt64(42)),
    dictGetOrDefault(dict_name, 'u8', k, toUInt8(42)),
    dictGetOrDefault(dict_name, 'u16', k, toUInt16(42)),
    dictGetOrDefault(dict_name, 'u32', k, toUInt32(42)),
    dictGetOrDefault(dict_name, 'u64', k, toUInt64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (toInt8(42), toInt16(42), toInt32(42)));

select 'dictGet', 'complex_hashed_ints' as dict_name, tuple(toUInt64(1)) as k,
    dictGet(dict_name, 'i8', k),
    dictGet(dict_name, 'i16', k),
    dictGet(dict_name, 'i32', k),
    dictGet(dict_name, 'i64', k),
    dictGet(dict_name, 'u8', k),
    dictGet(dict_name, 'u16', k),
    dictGet(dict_name, 'u32', k),
    dictGet(dict_name, 'u64', k),
    dictGet(dict_name, ('i8', 'i16', 'i32'), k);
select 'dictGetOrDefault', 'complex_hashed_ints' as dict_name, tuple(toUInt64(1)) as k,
    dictGetOrDefault(dict_name, 'i8', k, toInt8(42)),
    dictGetOrDefault(dict_name, 'i16', k, toInt16(42)),
    dictGetOrDefault(dict_name, 'i32', k, toInt32(42)),
    dictGetOrDefault(dict_name, 'i64', k, toInt64(42)),
    dictGetOrDefault(dict_name, 'u8', k, toUInt8(42)),
    dictGetOrDefault(dict_name, 'u16', k, toUInt16(42)),
    dictGetOrDefault(dict_name, 'u32', k, toUInt32(42)),
    dictGetOrDefault(dict_name, 'u64', k, toUInt64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (toInt8(42), toInt16(42), toInt32(42)));
select 'dictGetOrDefault', 'complex_hashed_ints' as dict_name, tuple(toUInt64(0)) as k,
    dictGetOrDefault(dict_name, 'i8', k, toInt8(42)),
    dictGetOrDefault(dict_name, 'i16', k, toInt16(42)),
    dictGetOrDefault(dict_name, 'i32', k, toInt32(42)),
    dictGetOrDefault(dict_name, 'i64', k, toInt64(42)),
    dictGetOrDefault(dict_name, 'u8', k, toUInt8(42)),
    dictGetOrDefault(dict_name, 'u16', k, toUInt16(42)),
    dictGetOrDefault(dict_name, 'u32', k, toUInt32(42)),
    dictGetOrDefault(dict_name, 'u64', k, toUInt64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (toInt8(42), toInt16(42), toInt32(42)));

select 'dictGet', 'complex_cache_ints' as dict_name, tuple(toUInt64(1)) as k,
    dictGet(dict_name, 'i8', k),
    dictGet(dict_name, 'i16', k),
    dictGet(dict_name, 'i32', k),
    dictGet(dict_name, 'i64', k),
    dictGet(dict_name, 'u8', k),
    dictGet(dict_name, 'u16', k),
    dictGet(dict_name, 'u32', k),
    dictGet(dict_name, 'u64', k),
    dictGet(dict_name, ('i8', 'i16', 'i32'), k);;
select 'dictGetOrDefault', 'complex_cache_ints' as dict_name, tuple(toUInt64(1)) as k,
    dictGetOrDefault(dict_name, 'i8', k, toInt8(42)),
    dictGetOrDefault(dict_name, 'i16', k, toInt16(42)),
    dictGetOrDefault(dict_name, 'i32', k, toInt32(42)),
    dictGetOrDefault(dict_name, 'i64', k, toInt64(42)),
    dictGetOrDefault(dict_name, 'u8', k, toUInt8(42)),
    dictGetOrDefault(dict_name, 'u16', k, toUInt16(42)),
    dictGetOrDefault(dict_name, 'u32', k, toUInt32(42)),
    dictGetOrDefault(dict_name, 'u64', k, toUInt64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (toInt8(42), toInt16(42), toInt32(42)));
select 'dictGetOrDefault', 'complex_cache_ints' as dict_name, tuple(toUInt64(0)) as k,
    dictGetOrDefault(dict_name, 'i8', k, toInt8(42)),
    dictGetOrDefault(dict_name, 'i16', k, toInt16(42)),
    dictGetOrDefault(dict_name, 'i32', k, toInt32(42)),
    dictGetOrDefault(dict_name, 'i64', k, toInt64(42)),
    dictGetOrDefault(dict_name, 'u8', k, toUInt8(42)),
    dictGetOrDefault(dict_name, 'u16', k, toUInt16(42)),
    dictGetOrDefault(dict_name, 'u32', k, toUInt32(42)),
    dictGetOrDefault(dict_name, 'u64', k, toUInt64(42)),
    dictGetOrDefault(dict_name, ('i8', 'i16', 'i32'), k, (toInt8(42), toInt16(42), toInt32(42)));

--

select 'dictGet', 'flat_strings' as dict_name, toUInt64(1) as k, dictGet(dict_name, 'str', k), dictGet(dict_name, ('str'), k);
select 'dictGetOrDefault', 'flat_strings' as dict_name, toUInt64(1) as k, dictGetOrDefault(dict_name, 'str', k, '*'), dictGetOrDefault(dict_name, ('str'), k, ('*'));
select 'dictGetOrDefault', 'flat_strings' as dict_name, toUInt64(0) as k, dictGetOrDefault(dict_name, 'str', k, '*'), dictGetOrDefault(dict_name, ('str'), k, ('*'));

select 'dictGet', 'hashed_strings' as dict_name, toUInt64(1) as k, dictGet(dict_name, 'str', k), dictGet(dict_name, ('str'), k);
select 'dictGetOrDefault', 'hashed_strings' as dict_name, toUInt64(1) as k, dictGetOrDefault(dict_name, 'str', k, '*'), dictGetOrDefault(dict_name, ('str'), k, ('*'));
select 'dictGetOrDefault', 'hashed_strings' as dict_name, toUInt64(0) as k, dictGetOrDefault(dict_name, 'str', k, '*'), dictGetOrDefault(dict_name, ('str'), k, ('*'));

select 'dictGet', 'cache_strings' as dict_name, toUInt64(1) as k, dictGet(dict_name, 'str', k), dictGet(dict_name, ('str'), k);
select 'dictGetOrDefault', 'cache_strings' as dict_name, toUInt64(1) as k, dictGetOrDefault(dict_name, 'str', k, '*'), dictGetOrDefault(dict_name, ('str'), k, ('*'));
select 'dictGetOrDefault', 'cache_strings' as dict_name, toUInt64(0) as k, dictGetOrDefault(dict_name, 'str', k, '*'), dictGetOrDefault(dict_name, ('str'), k, ('*'));

select 'dictGet', 'complex_hashed_strings' as dict_name, toUInt64(1) as k, dictGet(dict_name, 'str', tuple(k)), dictGet(dict_name, ('str'), tuple(k));
select 'dictGetOrDefault', 'complex_hashed_strings' as dict_name, toUInt64(1) as k, dictGetOrDefault(dict_name, 'str', tuple(k), '*'), dictGetOrDefault(dict_name, ('str'), tuple(k), ('*'));
select 'dictGetOrDefault', 'complex_hashed_strings' as dict_name, toUInt64(0) as k, dictGetOrDefault(dict_name, 'str', tuple(k), '*'), dictGetOrDefault(dict_name, ('str'), tuple(k), ('*'));

select 'dictGet', 'complex_cache_strings' as dict_name, toUInt64(1) as k, dictGet(dict_name, 'str', tuple(k)), dictGet(dict_name, ('str'), tuple(k));
select 'dictGetOrDefault', 'complex_cache_strings' as dict_name, toUInt64(1) as k, dictGetOrDefault(dict_name, 'str', tuple(k), '*'), dictGetOrDefault(dict_name, ('str'), tuple(k), ('*'));
select 'dictGetOrDefault', 'complex_cache_strings' as dict_name, toUInt64(0) as k, dictGetOrDefault(dict_name, 'str', tuple(k), '*'), dictGetOrDefault(dict_name, ('str'), tuple(k), ('*'));

--

select 'dictGet', 'flat_decimals' as dict_name, toUInt64(1) as k,
    dictGet(dict_name, 'd32', k),
    dictGet(dict_name, 'd64', k),
    dictGet(dict_name, 'd128', k),
    dictGet(dict_name, ('d32', 'd64', 'd128'), k);
select 'dictGetOrDefault', 'flat_decimals' as dict_name, toUInt64(1) as k,
    dictGetOrDefault(dict_name, 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (toDecimal32(42, 4), toDecimal64(42, 6), toDecimal128(42, 1)));
select 'dictGetOrDefault', 'flat_decimals' as dict_name, toUInt64(0) as k,
    dictGetOrDefault(dict_name, 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (toDecimal32(42, 4), toDecimal64(42, 6), toDecimal128(42, 1)));

select 'dictGet', 'hashed_decimals' as dict_name, toUInt64(1) as k,
    dictGet(dict_name, 'd32', k),
    dictGet(dict_name, 'd64', k),
    dictGet(dict_name, 'd128', k),
    dictGet(dict_name, ('d32', 'd64', 'd128'), k);
select 'dictGetOrDefault', 'hashed_decimals' as dict_name, toUInt64(1) as k,
    dictGetOrDefault(dict_name, 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (toDecimal32(42, 4), toDecimal64(42, 6), toDecimal128(42, 1)));
select 'dictGetOrDefault', 'hashed_decimals' as dict_name, toUInt64(0) as k,
    dictGetOrDefault(dict_name, 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (toDecimal32(42, 4), toDecimal64(42, 6), toDecimal128(42, 1)));

select 'dictGet', 'cache_decimals' as dict_name, toUInt64(1) as k,
    dictGet(dict_name, 'd32', k),
    dictGet(dict_name, 'd64', k),
    dictGet(dict_name, 'd128', k),
    dictGet(dict_name, ('d32', 'd64', 'd128'), k);
select 'dictGetOrDefault', 'cache_decimals' as dict_name, toUInt64(1) as k,
    dictGetOrDefault(dict_name, 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (toDecimal32(42, 4), toDecimal64(42, 6), toDecimal128(42, 1)));
select 'dictGetOrDefault', 'cache_decimals' as dict_name, toUInt64(0) as k,
    dictGetOrDefault(dict_name, 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (toDecimal32(42, 4), toDecimal64(42, 6), toDecimal128(42, 1)));

select 'dictGet', 'complex_hashed_decimals' as dict_name, tuple(toUInt64(1)) as k,
    dictGet(dict_name, 'd32', k),
    dictGet(dict_name, 'd64', k),
    dictGet(dict_name, 'd128', k),
    dictGet(dict_name, ('d32', 'd64', 'd128'), k);
select 'dictGetOrDefault', 'complex_hashed_decimals' as dict_name, tuple(toUInt64(1)) as k,
    dictGetOrDefault(dict_name, 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (toDecimal32(42, 4), toDecimal64(42, 6), toDecimal128(42, 1)));
select 'dictGetOrDefault', 'complex_hashed_decimals' as dict_name, tuple(toUInt64(0)) as k,
    dictGetOrDefault(dict_name, 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (toDecimal32(42, 4), toDecimal64(42, 6), toDecimal128(42, 1)));

select 'dictGet', 'complex_cache_decimals' as dict_name, tuple(toUInt64(1)) as k,
    dictGet(dict_name, 'd32', k),
    dictGet(dict_name, 'd64', k),
    dictGet(dict_name, 'd128', k),
    dictGet(dict_name, ('d32', 'd64', 'd128'), k);
select 'dictGetOrDefault', 'complex_cache_decimals' as dict_name, tuple(toUInt64(1)) as k,
    dictGetOrDefault(dict_name, 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (toDecimal32(42, 4), toDecimal64(42, 6), toDecimal128(42, 1)));
select 'dictGetOrDefault', 'complex_cache_decimals' as dict_name, tuple(toUInt64(0)) as k,
    dictGetOrDefault(dict_name, 'd32', k, toDecimal32(42, 4)),
    dictGetOrDefault(dict_name, 'd64', k, toDecimal64(42, 6)),
    dictGetOrDefault(dict_name, 'd128', k, toDecimal128(42, 1)),
    dictGetOrDefault(dict_name, ('d32', 'd64', 'd128'), k, (toDecimal32(42, 4), toDecimal64(42, 6), toDecimal128(42, 1)));

--
-- Keep the tables, so that the dictionaries can be reloaded correctly and
-- SYSTEM RELOAD DICTIONARIES doesn't break.
-- We could also:
-- * drop the dictionaries -- not possible, they are configured in a .xml;
-- * switch dictionaries to DDL syntax so that they can be dropped -- tedious,
--   because there are a couple dozens of them, and also we need to have some
--   .xml dictionaries in tests so that we test backward compatibility with this
--   format;
-- * unload dictionaries -- no command for that.
--
