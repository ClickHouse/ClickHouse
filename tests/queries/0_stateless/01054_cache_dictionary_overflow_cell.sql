
create database if not exists test_01054_overflow;
drop table if exists test_01054_overflow.ints;

create table test_01054_overflow.ints (key UInt64, i8 Int8, i16 Int16, i32 Int32, i64 Int64, u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64) Engine = Memory;

insert into test_01054_overflow.ints values (1, 1, 1, 1, 1, 1, 1, 1, 1);
insert into test_01054_overflow.ints values (2, 2, 2, 2, 2, 2, 2, 2, 2);
insert into test_01054_overflow.ints values (3, 3, 3, 3, 3, 3, 3, 3, 3);
insert into test_01054_overflow.ints values (4, 4, 4, 4, 4, 4, 4, 4, 4);
insert into test_01054_overflow.ints values (5, 5, 5, 5, 5, 5, 5, 5, 5);
insert into test_01054_overflow.ints values (6, 6, 6, 6, 6, 6, 6, 6, 6);
insert into test_01054_overflow.ints values (7, 7, 7, 7, 7, 7, 7, 7, 7);
insert into test_01054_overflow.ints values (8, 8, 8, 8, 8, 8, 8, 8, 8);
insert into test_01054_overflow.ints values (9, 9, 9, 9, 9, 9, 9, 9, 9);
insert into test_01054_overflow.ints values (10, 10, 10, 10, 10, 10, 10, 10, 10);
insert into test_01054_overflow.ints values (11, 11, 11, 11, 11, 11, 11, 11, 11);
insert into test_01054_overflow.ints values (12, 12, 12, 12, 12, 12, 12, 12, 12);
insert into test_01054_overflow.ints values (13, 13, 13, 13, 13, 13, 13, 13, 13);
insert into test_01054_overflow.ints values (14, 14, 14, 14, 14, 14, 14, 14, 14);
insert into test_01054_overflow.ints values (15, 15, 15, 15, 15, 15, 15, 15, 15);
insert into test_01054_overflow.ints values (16, 16, 16, 16, 16, 16, 16, 16, 16);
insert into test_01054_overflow.ints values (17, 17, 17, 17, 17, 17, 17, 17, 17);
insert into test_01054_overflow.ints values (18, 18, 18, 18, 18, 18, 18, 18, 18);
insert into test_01054_overflow.ints values (19, 19, 19, 19, 19, 19, 19, 19, 19);
insert into test_01054_overflow.ints values (20, 20, 20, 20, 20, 20, 20, 20, 20);

select 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(1)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(2)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(3)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(4)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(5)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(6)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(7)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(8)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(9)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(10)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(11)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(12)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(13)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(14)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(15)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(16)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(17)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(18)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(19)), 
dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(20));

SELECT arrayMap(x -> dictGet('one_cell_cache_ints_overflow', 'i8', toUInt64(x)), array)
FROM 
(
    SELECT [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20] AS array
);

DROP TABLE if exists test_01054.ints;
