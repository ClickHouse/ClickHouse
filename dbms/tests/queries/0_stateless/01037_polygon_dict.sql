-- Must use `test_00950` database and these tables - they're configured in dbms/tests/*_dictionary.xml
create database if not exists test_01037;
use test_01037;
drop table if exists polygons;

create table polygons (key Array(Array(Array(Array(Float64)))), name String, u64 UInt64) Engine = Memory;
insert into polygons values ([[[[1, 3], [1, 1], [3, 1], [3, -1], [1, -1], [1, -3], [-1. -3], [-1, -1], [-3, -1], [-3, 1], [-1, 1], [-1, 3]]], [[[5, 5], [5, 1], [7, 1], [7, 7], [1, 7], [1, 5]]]],
                             'Click',
                             42);
/*
insert into polygons values (
                             [
                                 [
                                     [
                                         [5, 5],
                                         [5, -5],
                                         [-5, -5],
                                         [-5, 5]
                                     ],
                                     [
                                         [1, 3],
                                         [1, 1],
                                         [3, 1],
                                         [3, -1],
                                         [1, -1],
                                         [1, -3],
                                         [-1. -3],
                                         [-1, -1],
                                         [-3, -1],
                                         [-3, 1],
                                         [-1, 1],
                                         [-1, 3]
                                     ]
                                 ]
                             ],
                             'House',
                             314159);
*/
select 'dictGet', 'polygons' as dict_name, (0.0, 0.0) as key,
       dictGet(dict_name, 'name', key),
       dictGet(dict_name, 'u64', key);
drop table polygons;
drop database test_01037;
