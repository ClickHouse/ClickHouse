create database if not exists test;

drop table if exists test.test_ins_arr;
create table test.test_ins_arr (date Date, val Array(UInt64)) engine = MergeTree(date, (date), 8192);
insert into test.test_ins_arr select toDate('2017-10-02'), [number, 42] from system.numbers limit 10000;
select * from test.test_ins_arr limit 10;
drop table test.test_ins_arr;

drop table if exists test.test_ins_null;
create table test.test_ins_null (date Date, val Nullable(UInt64)) engine = MergeTree(date, (date), 8192);
insert into test.test_ins_null select toDate('2017-10-02'), if(number % 2, number, Null) from system.numbers limit 10000;
select * from test.test_ins_null limit 10;
drop table test.test_ins_null;

drop table if exists test.test_ins_arr_null;
create table test.test_ins_arr_null (date Date, val Array(Nullable(UInt64))) engine = MergeTree(date, (date), 8192);
insert into test.test_ins_arr_null select toDate('2017-10-02'), [if(number % 2, number, Null), number, Null] from system.numbers limit 10000;
select * from test.test_ins_arr_null limit 10;
drop table test.test_ins_arr_null;

drop table if exists test.test_ins_arr_arr;
create table test.test_ins_arr_arr (date Date, val Array(Array(UInt64))) engine = MergeTree(date, (date), 8192);
insert into test.test_ins_arr_arr select toDate('2017-10-02'), [[number],[number + 1, number + 2]] from system.numbers limit 10000;
select * from test.test_ins_arr_arr limit 10;
drop table test.test_ins_arr_arr;

drop table if exists test.test_ins_arr_arr_null;
create table test.test_ins_arr_arr_null (date Date, val Array(Array(Nullable(UInt64)))) engine = MergeTree(date, (date), 8192);
insert into test.test_ins_arr_arr_null select toDate('2017-10-02'), [[1, Null, number], [3, Null, number]] from system.numbers limit 10000;
select * from test.test_ins_arr_arr_null limit 10;
drop table test.test_ins_arr_arr_null;

drop table if exists test.test_ins_arr_arr_arr;
create table test.test_ins_arr_arr_arr (date Date, val Array(Array(Array(UInt64)))) engine = MergeTree(date, (date), 8192);
insert into test.test_ins_arr_arr_arr select toDate('2017-10-02'), [[[number]],[[number + 1], [number + 2, number + 3]]] from system.numbers limit 10000;
select * from test.test_ins_arr_arr_arr limit 10;
drop table test.test_ins_arr_arr_arr;
