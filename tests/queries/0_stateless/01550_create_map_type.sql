-- String type
drop table if exists table_map;
create table table_map (a Map(String, String)) engine = Memory;
insert into table_map values ({'name':'zhangsan', 'gender':'male'}), ({'name':'lisi', 'gender':'female'});
select a['name'] from table_map;
drop table if exists table_map;


drop table if exists table_map;
create table table_map (a Map(String, UInt64)) engine = MergeTree() order by a;
insert into table_map select map('key1', number, 'key2', number * 2) from numbers(1111, 3);
select a['key1'], a['key2'] from table_map;
drop table if exists table_map;

-- MergeTree Engine
drop table if exists table_map;
create table table_map (a Map(String, String), b String) engine = MergeTree() order by a;
insert into table_map values ({'name':'zhangsan', 'gender':'male'}, 'name'), ({'name':'lisi', 'gender':'female'}, 'gender');
select a[b] from table_map;
drop table if exists table_map;

-- Int type
drop table if exists table_map;
create table table_map(a Map(UInt8, UInt64), b UInt8) Engine = MergeTree() order by b;
insert into table_map select {number:number+5}, number from numbers(1111,4);
select a[b] from table_map;
drop table if exists table_map;


-- Array Type
drop table if exists table_map;
create table table_map(a Map(String, Array(UInt8))) Engine = MergeTree() order by a;
insert into table_map values({'k1':[1,2,3], 'k2':[4,5,6]}), ({'k0':[], 'k1':[100,20,90]});
insert into table_map select {'k1' : [number, number + 2, number * 2]} from numbers(6);
insert into table_map select map('k2' , [number, number + 2, number * 2]) from numbers(6);
select a['k1'] as col1 from table_map order by col1;
drop table if exists table_map;
