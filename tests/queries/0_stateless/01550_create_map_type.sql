-- String type
drop table if exists table_map;

create table table_map (a Map(String, String)) engine Memory;

insert into table_map values ({'name':'zhangsan', 'gender':'male'}), ({'name':'lisi', 'gender':'female'});

select a['name'] from table_map;

drop table if exists table_map;

-- Int type
drop table if exists table_map2;

create table table_map2(a Map(UInt8, UInt64)) Engine = Memory;

insert into table_map2 values({1: 100, 1000: 300}), ({1: 60}), ({2: 40, 7:90, 1:100});

select a[1] from table_map2;

drop table if exists table_map2;


-- Array Type
drop table if exists table_map3;

create table table_map3(a Map(String, Array(UInt8))) Engine = Memory;

insert into table_map3 values({'k1':[1,2,3], 'k2':[4,5,6]}), ({'k0':[], 'k1':[100,20,90]});

select a['k1'] from table_map3;

drop table if exists table_map3;
