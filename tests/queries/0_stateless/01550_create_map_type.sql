-- String type
drop table if exists table_map;
create table table_map (a Map(String, String)) engine = Memory;
insert into table_map values ({'name':'zhangsan', 'gender':'male'}), ({'name':'lisi', 'gender':'female'});
select a['name'] from table_map;
drop table if exists table_map;


drop table if exists table_map;
create table table_map (a Map(String, UInt64)) engine = MergeTree() order by a SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into table_map select map('key1', number, 'key2', number * 2) from numbers(1111, 3);
select a['key1'], a['key2'] from table_map;
drop table if exists table_map;

-- MergeTree Engine
drop table if exists table_map;
create table table_map (a Map(String, String), b String) engine = MergeTree() order by a SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into table_map values ({'name':'zhangsan', 'gender':'male'}, 'name'), ({'name':'lisi', 'gender':'female'}, 'gender');
select a[b] from table_map;
select b from table_map where a = map('name','lisi', 'gender', 'female');
drop table if exists table_map;

-- Big Integer type

create table table_map (d DATE, m Map(Int8, UInt256)) ENGINE = MergeTree() order by d SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into table_map values ('2020-01-01', map(1, 0, 2, 1));
select * from table_map;
drop table table_map;

-- Integer type

create table table_map (d DATE, m Map(Int8, Int8)) ENGINE = MergeTree() order by d SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into table_map values ('2020-01-01', map(1, 0, 2, -1));
select * from table_map;
drop table table_map;

-- Unsigned Int type
drop table if exists table_map;
create table table_map(a Map(UInt8, UInt64), b UInt8) Engine = MergeTree() order by b SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into table_map select map(number, number+5), number from numbers(1111,4);
select a[b] from table_map;
drop table if exists table_map;


-- Array Type
drop table if exists table_map;
create table table_map(a Map(String, Array(UInt8))) Engine = MergeTree() order by a SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into table_map values(map('k1', [1,2,3], 'k2', [4,5,6])), (map('k0', [], 'k1', [100,20,90]));
insert into table_map select map('k1', [number, number + 2, number * 2]) from numbers(6);
insert into table_map select map('k2', [number, number + 2, number * 2]) from numbers(6);
select a['k1'] as col1 from table_map order by col1;
drop table if exists table_map;

SELECT CAST(([1, 2, 3], ['1', '2', 'foo']), 'Map(UInt8, String)') AS map, map[1];

CREATE TABLE table_map (n UInt32, m Map(String, Int))
ENGINE = MergeTree ORDER BY n SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi';

-- coversion from Tuple(Array(K), Array(V))
INSERT INTO table_map SELECT number, (arrayMap(x -> toString(x), range(number % 10 + 2)), range(number % 10 + 2)) FROM numbers(100000);
-- coversion from Array(Tuple(K, V))
INSERT INTO table_map SELECT number, arrayMap(x -> (toString(x), x), range(number % 10 + 2)) FROM numbers(100000);
SELECT sum(m['1']), sum(m['7']), sum(m['100']) FROM table_map;

DROP TABLE IF EXISTS table_map;

CREATE TABLE table_map (n UInt32, m Map(String, Int))
ENGINE = MergeTree ORDER BY n SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

-- coversion from Tuple(Array(K), Array(V))
INSERT INTO table_map SELECT number, (arrayMap(x -> toString(x), range(number % 10 + 2)), range(number % 10 + 2)) FROM numbers(100000);
-- coversion from Array(Tuple(K, V))
INSERT INTO table_map SELECT number, arrayMap(x -> (toString(x), x), range(number % 10 + 2)) FROM numbers(100000);
SELECT sum(m['1']), sum(m['7']), sum(m['100']) FROM table_map;

DROP TABLE IF EXISTS table_map;

SELECT CAST(([2, 1, 1023], ['', '']), 'Map(UInt8, String)') AS map, map[10] -- { serverError TYPE_MISMATCH}
