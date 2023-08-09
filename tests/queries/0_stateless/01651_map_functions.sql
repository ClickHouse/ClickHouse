set allow_experimental_map_type = 1;

-- String type
drop table if exists table_map;
create table table_map (a Map(String, String), b String) engine = Memory;
insert into table_map values ({'name':'zhangsan', 'age':'10'}, 'name'), ({'name':'lisi', 'gender':'female'},'age');
select mapContains(a, 'name') from table_map;
select mapContains(a, 'gender') from table_map;
select mapContains(a, 'abc') from table_map;
select mapContains(a, b) from table_map;
select mapContains(a, 10) from table_map; -- { serverError 386 }
select mapKeys(a) from table_map;
drop table if exists table_map;

CREATE TABLE table_map (a Map(UInt8, Int), b UInt8, c UInt32) engine = MergeTree order by tuple();
insert into table_map select map(number, number), number, number from numbers(1000, 3);
select mapContains(a, b), mapContains(a, c), mapContains(a, 233) from table_map;
select mapContains(a, 'aaa') from table_map; -- { serverError 386 }
select mapContains(b, 'aaa') from table_map; -- { serverError 43 }
select mapKeys(a) from table_map;
select mapValues(a) from table_map;
drop table if exists table_map;


-- Const column
select map( 'aa', 4, 'bb' , 5) as m, mapKeys(m), mapValues(m);
select map( 'aa', 4, 'bb' , 5) as m, mapContains(m, 'aa'), mapContains(m, 'k');

select map(0, 0) as m, mapContains(m, number % 2) from numbers(2);
