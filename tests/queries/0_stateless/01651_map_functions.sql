set allow_experimental_map_type = 1;

-- String type
drop table if exists table_map;
create table table_map (a Map(String, String), b String, c Array(String), d Array(String)) engine = Memory;
insert into table_map values ({'name':'zhangsan', 'age':'10'}, 'name', ['name', 'age'], ['zhangsan', '10']), ({'name':'lisi', 'gender':'female'},'age',['name', 'gender'], ['lisi', 'female']);
select mapContains(a, 'name') from table_map;
select mapContains(a, 'gender') from table_map;
select mapContains(a, 'abc') from table_map;
select mapContains(a, b) from table_map;
select mapContains(a, 10) from table_map; -- { serverError NO_COMMON_TYPE }
select mapKeys(a) from table_map;
select mapFromArrays(c, d) from table_map;
drop table if exists table_map;

CREATE TABLE table_map (a Map(UInt8, Int), b UInt8, c UInt32, d Array(String), e Array(String)) engine = MergeTree order by tuple();
insert into table_map select map(number, number), number, number, [number, number, number], [number*2, number*3, number*4] from numbers(1000, 3);
select mapContains(a, b), mapContains(a, c), mapContains(a, 233) from table_map;
select mapContains(a, 'aaa') from table_map; -- { serverError NO_COMMON_TYPE }
select mapContains(b, 'aaa') from table_map; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select mapKeys(a) from table_map;
select mapValues(a) from table_map;
select mapFromArrays(d, e) from table_map;
drop table if exists table_map;


-- Const column
select map( 'aa', 4, 'bb' , 5) as m, mapKeys(m), mapValues(m);
select map( 'aa', 4, 'bb' , 5) as m, mapContains(m, 'aa'), mapContains(m, 'k');

select map(0, 0) as m, mapContains(m, number % 2) from numbers(2);

select mapFromArrays(['aa', 'bb'], [4, 5]);
select mapFromArrays(['aa', 'bb'], materialize([4, 5])) from numbers(2);
select mapFromArrays(materialize(['aa', 'bb']), [4, 5]) from numbers(2);
select mapFromArrays(materialize(['aa', 'bb']), materialize([4, 5])) from numbers(2);
select mapFromArrays('aa', [4, 5]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select mapFromArrays(['aa', 'bb'], 5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select mapFromArrays(['aa', 'bb'], [4, 5], [6, 7]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select mapFromArrays(['aa', 'bb'], [4, 5, 6]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
select mapFromArrays([[1,2], [3,4]], [4, 5, 6]); -- { serverError BAD_ARGUMENTS }
