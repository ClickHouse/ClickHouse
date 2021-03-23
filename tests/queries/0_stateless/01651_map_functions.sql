set allow_experimental_map_type = 1;

-- String type
drop table if exists table_map;
create table table_map (a Map(String), b String) engine = Memory;

-- FIXME(map): INSERT mapBuild doesn't work!
-- INSERT INTO table_map VALUES (mapBuild('name', 'zhangsan', 'age', '10'), 'name');
insert into table_map values ({'name':'zhangsan', 'age':'10'}, 'name'), ({'name':'lisi', 'gender':'female'},'age');

select mapContains(a, 'name') from table_map;
select mapContains(a, 'gender') from table_map;
select mapContains(a, 'abc') from table_map;
select mapContains(a, b) from table_map;
select mapContains(a, 10) from table_map; -- { serverError 43 }
select mapKeys(a) from table_map;
select mapValues(a) from table_map;
drop table if exists table_map;

-- multiple map functions in one statement
select mapBuild('aa', 4, 'bb', 5) as m, mapContains(m, 'aa'), mapContains(m, 'k'); 
select mapBuild('aa', 4, 'bb', 5) as m, mapKeys(m);

-- FIXME(map): server exception: Unexpected field type [ERRFMT] when reading Map(Nullable(UInt8))
-- select mapBuild('aa', 4, 'bb', 5) as m, mapValues(m);
