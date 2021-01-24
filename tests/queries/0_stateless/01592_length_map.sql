set allow_experimental_map_type = 1;

drop table if exists table_map;
create table table_map (a Map(String, String)) engine = Memory;
insert into table_map values ({'name':'zhangsan', 'gender':'male'}), ({'name':'lisi', 'gender':'female'});
select length(a) from table_map; 
