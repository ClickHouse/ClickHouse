drop table if exists table_map;

create table table_map (a Map(String, String)) engine Memory;

insert into table_map values ({'name':'zhangsan', 'gender':'male'}), ({'name':'lisi', 'gender':'female'});

select a['name'] from table_map;

drop table if exists table_map;
