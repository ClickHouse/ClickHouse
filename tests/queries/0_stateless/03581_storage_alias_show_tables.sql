-- Tags: no-parallel
drop table if exists ref_table;
drop table if exists alias_table;

create table ref_table (id UInt32, name String) Engine=MergeTree order by id;
create table alias_table Engine=Alias(currentDatabase(), ref_table);

show tables;
select name from system.tables where database=currentDatabase() order by name;

drop table ref_table;

show tables;
select name from system.tables where database=currentDatabase() order by name;
