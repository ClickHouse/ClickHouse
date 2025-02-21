drop table if exists ref_table;
drop table if exists alias_table;

create table ref_table (id UInt32, name String) Engine=MergeTree order by id;
insert into ref_table values (1, 'one'), (2, 'two'), (3, 'three');

create table alias_table Engine=Alias(currentDatabase(), ref_table);
select * from alias_table order by id;

insert into alias_table values (4, 'four');
select * from alias_table order by id;
select * from ref_table order by id;
select * from ref_table order by id settings allow_experimental_analyzer=0;

drop table ref_table;
select * from alias_table order by id; -- { serverError 60 }

create table ref_table (id UInt32, not_name String) Engine=MergeTree order by id;
select * from alias_table order by id; -- { serverError 36 }

drop table ref_table;
create table ref_table (id UInt32, name Int) Engine=MergeTree order by id;
select * from alias_table order by id; -- { serverError 36 }

drop table ref_table;
create table ref_table (id UInt32, name String) Engine=MergeTree order by id;
select * from alias_table order by id;

drop table ref_table;
drop table alias_table;
