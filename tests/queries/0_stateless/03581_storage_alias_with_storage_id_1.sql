-- Tags: no-parallel
drop table if exists ref_table;
drop table if exists alias_table;

create table ref_table (id UInt32, name String) Engine=MergeTree order by id;
create table alias_table Engine=Alias(currentDatabase(), ref_table);

select '-- Insert into reference table --';
insert into ref_table values (1, 'one'), (2, 'two'), (3, 'three');
select * from alias_table order by id;

select '-- Insert into alias table --';
insert into alias_table values (4, 'four');
select * from alias_table order by id;
select * from ref_table order by id;
select * from alias_table order by id settings enable_analyzer=0;

select '-- Rename reference table --';
rename table ref_table to ref_table2;
select * from alias_table order by id; -- { serverError 60 }

select '-- Rename reference table back --';
rename table ref_table2 to ref_table;
select * from alias_table order by id;

select '-- Alter reference table --';
alter table ref_table drop column name;
select * from alias_table order by id;

select '-- Alter alias table --';
alter table alias_table add column col Int32 Default 2; -- { serverError UNSUPPORTED_METHOD }
alter table ref_table add column col Int32 Default 2;
select * from ref_table order by id;
select * from alias_table order by id;

select '-- Drops reference table --';
drop table ref_table;
select * from alias_table order by id; -- { serverError 60 }

select '-- Re-create reference table with same name --';
create table ref_table (id UInt32, b Int32, c UInt32) Engine=MergeTree order by id;
insert into ref_table values (1, 2, 3);
select * from alias_table order by id;

select '-- Truncate alias table --';
truncate table alias_table; -- { serverError UNSUPPORTED_METHOD }
truncate table ref_table;
select * from ref_table order by id;
select * from alias_table order by id;

drop table alias_table;
drop table ref_table;
