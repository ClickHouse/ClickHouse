drop table if exists data_01295;
create table data_01295 (key Int) Engine=AggregatingMergeTree() order by key;

insert into data_01295 values (1);
select * from data_01295;

select 'INSERT SELECT';
insert into data_01295 select * from data_01295; -- no stuck for now
select * from data_01295;

select 'INSERT SELECT max_threads';
insert into data_01295 select * from data_01295 final settings max_threads=2; -- stuck with multiple threads
select * from data_01295;

select 'INSERT SELECT max_insert_threads max_threads';
set max_insert_threads=2;
insert into data_01295 select * from data_01295 final settings max_threads=2; -- no stuck for now
select * from data_01295;
