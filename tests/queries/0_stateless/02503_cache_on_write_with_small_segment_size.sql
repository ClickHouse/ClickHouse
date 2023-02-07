drop table ttt if exists;
create table ttt (id Int32, value String) engine=MergeTree() order by tuple()  settings storage_policy='s3_cache_7';
insert into ttt settings throw_on_error_from_cache_on_write_operations = 1 select number, toString(number) from numbers(100000);
select * from ttt format Null;
select count() from ttt;
drop table ttt no delay;
