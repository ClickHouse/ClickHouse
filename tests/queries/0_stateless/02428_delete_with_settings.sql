drop table if exists test;
create table test (id Int32, key String) engine=MergeTree() order by tuple();
insert into test select number, toString(number) from numbers(1000000);
set allow_experimental_lightweight_delete=1;
delete from test where id % 2 = 0 SETTINGS mutations_sync=1;
select count() from test;
