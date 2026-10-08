drop table if exists test;
drop table if exists test2;
create table test (id UInt64, json JSON) engine=MergeTree order by id;
insert into test select number, '{}' from numbers(100000);
alter table test update json = '{"a" : 42}' where id > 50000 settings mutations_sync=1;
create table test2 (json JSON) engine=MergeTree order by tuple();
insert into test2 select if(id < 75000, json, '{"a" : 42}'::JSON) from test;
select * from test2 format Null;

