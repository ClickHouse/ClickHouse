drop table if exists test;
create table test (s String, json JSON materialized s) engine=MergeTree order by tuple();
insert into test select '{"a" : 42}';
alter table test update s = '{}' where json.a = 42 settings mutations_sync=1;
select * from test;
drop table test;

