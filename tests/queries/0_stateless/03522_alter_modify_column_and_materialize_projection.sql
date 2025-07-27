drop table if exists test;
create table test (s String) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=0;
insert into test select 'str' from numbers(1);
alter table test modify column s Nullable(String);
alter table test add projection p1 (select s order by s);
alter table test materialize projection p1 settings mutations_sync=1;
select * from test;
drop table test;

