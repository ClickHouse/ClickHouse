drop table if exists test;
create table test (s String) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, string_serialization_version='single_stream';
insert into test select 'abc' || toString(number) from numbers(100);
alter table test modify setting string_serialization_version='with_size_stream';
alter table test add column s1 String default 'def' settings alter_sync=2;
alter table test materialize column s1 settings mutations_sync=1;
detach table test;
attach table test;
select max(s), max(s1) from test;
drop table test;

