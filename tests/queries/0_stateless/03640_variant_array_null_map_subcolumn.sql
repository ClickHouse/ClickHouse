drop table if exists test;
create table test (v Variant(Array(Nullable(String)))) engine=MergeTree order by tuple();
insert into test select ['hello', null, 'world'];
select v.`Array(Nullable(String))`.null from test;
drop table test;

