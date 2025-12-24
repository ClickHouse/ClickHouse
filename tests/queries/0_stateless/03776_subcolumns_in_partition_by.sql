drop table if exists test;
create table test (json JSON(d Date)) engine=MergeTree order by tuple() partition by json.d;
insert into test select '{"d" : "2020-01-01"}';
insert into test select '{"d" : "2021-01-01"}';
insert into test select '{"d" : "2022-01-01"}';
insert into test select '{"d" : "2023-01-01"}';
select * from test order by json.d;
explain indexes=1 select * from test where json.d = '2020-01-01';
drop table test;

