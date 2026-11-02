-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: result of explain is different

set enable_analyzer=1;
drop table if exists test;
create table test (json JSON(d Date)) engine=MergeTree order by tuple() partition by json.d;
insert into test select '{"d" : "2020-01-01"}';
insert into test select '{"d" : "2021-01-01"}';
insert into test select '{"d" : "2022-01-01"}';
insert into test select '{"d" : "2023-01-01"}';
select * from test order by json.d;
explain indexes=1 select * from test where json.d = '2020-01-01';
drop table test;

create table test (c0 Array(Nullable(Int)), c1 Int, c2 Int) ENGINE = MergeTree() partition by (c0.null) order by (c1);
insert into test  VALUES ([], 1, 2), ([1], 3, 4);
select * from test order by all;
drop table test;

