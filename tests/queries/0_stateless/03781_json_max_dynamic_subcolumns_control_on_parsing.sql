-- Tags: no-msan, no-tsan

drop table if exists test;
create table test (json JSON) engine=MergeTree order by tuple();
insert into test select toJSONString(arrayMap(x -> tuple('a' || x, x), range(20))::Map(String, UInt32)) from numbers(1000000) settings max_dynamic_subcolumns_in_json_type_parsing=10;
select distinct(arrayJoin(JSONDynamicPaths(json))) as path from test order by path;
drop table test;
