-- Tags: no-parallel-replicas

set enable_json_type=1;
set allow_experimental_variant_type=1;
set use_variant_as_common_type=1;
set enable_analyzer=1;

drop table if exists test;
create table test (json JSON(a Array(UInt32), b Array(UInt32), c UInt32), index idx1 json.a type set(0), index idx2 json.c type minmax) engine=MergeTree order by tuple() settings index_granularity=1;
insert into test select toJSONString(map('a', range(number % 3 + 1), 'b', range(number % 2 + 1), 'c', number)) from numbers(10);

select json.a from test array join json.b where has(json.a, 2);
explain indexes=1 select json.a from test array join json.b where has(json.a, 2) and json.c < 5;

drop table test;
