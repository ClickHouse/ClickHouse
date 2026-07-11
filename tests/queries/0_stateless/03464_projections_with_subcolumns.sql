-- Tags: long

set enable_analyzer=1;
set mutations_sync=1;
set parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, optimize_aggregation_in_order = 0;

drop table if exists test;

create table test (
    a UInt32,
    json JSON(a UInt32),
    t Tuple(a UInt32, b UInt32),
    projection p1 (select json order by json.a),
    projection p2 (select t order by t.a),
    projection p3 (select json order by json.c[].d.:Int64),
) engine=MergeTree order by tuple() settings index_granularity=1;

insert into test select number, toJSONString(map('a', number, 'b', 'str', 'c', [toJSONString(map('d', number::UInt32))::JSON])), tuple(number, number) from numbers(100) settings use_variant_as_common_type=1, output_format_json_quote_64bit_integers=0;

explain indexes=1 select json from test where json.a = 1 settings enable_parallel_replicas=0;
select trimLeft(*) from (explain indexes=1 select json from test where json.a = 1) where explain like '%ReadFromMergeTree%';
select json from test where json.a = 1;

explain indexes=1 select t from test where t.a = 1 settings enable_parallel_replicas=0;
select trimLeft(*) from (explain indexes=1 select t from test where t.a = 1) where explain like '%ReadFromMergeTree%';
select t from test where t.a = 1;

explain indexes=1 select json from test where json.c[].d.:Int64 = [1] settings enable_parallel_replicas=0;
select trimLeft(*) from (explain indexes=1 select json from test where json.c[].d.:Int64 = [1]) where explain like '%ReadFromMergeTree%';
select json from test where json.c[].d.:Int64 = [1];

insert into test select number, toJSONString(map('a', number, 'b', 'str', 'c', [toJSONString(map('d', number::UInt32))::JSON])), tuple(number, number) from numbers(100) settings use_variant_as_common_type=1, output_format_json_quote_64bit_integers=0;

optimize table test final;

explain indexes=1 select json from test where json.a = 1 settings enable_parallel_replicas=0;
select trimLeft(*) from (explain indexes=1 select json from test where json.a = 1) where explain like '%ReadFromMergeTree%';
select json from test where json.a = 1;

explain indexes=1 select t from test where t.a = 1 settings enable_parallel_replicas=0;
select trimLeft(*) from (explain indexes=1 select t from test where t.a = 1) where explain like '%ReadFromMergeTree%';
select t from test where t.a = 1;

explain indexes=1 select json from test where json.c[].d.:Int64 = [1] settings enable_parallel_replicas=0;
select trimLeft(*) from (explain indexes=1 select json from test where json.c[].d.:Int64 = [1]) where explain like '%ReadFromMergeTree%';
select json from test where json.c[].d.:Int64 = [1];

drop table test;

select '------------------------------------------------------------------';

create table test (
    a UInt32,
    json JSON(a UInt32),
    t Tuple(a UInt32, b UInt32),
) engine=MergeTree order by tuple() settings index_granularity=1;

insert into test select number, toJSONString(map('a', number, 'b', 'str', 'c', [toJSONString(map('d', number::UInt32))::JSON])), tuple(number, number) from numbers(100) settings use_variant_as_common_type=1, output_format_json_quote_64bit_integers=0;

alter table test add projection p1 (select json order by json.a);
alter table test materialize projection p1;

alter table test add projection p2 (select t order by t.a);
alter table test materialize projection p2;

alter table test add projection p3 (select json order by json.c[].d.:Int64);
alter table test materialize projection p3;

alter table test add projection p (select json.b order by json.a); -- {serverError NOT_IMPLEMENTED}
alter table test add projection p (select t.a order by json.a); -- {serverError NOT_IMPLEMENTED}
alter table test add projection p (select a order by json.a); -- {serverError NOT_IMPLEMENTED}
alter table test add projection p (select t.b order by t.a); -- {serverError NOT_IMPLEMENTED}
alter table test add projection p (select json.a order by t.a); -- {serverError NOT_IMPLEMENTED}
alter table test add projection p (select a order by t.a); -- {serverError NOT_IMPLEMENTED}
alter table test add projection p (select json.a order by json.c[].d.:Int64); -- {serverError NOT_IMPLEMENTED}
alter table test add projection p (select t.a order by json.c[].d.:Int64); -- {serverError NOT_IMPLEMENTED}
alter table test add projection p (select a order by json.c[].d.:Int64);-- {serverError NOT_IMPLEMENTED}

explain indexes=1 select json from test where json.a = 1 settings enable_parallel_replicas=0;
select trimLeft(*) from (explain indexes=1 select json from test where json.a = 1) where explain like '%ReadFromMergeTree%';
select json from test where json.a = 1;

explain indexes=1 select t from test where t.a = 1 settings enable_parallel_replicas=0;
select trimLeft(*) from (explain indexes=1 select t from test where t.a = 1) where explain like '%ReadFromMergeTree%';
select t from test where t.a = 1;

explain indexes=1 select json from test where json.c[].d.:Int64 = [1] settings enable_parallel_replicas=0;
select trimLeft(*) from (explain indexes=1 select json from test where json.c[].d.:Int64 = [1]) where explain like '%ReadFromMergeTree%';
select json from test where json.c[].d.:Int64 = [1];


drop table test;
