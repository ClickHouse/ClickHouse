-- Tags: no-parallel-replicas

set enable_analyzer=1;

drop table if exists test;
create table test (x UInt64, n Nullable(UInt32), t Tuple(a UInt32, b UInt32), json JSON) engine=MergeTree order by tuple() settings index_granularity=4;
insert into test select number, number < 4 ? NULL : number, tuple(number, number), toJSONString(map('a', number, 'b', number)) from numbers(12);
explain actions=1 select * from test where n.null settings optimize_move_to_prewhere=1;
select * from test where n.null settings optimize_move_to_prewhere=1;
explain actions=1 select * from test where n.null settings optimize_move_to_prewhere=0;
select * from test where n.null settings optimize_move_to_prewhere=0;

explain actions=1 select * from test where t.a < 4 settings optimize_move_to_prewhere=1;
select * from test where t.a < 4 settings optimize_move_to_prewhere=1;
explain actions=1 select * from test where t.a < 4 settings optimize_move_to_prewhere=0;
select * from test where t.a < 4 settings optimize_move_to_prewhere=0;

explain actions=1 select * from test where json.a::Int64 < 4 settings optimize_move_to_prewhere=1;
select * from test where json.a::Int64 < 4 settings optimize_move_to_prewhere=1;
explain actions=1 select * from test where json.a::Int64 < 4 settings optimize_move_to_prewhere=0;
select * from test where json.a::Int64 < 4 settings optimize_move_to_prewhere=0;

drop table test;

