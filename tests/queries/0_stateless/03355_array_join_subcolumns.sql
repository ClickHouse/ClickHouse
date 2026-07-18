-- Tags: no-parallel-replicas

set enable_analyzer=1;

drop table if exists test;
create table test (arr Array(UInt64), t Tuple(a Array(UInt32), b Array(UInt32))) engine=Memory;
insert into test select [1, 2, 3], tuple([1, 2], [1, 2, 3, 4]);

select t.a from test array join arr;
explain query tree select t.a from test array join arr;

select t.a from test array join t.b;
explain query tree select t.a from test array join t.b;

drop table test;

