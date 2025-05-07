drop table if exists test_minmax;
drop table if exists test_minmax_v;

set enable_analyzer=1;

create table test_minmax (x UInt32) engine = MergeTree order by x;
insert into test_minmax select number + 42 from numbers(1e6);

create view test_minmax_v (x UInt32) as select x from test_minmax;
explain select min(x), max(x) from test_minmax_v;
