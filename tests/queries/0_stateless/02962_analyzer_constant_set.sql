create table test_parallel_index (
  x UInt64, 
  y UInt64, 
  z UInt64, 
  INDEX a (y) TYPE minmax GRANULARITY 2,
  INDEX b (z) TYPE set(8) GRANULARITY 2
) engine = MergeTree
order by x 
partition by bitAnd(x, 63 * 64)
settings index_granularity = 4;

insert into test_parallel_index
  select number, number, number from numbers(1048576);

select sum(z)
from test_parallel_index
where z = 2 or z = 7 or z = 13 or z = 17 or z = 19 or z = 23;
