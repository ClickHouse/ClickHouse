drop table if exists nums_in_mem;
drop table if exists nums_in_mem_dist;

create table nums_in_mem(v UInt64) engine=Memory;
insert into nums_in_mem select * from system.numbers limit 1000000;

create table nums_in_mem_dist as nums_in_mem engine=Distributed('test_shard_localhost', currentDatabase(), nums_in_mem);

set prefer_localhost_replica = 0;
set max_rows_to_read = 100;

select
  count()
    /
  (select count() from nums_in_mem_dist where rand() > 0)
from system.one; -- { serverError TOO_MANY_ROWS }

drop table nums_in_mem;
drop table nums_in_mem_dist;
