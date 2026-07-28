-- Tags: no-tsan, no-asan, no-msan, no-ubsan, no-parallel-replicas

drop table if exists lhs;
drop table if exists rhs;

create table lhs(a UInt64, b UInt64) Engine = MergeTree order by tuple();
create table rhs(a UInt64, b UInt64) Engine = MergeTree order by tuple();

insert into lhs select number, number from numbers_mt(2e5);
-- rhs should be bigger to trigger tables swap (see `query_plan_join_swap_table`)
insert into rhs select number, number from numbers_mt(1e6);

set max_threads = 8, query_plan_join_swap_table = 1, join_algorithm = 'parallel_hash', enable_analyzer = 1;

-- First populate the cache of hash table sizes
select * from lhs as t1 join rhs as t2 on t1.a = t2.a format Null;

-- For the next run we will preallocate the space
select * from lhs as t1 join rhs as t2 on t1.a = t2.a format Null settings log_comment = '03319_second_query';

system flush logs query_log;

select ProfileEvents['HashJoinPreallocatedElementsInHashTables']
from system.query_log
where event_date >= yesterday() and current_database = currentDatabase() and type = 'QueryFinish' and log_comment = '03319_second_query';
