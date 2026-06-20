-- Tags: no-tsan, no-asan, no-msan, no-ubsan, no-parallel-replicas

drop table if exists lhs;
drop table if exists rhs;

create table lhs(a UInt64, b UInt64) Engine = MergeTree order by tuple();
create table rhs(a UInt64, b UInt64) Engine = MergeTree order by tuple();

insert into lhs select number, number from numbers_mt(2e5);
-- rhs should be bigger to trigger tables swap (see `query_plan_join_swap_table`)
insert into rhs select number, number from numbers_mt(1e6);

set max_threads = 8, query_plan_join_swap_table = 1, join_algorithm = 'parallel_hash', enable_analyzer = 1, enable_join_runtime_filters = 0, query_plan_read_in_order_through_join = 0, query_plan_optimize_join_order_limit = 10; -- runtime filters and read-in-order-through-join change execution path, bypassing hash table preallocation; CI may inject join_order_limit=0, skipping chooseJoinOrder so join swap never happens and preallocation cache key mismatches

-- First populate the cache of hash table sizes
select * from lhs as t1 join rhs as t2 on t1.a = t2.a format Null;

-- For the next run we will preallocate the space
-- Inline SETTINGS resets all unlisted settings to CLI defaults, so repeat the critical session settings here:
-- CI injects enable_join_runtime_filters=True (bypasses preallocation), query_plan_join_swap_table=false, max_threads=1
select * from lhs as t1 join rhs as t2 on t1.a = t2.a format Null settings log_comment = '03319_second_query', join_algorithm = 'parallel_hash', enable_join_runtime_filters = 0, query_plan_read_in_order_through_join = 0, query_plan_join_swap_table = 1, max_threads = 8, query_plan_optimize_join_order_limit = 10; -- CI may inject 0; chooseJoinOrder skipped, swap not applied, cache key mismatches first query → preallocation returns 0

system flush logs query_log;

select ProfileEvents['HashJoinPreallocatedElementsInHashTables']
from system.query_log
where event_date >= yesterday() AND event_time >= now() - 600 and current_database = currentDatabase() and type = 'QueryFinish' and log_comment = '03319_second_query';
