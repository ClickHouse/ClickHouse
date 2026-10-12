SET query_plan_optimize_join_order_randomize = 0; -- Pinned because the test asserts on join plan/order
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0; -- Disable automatic spilling for this test

create table lhs(a UInt64) Engine=MergeTree order by ();
create table rhs(a UInt64) Engine=MergeTree order by ();

insert into lhs select * from numbers_mt(1e5);
insert into rhs select * from numbers_mt(1e6);

set enable_parallel_replicas = 0; -- join optimization (and table size estimation) disabled with parallel replicas
set enable_analyzer = 1, use_query_condition_cache = 0;
set query_plan_optimize_join_order_limit = 10; -- CI may inject 0; chooseJoinOrder skipped → join swap never happens → right table stays large → parallel layout instead of serial
set max_threads = 4;

set join_algorithm = 'hash';
set parallel_hash_join_threshold = 100001;

-- Tables should be swapped; the new right table is below the threshold - serial layout
select * from lhs t0 join rhs t1 on t0.a = t1.a settings query_plan_join_swap_table = 'auto', query_plan_optimize_join_order_limit = 10, log_comment = '03356_swap_auto' format Null;

-- Tables were not swapped; the right table is above the threshold - parallel layout
select * from lhs t0 join rhs t1 on t0.a = t1.a settings query_plan_join_swap_table = false, query_plan_optimize_join_order_limit = 10, log_comment = '03356_swap_false' format Null;

-- Check estimations obtained from the cache
-- Tables should be swapped; the new right table is below the threshold - serial layout
select * from lhs t0 join rhs t1 on t0.a = t1.a settings query_plan_join_swap_table = true, query_plan_optimize_join_order_limit = 10, log_comment = '03356_swap_true' format Null;

-- Right table is big, regardless of cardinality of join key - parallel layout, on a cold run and with its size in the cache
select * from lhs t0 join (select a % 10000 as a from rhs) t1 on t0.a = t1.a settings query_plan_join_swap_table = false, query_plan_optimize_join_order_limit = 10, log_comment = '03356_modulo_cold' format Null;
select * from lhs t0 join (select a % 10000 as a from rhs) t1 on t0.a = t1.a settings query_plan_join_swap_table = false, query_plan_optimize_join_order_limit = 10, log_comment = '03356_modulo_warm' format Null;

system flush logs query_log;

select log_comment, ProfileEvents['HashJoinBuiltWithSerialLayout'] as serial, ProfileEvents['HashJoinBuiltWithParallelLayout'] as parallel
from system.query_log
where current_database = currentDatabase() and type = 'QueryFinish'
    and log_comment in ('03356_swap_auto', '03356_swap_false', '03356_swap_true', '03356_modulo_cold', '03356_modulo_warm')
order by event_time_microseconds;
