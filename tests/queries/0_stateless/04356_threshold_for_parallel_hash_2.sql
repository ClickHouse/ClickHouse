-- The opposite direction of 05056_join_hash_stats_cache_serial_layout:
-- switching from the serial to the parallel layout based on runtime statistics.
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0; -- Disable automatic spilling for this test

create table lhs(a UInt64) Engine=MergeTree order by ();
create table rhs(a UInt64) Engine=MergeTree order by ();

insert into lhs select * from numbers_mt(1e5);
insert into rhs select * from numbers_mt(1e6);

set enable_parallel_replicas = 0; -- join optimization (and table size estimation) disabled with parallel replicas
set enable_analyzer = 1, use_query_condition_cache = 0;
set query_plan_optimize_join_order_limit = 10; -- CI may inject 0; chooseJoinOrder skipped → estimation does not run
SET query_plan_optimize_join_order_randomize = 0;

SET use_statistics = 0; -- statistics does not override estimation from cache

set join_algorithm = 'hash';
set parallel_hash_join_threshold = 100001;
set max_threads = 4;

-- Pretend the right table is small so the first run is planned with the serial layout, even though it actually has 1e6 rows.
set param__internal_join_table_stat_hints = '{ "rhs": { "cardinality": 50000 } }';

-- First run: the estimate comes from the hint (below the threshold) - serial layout.
-- The join records the real right-side size into the statistics cache.
select * from lhs t0 join rhs t1 on t0.a = t1.a settings query_plan_join_swap_table = false, query_plan_optimize_join_order_limit = 10, log_comment = '04356_hint' format Null;

-- Second run: the cached size (1e6) overrides the hint - parallel layout.
select * from lhs t0 join rhs t1 on t0.a = t1.a settings query_plan_join_swap_table = false, query_plan_optimize_join_order_limit = 10, log_comment = '04356_cache' format Null;

system flush logs query_log;

select log_comment, ProfileEvents['HashJoinBuiltWithSerialLayout'] as serial, ProfileEvents['HashJoinBuiltWithParallelLayout'] as parallel
from system.query_log
where current_database = currentDatabase() and type = 'QueryFinish' and log_comment in ('04356_hint', '04356_cache')
order by event_time_microseconds;
