-- The opposite direction of 03356_threshold_for_parallel_hash:
-- switching from `HashJoin` to `ConcurrentHashJoin` based on runtime statistics.
SET explain_query_plan_default = 'legacy';
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

set join_algorithm = 'direct,parallel_hash,hash'; -- default
set parallel_hash_join_threshold = 100001;

-- Pretend the right table is small so the first run is planned as `HashJoin`, even though it actually has 1e6 rows.
set param__internal_join_table_stat_hints = '{ "rhs": { "cardinality": 50000 } }';

-- First run: the estimate comes from the hint (below the threshold) - use `HashJoin`.
select trimBoth(explain)
from (
  explain actions=1 select * from lhs t0 join rhs t1 on t0.a = t1.a settings query_plan_join_swap_table = false, query_plan_optimize_join_order_limit = 10
)
where explain ilike '%Algorithm%';

-- Execute the query so the `HashJoin` records the real right-side size into the statistics cache.
select * from lhs t0 join rhs t1 on t0.a = t1.a settings query_plan_join_swap_table = false, query_plan_optimize_join_order_limit = 10 format Null;

-- Second run: the cached size (1e6) overrides the hint - use `ConcurrentHashJoin`.
select trimBoth(explain)
from (
  explain actions=1 select * from lhs t0 join rhs t1 on t0.a = t1.a settings query_plan_join_swap_table = false, query_plan_optimize_join_order_limit = 10
)
where explain ilike '%Algorithm%';
