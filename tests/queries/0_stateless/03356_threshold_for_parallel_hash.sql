create table lhs(a UInt64) Engine=MergeTree order by ();
create table rhs(a UInt64) Engine=MergeTree order by ();

insert into lhs select * from numbers_mt(1e5);
insert into rhs select * from numbers_mt(1e6);

set enable_parallel_replicas = 0; -- join optimization (and table size estimation) disabled with parallel replicas
set enable_analyzer = 1, use_query_condition_cache = 0;

set join_algorithm = 'direct,parallel_hash,hash'; -- default
set parallel_hash_join_threshold = 100001;

-- Tables should be swapped; the new right table is below the threshold - use HashJoin
select trimBoth(explain)
from (
  explain actions=1 select * from lhs t0 join rhs t1 on t0.a = t1.a settings query_plan_join_swap_table = 'auto'
)
where explain ilike '%Algorithm%';

-- Tables were not swapped; the right table is above the threshold - use ConcurrentHashJoin
select trimBoth(explain)
from (
  explain actions=1 select * from lhs t0 join rhs t1 on t0.a = t1.a settings query_plan_join_swap_table = false
)
where explain ilike '%Algorithm%';

-- Check estimations obtained from the cache
-- Tables should be swapped; the new right table is below the threshold - use HashJoin
select trimBoth(explain)
from (
  explain actions=1 select * from lhs t0 join rhs t1 on t0.a = t1.a settings query_plan_join_swap_table = true
)
where explain ilike '%Algorithm%';

-- Same queries but we cannot do fallback to `hash`
set join_algorithm = 'parallel_hash';

select trimBoth(explain)
from (
  explain actions=1 select * from lhs t0 join rhs t1 on t0.a = t1.a settings query_plan_join_swap_table = 'auto'
)
where explain ilike '%Algorithm%';

select trimBoth(explain)
from (
  explain actions=1 select * from lhs t0 join rhs t1 on t0.a = t1.a settings query_plan_join_swap_table = false
)
where explain ilike '%Algorithm%';

select trimBoth(explain)
from (
  explain actions=1 select * from lhs t0 join rhs t1 on t0.a = t1.a settings query_plan_join_swap_table = true
)
where explain ilike '%Algorithm%';

set join_algorithm = 'direct,parallel_hash,hash'; -- default

-- Check estimations obtained from the cache

-- Right table is big, regardless of cardinality of join key, we should use ConcurrentHashJoin
select * from lhs t0 join (select a % 10000 as a from rhs) t1 on t0.a = t1.a settings query_plan_join_swap_table = false format Null;

select trimBoth(explain)
from (
  explain actions=1 select * from lhs t0 join (select a % 10000 as a from rhs) t1 on t0.a = t1.a settings query_plan_join_swap_table = false
)
where explain ilike '%Algorithm%';

-- Right table is big, but only a small fraction of rows reaches the join - use HashJoin
select * from lhs t0 join rhs t1 on t0.a = t1.a where t1.a < 10000 settings query_plan_join_swap_table = false format Null;

select trimBoth(explain)
from (
  explain actions=1 select * from lhs t0 join rhs t1 on t0.a = t1.a where t1.a < 10000 settings query_plan_join_swap_table = false
)
where explain ilike '%Algorithm%';

