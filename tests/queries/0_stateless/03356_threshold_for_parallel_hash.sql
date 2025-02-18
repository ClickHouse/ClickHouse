create table lhs(a UInt64) Engine=MergeTree order by ();
create table rhs(a UInt64) Engine=MergeTree order by ();

insert into lhs select * from numbers_mt(1e5);
insert into rhs select * from numbers_mt(1e6);

set enable_analyzer = 1;

set join_algorithm = 'direct,parallel_hash,hash'; -- default
set parallel_hash_join_threshold = 100001;

-- Tables should be swapped; the new right table is below the threshold
select trimBoth(explain)
from (
  explain actions=1 select * from lhs t0 join rhs t1 on t0.a = t1.a settings query_plan_join_swap_table = 'auto'
)
where explain ilike '%Algorithm%';

-- Tables were not swapped; the right table is above the threshold
select trimBoth(explain)
from (
  explain actions=1 select * from lhs t0 join rhs t1 on t0.a = t1.a settings query_plan_join_swap_table = false
)
where explain ilike '%Algorithm%';

-- Tables should be swapped; the new right table is below the threshold
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

