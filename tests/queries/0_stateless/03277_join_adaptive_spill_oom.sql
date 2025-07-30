-- Tags: no-asan, no-tsan
create table adaptive_spill_03277_1 (`k` String, `x` String ) Engine=Memory;
create table adaptive_spill_03277_2 (`k` String, `x` String ) Engine=Memory;
create table adaptive_spill_03277_3 (`k` String, `x` String ) Engine=Memory;

insert into adaptive_spill_03277_1 select cast(rand() as String) as k, cast(rand() as String) as x from numbers(1000000);
insert into adaptive_spill_03277_2 select cast(rand() as String) as k, cast(rand() as String) as x from numbers(1000000);
insert into adaptive_spill_03277_3 select cast(rand() as String) as k, cast(rand() as String) as x from numbers(1000000);

set max_threads=1;
set join_algorithm='grace_hash';
set max_memory_usage=314572800;
set collect_hash_table_stats_during_joins=0;
-- don't limit the memory usage for join
set max_bytes_in_join=0;

set enable_adaptive_memory_spill_scheduler=false;
select t1.k, t2.x, t3.x from adaptive_spill_03277_1 as t1 left join adaptive_spill_03277_2 as t2 on t1.k = t2.k left join adaptive_spill_03277_3 as t3 on t1.k = t3.k Format Null; --{serverError MEMORY_LIMIT_EXCEEDED}

set enable_adaptive_memory_spill_scheduler=true;
select t1.k, t2.x, t3.x from adaptive_spill_03277_1 as t1 left join adaptive_spill_03277_2 as t2 on t1.k = t2.k left join adaptive_spill_03277_3 as t3 on t1.k = t3.k Format Null;

drop table if exists adaptive_spill_03277_1;
drop table if exists adaptive_spill_03277_2;
drop table if exists adaptive_spill_03277_3;
