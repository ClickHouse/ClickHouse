-- Tags: no-asan, no-tsan
create table adaptive_spill_03277_1 (`k` String, `x` String ) Engine=Memory;
create table adaptive_spill_03277_2 (`k` String, `x` String ) Engine=Memory;
create table adaptive_spill_03277_3 (`k` String, `x` String ) Engine=Memory;

insert into adaptive_spill_03277_1 select cast(rand() as String) as k, cast(rand() as String) as x from numbers(1000000);
insert into adaptive_spill_03277_2 select cast(rand() as String) as k, cast(rand() as String) as x from numbers(1000000);
insert into adaptive_spill_03277_3 select cast(rand() as String) as k, cast(rand() as String) as x from numbers(1000000);

set max_threads=1;
set join_algorithm='grace_hash';
-- 220 MiB. Two grace buckets of 1M String rows each are alive during the second probe; without the adaptive
-- scheduler nothing rebuckets on its own and the query peaks at about 271 MiB (measured, both arms unlimited),
-- so this limit must fail it; with the scheduler the peak stays at 118-133 MiB under limits of 180-240 MB, so the
-- same limit must pass. The former 300 MiB fitted the old `HashJoin` buckets, whose key arena grew in doubling
-- chunks and whose hash buffer doubled in place, about 40 MiB more than the exactly sized bucket of today.
set max_memory_usage=230686720;
set enable_parallel_replicas=0; -- parallel replicas distribute data across nodes, reducing per-node memory and preventing the expected OOM
set grace_hash_join_initial_buckets=1; -- more initial buckets split the right side, reducing per-bucket memory and preventing the expected OOM
set collect_hash_table_stats_during_joins=0;
-- don't limit the memory usage for join
set max_bytes_in_join=0;
-- keep the join's own spill threshold out of reach, so that only the scheduler can trigger spilling
set max_bytes_before_external_join='100Gi';
set max_bytes_ratio_before_external_join=0;

set enable_adaptive_memory_spill_scheduler=false;
select t1.k, t2.x, t3.x from adaptive_spill_03277_1 as t1 left join adaptive_spill_03277_2 as t2 on t1.k = t2.k left join adaptive_spill_03277_3 as t3 on t1.k = t3.k Format Null; --{serverError MEMORY_LIMIT_EXCEEDED}

set enable_adaptive_memory_spill_scheduler=true;
select t1.k, t2.x, t3.x from adaptive_spill_03277_1 as t1 left join adaptive_spill_03277_2 as t2 on t1.k = t2.k left join adaptive_spill_03277_3 as t3 on t1.k = t3.k Format Null;

drop table if exists adaptive_spill_03277_1;
drop table if exists adaptive_spill_03277_2;
drop table if exists adaptive_spill_03277_3;
