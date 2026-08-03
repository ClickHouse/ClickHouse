-- Tags: no-asan, no-tsan
-- The memory spill scheduler must be able to spill a join that runs under `SpillingHashJoin`,
-- not only a top-level `grace_hash` one.
create table spill_scheduler_04695_1 (`k` String, `x` String) Engine=Memory;
create table spill_scheduler_04695_2 (`k` String, `x` String) Engine=Memory;
create table spill_scheduler_04695_3 (`k` String, `x` String) Engine=Memory;

insert into spill_scheduler_04695_1 select cast(rand() as String) as k, cast(rand() as String) as x from numbers(1000000);
insert into spill_scheduler_04695_2 select cast(rand() as String) as k, cast(rand() as String) as x from numbers(1000000);
insert into spill_scheduler_04695_3 select cast(rand() as String) as k, cast(rand() as String) as x from numbers(1000000);

set max_threads=1;
set join_algorithm='hash';
set max_memory_usage=314572800;
set enable_parallel_replicas=0; -- parallel replicas distribute data across nodes, reducing per-node memory and preventing the expected OOM
set grace_hash_join_initial_buckets=1; -- more initial buckets split the right side, reducing per-bucket memory and preventing the expected OOM
set collect_hash_table_stats_during_joins=0;
set max_bytes_in_join=0;
-- Keep the wrapper's own spill threshold out of reach so that only the scheduler can trigger spilling.
set max_bytes_before_external_join='100Gi';
set max_bytes_ratio_before_external_join=0;

set enable_adaptive_memory_spill_scheduler=false;
select t1.k, t2.x, t3.x from spill_scheduler_04695_1 as t1 left join spill_scheduler_04695_2 as t2 on t1.k = t2.k left join spill_scheduler_04695_3 as t3 on t1.k = t3.k Format Null; --{serverError MEMORY_LIMIT_EXCEEDED}

set enable_adaptive_memory_spill_scheduler=true;
select t1.k, t2.x, t3.x from spill_scheduler_04695_1 as t1 left join spill_scheduler_04695_2 as t2 on t1.k = t2.k left join spill_scheduler_04695_3 as t3 on t1.k = t3.k Format Null;

drop table if exists spill_scheduler_04695_1;
drop table if exists spill_scheduler_04695_2;
drop table if exists spill_scheduler_04695_3;
