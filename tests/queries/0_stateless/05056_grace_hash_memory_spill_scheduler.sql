-- Tags: no-asan, no-tsan
-- The memory spill scheduler should also reach a standalone `grace_hash`: its bucket is repartitioned on
-- request, including when no further right-side rows land in it.
create table grace_spill_05056_1 (`k` String, `x` String) Engine=Memory;
create table grace_spill_05056_2 (`k` String, `x` String) Engine=Memory;

insert into grace_spill_05056_1 select cast(rand() as String) as k, cast(rand() as String) as x from numbers(1000000);
insert into grace_spill_05056_2 select cast(rand() as String) as k, cast(rand() as String) as x from numbers(1000000);

set max_threads=1;
set join_algorithm='grace_hash';
set max_memory_usage=314572800;
set enable_parallel_replicas=0;
-- One bucket, so no row reaches disk unless the join is asked to repartition.
set grace_hash_join_initial_buckets=1;
set collect_hash_table_stats_during_joins=0;
set max_bytes_in_join=0;
-- Put the join's own spill threshold out of reach so that only the scheduler can make it spill.
set max_bytes_before_external_join='100Gi';
set max_bytes_ratio_before_external_join=0;

set enable_adaptive_memory_spill_scheduler=false;
select t1.k, t2.x from grace_spill_05056_1 as t1 left join grace_spill_05056_2 as t2 on t1.k = t2.k Format Null
settings log_comment = 'no_scheduler_05056';

set enable_adaptive_memory_spill_scheduler=true;
select t1.k, t2.x from grace_spill_05056_1 as t1 left join grace_spill_05056_2 as t2 on t1.k = t2.k Format Null
settings log_comment = 'scheduler_spill_05056';

-- Count bytes, not files: every bucket creates its two temporary files up front, empty or not.
system flush logs query_log;
select log_comment, ProfileEvents['ExternalJoinUncompressedBytes'] > 0 from system.query_log
where current_database = currentDatabase() and log_comment in ('no_scheduler_05056', 'scheduler_spill_05056')
    and type = 'QueryFinish' and event_date >= yesterday()
order by log_comment;

-- A spill request is a hint: at the bucket limit it is ignored, not turned into an error.
select 'a spill request at the bucket limit does not fail the query';
select t1.k, t2.x from grace_spill_05056_1 as t1 left join grace_spill_05056_2 as t2 on t1.k = t2.k Format Null
settings join_algorithm = 'hash', grace_hash_join_max_buckets = 1;

drop table if exists grace_spill_05056_1;
drop table if exists grace_spill_05056_2;
