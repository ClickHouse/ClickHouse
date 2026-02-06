-- Tags: no-debug, no-debug, no-asan, no-tsan, no-msan, no-ubsan, no-sanitize-coverage, no-parallel-replicas, no-flaky-check
-- - debug build adds CheckTokenTransform
-- - no-parallel-replicas - has --replace-log-memory-with-mergetree switch

drop table if exists t_log;
drop table if exists t_mt;
drop table if exists mv;

set max_threads=1;
set max_insert_threads=1;
set deduplicate_blocks_in_dependent_materialized_views=0;

-- { echo }

-- Log does not support sparse columns - RemovingSparseTransform added
create table t_log (key Int) engine=Log;
explain pipeline insert into t_log select * from system.one;

-- MergeTree support sparse columns - no RemovingSparseTransform
create table t_mt (key Int) engine=MergeTree order by ();
explain pipeline insert into t_mt select * from system.one;

-- MergeTree pushes to Log, which does not support sparse columns - RemovingSparseTransform added
create materialized view mv to t_log as select * from t_mt;
explain pipeline insert into t_mt select * from system.one;
drop table mv;

-- Log does not support sparse columns - RemovingSparseTransform added
create materialized view mv to t_mt as select * from t_log;
explain pipeline insert into t_log select * from system.one;
