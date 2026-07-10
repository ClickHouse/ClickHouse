-- Tags: no-parallel, no-replicated-database, no-async-insert, no-parallel-replicas, no-s3-storage, no-fasttest
-- no-parallel: checks thread count, which can be affected by concurrent queries
-- no-replicated-database: query_log lookup assumes single-node execution
-- no-async-insert: test measures synchronous INSERT pipeline threading
-- no-parallel-replicas: parallel replicas settings alter query execution plans and thread allocation
-- no-s3-storage: S3 I/O threads inflate peak_threads_usage beyond the pipeline thread count
-- no-fasttest: needs the materialized view path to exercise the with-MV code branch

-- Regression test for https://github.com/ClickHouse/ClickHouse/pull/102928.
-- Issue: https://github.com/ClickHouse/ClickHouse/issues/102947
--
-- When materialized views are involved, the INSERT pipeline asks for `max_threads`
-- because downstream view selects may need full parallelism. Without lazy allocation
-- in ConcurrencyControl, the scheduler eagerly grants all `max_threads` slots, even
-- when the actual pipeline pushes only a tiny amount of parallel work.
--
-- Pre-fix: with `max_threads=10, max_insert_threads=1, parallel_view_processing=0`
-- the pipeline reserves ~10 CPU slots and `peak_threads_usage` is close to 10.
-- Post-fix (lazy CC): only the slots the pipeline actually needs are granted, and
-- `peak_threads_usage` stays small.

drop table if exists test_lazy_cc_src;
drop table if exists test_lazy_cc_mv_target;
drop view if exists test_lazy_cc_mv;

create table test_lazy_cc_src (x UInt64) engine = MergeTree order by tuple();
create table test_lazy_cc_mv_target (x UInt64) engine = MergeTree order by tuple();
create materialized view test_lazy_cc_mv to test_lazy_cc_mv_target as select x from test_lazy_cc_src;

-- INSERT with a materialized view, serial view processing.
-- Pipeline asks for max_threads (because isViewsInvolved()), but the actual
-- parallel work is small. Lazy CC keeps peak_threads_usage low.
-- Note: `concurrent_threads_lazy_allocation` is a server-level setting (default true).
-- This test relies on its default value and does not change it per-query.
insert into test_lazy_cc_src select number from numbers(1000) settings
    max_threads = 10,
    max_insert_threads = 1,
    parallel_view_processing = 0,
    use_concurrency_control = 1,
    log_queries = 1;

system flush logs query_log;

-- Pre-fix: peak_threads_usage was close to max_threads (10). Post-fix: small.
-- The threshold 4 is generous to absorb async readers and tracing overhead.
select if(peak_threads_usage <= 4, 'FEW THREADS', concat('TOO MANY THREADS: ', toString(peak_threads_usage)))
from system.query_log
where event_date >= yesterday()
    and event_time >= now() - 600
    and current_database = currentDatabase()
    and type = 'QueryFinish'
    and query like '%insert into test_lazy_cc_src%';

drop view test_lazy_cc_mv;
drop table test_lazy_cc_mv_target;
drop table test_lazy_cc_src;
