-- Tags: no-parallel-replicas, no-object-storage

set min_bytes_to_use_direct_io = 0;

drop table if exists 03595_data;

create table 03595_data (key UInt32, val String) engine = MergeTree order by key
as
select number, 'val-' || number from numbers(100000);

select * from 03595_data
format Null
settings
    local_filesystem_read_method = 'pread_threadpool',
    min_bytes_to_use_direct_io = 1,
    log_query_threads = 1,
    use_uncompressed_cache = 0;

-- If previous query was running w/o O_DIRECT (due to some bug) it may fill pagecache,
-- and then subsequent query will read from cache (if it will ignore O_DIRECT flag as well) with RWF_NOWAIT,
-- so let's make sure that this is not the case
select * from 03595_data
format Null
settings
    local_filesystem_read_method = 'pread_threadpool',
    min_bytes_to_use_direct_io = 1,
    log_query_threads = 1,
    use_uncompressed_cache = 0;

system flush logs query_log, query_thread_log;

with queries as (
    select query_id, row_number() over(order by event_time_microseconds) as ordinal
    from system.query_log
    where type = 'QueryFinish'
     and current_database = currentDatabase()
     and query_kind = 'Select'
     and Settings['min_bytes_to_use_direct_io'] = '1'
)
select queries.ordinal, thread_name, sum(ProfileEvents['OSReadBytes']) > 0 as disk_reading
from system.query_thread_log qtl
    join queries
        on queries.query_id = qtl.query_id
where current_database = currentDatabase()
  and query_id in (select query_id from queries)
  and ProfileEvents['OSReadBytes'] > 0
group by 1, 2
order by 1, 2;

drop table 03595_data;
