-- Tags: no-parallel-replicas, no-parallel
-- Test depends on mark cache, don't run with others in parallel

-- Note: we need to have index_granularity==number or rows, to avoid processing
-- parts in parallel, since in this case marks can be requested from multiple
-- threads, which will lead to the query will have both MarksTasksFromCache and
-- BackgroundLoadingMarksTasks
create table data (key int) engine=MergeTree() order by key settings prewarm_mark_cache=0, index_granularity=1000;
insert into data select * from numbers(1000);

select * from data settings load_marks_asynchronously=1 format Null /* 1 */;
select * from data settings load_marks_asynchronously=1 format Null /* 2 */;

system flush logs query_log;
select query, ProfileEvents['BackgroundLoadingMarksTasks']>0 async, ProfileEvents['MarksTasksFromCache']>0 sync
from system.query_log
where current_database = currentDatabase() and query_kind = 'Select' and type != 'QueryStart'
order by event_time_microseconds
format Vertical;
