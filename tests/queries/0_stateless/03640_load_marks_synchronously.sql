-- Tags: no-parallel-replicas

create table data (key int) engine=MergeTree() order by key settings prewarm_mark_cache=0;
insert into data select * from numbers(1000);

-- Clear marks cache
detach table data;
attach table data;

select * from data settings load_marks_asynchronously=1 format Null /* 1 */;
select * from data settings load_marks_asynchronously=1 format Null /* 2 */;

system flush logs query_log;
select query, ProfileEvents['BackgroundLoadingMarksTasks']>0 async, ProfileEvents['MarksTasksFromCache']>0 sync
from system.query_log
where current_database = currentDatabase() and query_kind = 'Select' and type != 'QueryStart'
order by event_time_microseconds
format Vertical;
