-- Tags: no-parallel-replicas

drop table if exists data;
create table data (key Int) engine=MergeTree() order by () settings prewarm_mark_cache=0;

set load_marks_asynchronously=0;

insert into data values (1);
--
-- SELECTs
--
select * from data format Null settings load_marks_asynchronously=0;
select * from data format Null settings load_marks_asynchronously=0;
-- drop marks cache
detach table data;
attach table data;
select * from data format Null settings load_marks_asynchronously=1;
select * from data format Null settings load_marks_asynchronously=1;

system flush logs query_log;
select query_kind, Settings['load_marks_asynchronously'] load_marks_asynchronously, ProfileEvents['MarkCacheHits'] hits, ProfileEvents['MarkCacheMisses'] misses
  from system.query_log
  where current_database = currentDatabase() and query_kind in ('Select', 'Insert') and type != 'QueryStart'
  order by event_time_microseconds
  format CSVWithNames;

--
-- metrics for merges
--
-- only hits
optimize table data final;
-- drop marks cache to trigger misses
detach table data;
attach table data;
optimize table data final;

system flush logs part_log;
select part_name, ProfileEvents['MarkCacheHits'] hits, ProfileEvents['MarkCacheMisses'] misses
  from system.part_log
  where database = currentDatabase() and event_type = 'MergeParts'
  order by event_time_microseconds
  format CSVWithNames;
