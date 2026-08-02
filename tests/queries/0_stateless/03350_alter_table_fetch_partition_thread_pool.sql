-- Tags: no-parallel, no-replicated-database, no-shared-merge-tree
-- Tag: no-parallel - to avoid polluting FETCH PARTITION thread pool with other fetches
-- Tag: no-replicated-database - replica_path is different

drop table if exists data1;
drop table if exists data2;

create table data1 (key Int) engine=ReplicatedMergeTree('/tables/{database}/{table}', 'r1') order by ();
create table data2 (key Int) engine=ReplicatedMergeTree('/tables/{database}/{table}', 'r1') order by ();

system stop merges data1;
insert into data1 select * from numbers(100) settings max_block_size=1, min_insert_block_size_rows=1;
select 'parts in data1', count() from system.parts where database = currentDatabase() and table = 'data1';

alter table data2 fetch partition () from '/tables/{database}/data1';
select 'detached parts in data2', count() from system.detached_parts where database = currentDatabase() and table = 'data2';

system flush logs query_log;
select 'FETCH PARTITION uses multiple threads', peak_threads_usage>10 from system.query_log where event_date >= yesterday() and type != 'QueryStart' and query_kind = 'Alter' and current_database = currentDatabase();
