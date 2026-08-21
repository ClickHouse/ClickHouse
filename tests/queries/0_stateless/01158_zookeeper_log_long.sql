-- Tags: long, zookeeper, no-replicated-database, no-polymorphic-parts, no-random-merge-tree-settings, no-shared-merge-tree, no-async-insert
-- Tag no-replicated-database: Fails due to additional replicas or shards
-- no-shared-merge-tree: depends on structure in zookeeper of replicated merge tree
-- no-async-insert: Test expects new part for each insert

SET insert_keeper_fault_injection_probability=0; -- disable fault injection; part ids are non-deterministic in case of insert retries

drop table if exists rmt sync;
-- cleanup code will perform extra Exists
-- (so the .reference will not match)
create table rmt (n int) engine=ReplicatedMergeTree('/test/01158/{database}/rmt', '1')
    order by n
    settings
        cleanup_delay_period=86400,
        max_cleanup_delay_period=86400,
        replicated_can_become_leader=0;
system sync replica rmt;
insert into rmt values (1);
insert into rmt values (1);
system sync replica rmt;
system flush logs zookeeper_log, query_log;

select 'log';
select address, type, has_watch, op_num, path, is_ephemeral, is_sequential, version, requests_size, request_idx, error, watch_type,
       watch_state, path_created, stat_version, stat_cversion, stat_dataLength, stat_numChildren
from system.zookeeper_log where path like '/test/01158/' || currentDatabase() || '/rmt/log%' and op_num not in (3, 4, 12, 500)
order by xid, type, request_idx;

select 'parts';
with now() - interval 1 hour as cutoff_time,
query_ids as
(
    select query_id from system.query_log where current_database=currentDatabase() and event_time>=cutoff_time
)
select type, has_watch, op_num, replace(path, toString(serverUUID()), '<uuid>'), is_ephemeral, is_sequential, if(startsWith(path, '/clickhouse/sessions'), 1, version), requests_size, request_idx, error, watch_type,
       watch_state, path_created, stat_version, stat_cversion, stat_dataLength, stat_numChildren
from system.zookeeper_log
where event_time>=cutoff_time and (session_id, xid) in (
    select session_id, xid from system.zookeeper_log where event_time>=cutoff_time
    and path='/test/01158/' || currentDatabase() || '/rmt/replicas/1/parts/all_0_0_0'
    and (query_id='' or query_id in query_ids)
)
order by xid, type, request_idx;

select 'blocks';

with now() - interval 1 hour as cutoff_time,
query_ids as
(
    select query_id from system.query_log where current_database=currentDatabase() and event_time>=cutoff_time
)
select type, has_watch, op_num, path, is_ephemeral, is_sequential, version, requests_size, request_idx, error, watch_type,
       watch_state, path_created, stat_version, stat_cversion, stat_dataLength, stat_numChildren
from system.zookeeper_log
where event_time>=cutoff_time and (session_id, xid) in (
    select session_id, xid from system.zookeeper_log where event_time>=cutoff_time
    and path like '/test/01158/' || currentDatabase() || '/rmt/blocks/%'
    and op_num not in (1, 12, 500)
    and (query_id='' or query_id in query_ids)
)
order by xid, type, request_idx;

drop table rmt sync;

system flush logs zookeeper_log;
select 'duration_microseconds';
select count()>0 from system.zookeeper_log where path like '/test/01158/' || currentDatabase() || '/rmt%' and duration_microseconds > 0;

system flush logs aggregated_zookeeper_log;
select 'aggregated_zookeeper_log';
select sum(errors[0]) > 0, sum(average_latency) > 0 from system.aggregated_zookeeper_log where parent_path = '/test/01158/' || currentDatabase() || '/rmt' and operation = 'Create';
