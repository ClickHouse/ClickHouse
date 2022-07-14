-- Tags: long, zookeeper, no-replicated-database, no-polymorphic-parts
-- Tag no-replicated-database: Fails due to additional replicas or shards

drop table if exists rmt;
-- cleanup code will perform extra Exists
-- (so the .reference will not match)
create table rmt (n int) engine=ReplicatedMergeTree('/test/01158/{database}/rmt', '1') order by n settings cleanup_delay_period=86400, replicated_can_become_leader=0;
system sync replica rmt;
insert into rmt values (1);
insert into rmt values (1);
system flush logs;

select 'log';
select address, type, has_watch, op_num, path, is_ephemeral, is_sequential, version, requests_size, request_idx, error, watch_type,
       watch_state, path_created, stat_version, stat_cversion, stat_dataLength, stat_numChildren
from system.zookeeper_log where path like '/test/01158/' || currentDatabase() || '/rmt/log%' and op_num not in (3, 4, 12, 500)
order by xid, type, request_idx;

select 'parts';
select type, has_watch, op_num, path, is_ephemeral, is_sequential, version, requests_size, request_idx, error, watch_type,
       watch_state, path_created, stat_version, stat_cversion, stat_dataLength, stat_numChildren
from system.zookeeper_log
where (session_id, xid) in (select session_id, xid from system.zookeeper_log where path='/test/01158/' || currentDatabase() || '/rmt/replicas/1/parts/all_0_0_0')
order by xid, type, request_idx;

select 'blocks';
select type, has_watch, op_num, path, is_ephemeral, is_sequential, version, requests_size, request_idx, error, watch_type,
       watch_state, path_created, stat_version, stat_cversion, stat_dataLength, stat_numChildren
from system.zookeeper_log
where (session_id, xid) in (select session_id, xid from system.zookeeper_log where path like '/test/01158/' || currentDatabase() || '/rmt/blocks%' and op_num not in (1, 12, 500))
order by xid, type, request_idx;

drop table rmt;
