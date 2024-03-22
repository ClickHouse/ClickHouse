-- Tags: no-replicated-database, no-parallel, no-shared-merge-tree
-- SMT: The merge process is completely different from RMT

drop table if exists rmt_master;
drop table if exists rmt_slave;

create table rmt_master (key Int) engine=ReplicatedMergeTree('/clickhouse/{database}', 'master') order by tuple() settings always_fetch_merged_part=0;
-- prefer_fetch_merged_part_*_threshold=0, consider this table as a "slave"
create table rmt_slave (key Int) engine=ReplicatedMergeTree('/clickhouse/{database}', 'slave') order by tuple() settings prefer_fetch_merged_part_time_threshold=0, prefer_fetch_merged_part_size_threshold=0;

insert into rmt_master values (1);

system sync replica rmt_master;
system sync replica rmt_slave;
system stop replicated sends rmt_master;
system stop pulling replication log rmt_slave;
alter table rmt_master update key=key+100 where 1 settings alter_sync=1;

-- first we need to make the rmt_master execute mutation so that it will have
-- the part, and rmt_slave will consider it instead of performing mutation on
-- it's own, otherwise prefer_fetch_merged_part_*_threshold will be simply ignored
select sleep(3) format Null;
system start pulling replication log rmt_slave;
-- and sleep few more seconds to try rmt_slave to fetch the part and reflect
-- this error in system.part_log
select sleep(3) format Null;

system flush logs;
select 'before';
select table, event_type, error>0, countIf(error=0) from system.part_log where database = currentDatabase() group by 1, 2, 3 order by 1, 2, 3;

system start replicated sends rmt_master;
select sleep(3) format Null;
system sync replica rmt_slave;

system flush logs;
select 'after';
select table, event_type, error>0, countIf(error=0) from system.part_log where database = currentDatabase() group by 1, 2, 3 order by 1, 2, 3;

drop table rmt_master;
drop table rmt_slave;
