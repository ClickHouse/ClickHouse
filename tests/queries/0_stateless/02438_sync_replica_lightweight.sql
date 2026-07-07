-- Tags: no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database: different number of replicas
-- Tag no-shared-merge-tree: sync replica lightweight by default

-- May affect part names
set prefer_warmed_unmerged_parts_seconds=0;
set ignore_cold_parts_seconds=0;

create table rmt1 (n int) engine=ReplicatedMergeTree('/test/{database}/02438/', '1') order by tuple() settings cache_populated_by_fetch=0;
create table rmt2 (n int) engine=ReplicatedMergeTree('/test/{database}/02438/', '2') order by tuple() settings cache_populated_by_fetch=0;

system stop replicated sends rmt1;
system stop merges rmt2;

set insert_keeper_fault_injection_probability=0;

insert into rmt1 values (1);
insert into rmt1 values (2);
system sync replica rmt2 pull;  -- does not wait
select type, new_part_name from system.replication_queue where database=currentDatabase() and table='rmt2' order by new_part_name;
select 1, n, _part from rmt1 order by n;
select 2, n, _part from rmt2 order by n;

set optimize_throw_if_noop = 1;
system sync replica rmt1 pull;
optimize table rmt1 final;

system start replicated sends rmt1;
system sync replica rmt2 lightweight;   -- waits for fetches, not merges
select type, new_part_name from system.replication_queue where database=currentDatabase() and table='rmt2' order by new_part_name;
select 3, n, _part from rmt1 order by n;
select 4, n from rmt2 order by n;
select type, new_part_name from system.replication_queue where database=currentDatabase() and table='rmt2' order by new_part_name;

system start merges rmt2;
system sync replica rmt2;

insert into rmt2 values (3);
system sync replica rmt2 pull;
optimize table rmt2 final;

system sync replica rmt1 strict;

select 5, n, _part from rmt1 order by n;
select 6, n, _part from rmt2 order by n;

drop table rmt1;
drop table rmt2;
