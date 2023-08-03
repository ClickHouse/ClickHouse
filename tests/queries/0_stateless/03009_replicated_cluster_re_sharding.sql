drop table if exists data_r1;
drop table if exists data_r2;
drop table if exists data_r3;
drop table if exists data_r4;

-- { echoOn }
create table data_r1 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '1') order by key partition by (part, key % 2) settings cluster=1, cluster_replication_factor=2;
create table data_r2 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '2') order by key partition by (part, key % 2) settings cluster=1, cluster_replication_factor=2;
select throwIf(is_readonly = 1) from system.replicas where database = currentDatabase() and table like 'data_%' format Null;
insert into data_r1 select number key, intDiv(number, 10) part, number value from numbers(100);

system sync replica data_r1;
system sync replica data_r2;

select count() from data_r1;
select count() from data_r2;

create table data_r3 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '3') order by key partition by (part, key % 2) settings cluster=1, cluster_replication_factor=2;
create table data_r4 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '4') order by key partition by (part, key % 2) settings cluster=1, cluster_replication_factor=2;
select throwIf(is_readonly = 1) from system.replicas where database = currentDatabase() and table like 'data_%' format Null;

system sync replica data_r1;
system sync replica data_r2;
system sync replica data_r3;
system sync replica data_r4;

-- TODO: SYSTEM SYNC CLUSTER
select sleepEachRow(1) from numbers(10) format Null settings max_block_size=1;

-- FIXME:
-- - arrayDistinct - what the heck, there should be only distinct values
-- - right now r1/r2 contains all records even after removal due to DISTRIBUTOR_PARTITION_DROP_TTL_SEC = 60
-- TODO: check real replica names
select partition, length(arraySort(arrayDistinct(active_replicas))) from system.cluster_partitions where database = currentDatabase() and table = 'data_r1' order by 1;
select _table, count(), length(groupArrayDistinct(_partition_id)) size from merge(currentDatabase(), '^data_') group by _table order by 1 settings cluster_query_shards=0;
select _table, count(), length(groupArrayDistinct(_partition_id)) size from merge(currentDatabase(), '^data_') group by _table order by 1 settings cluster_query_shards=1;
