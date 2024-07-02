SET allow_experimental_analyzer=0; -- FIXME: analyzer is not supported yet

drop table if exists data_r1;
drop table if exists data_r2;
drop table if exists data_r3;
drop table if exists data_r4;

-- { echoOn }
create table data_r1 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '1') order by key partition by (part, key % 2) settings cluster=1, cluster_replication_factor=2;
create table data_r2 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '2') order by key partition by (part, key % 2) settings cluster=1, cluster_replication_factor=2;
select throwIf(is_readonly = 1) from system.replicas where database = currentDatabase() and table like 'data_%' format Null;
insert into data_r1 select number key, intDiv(number, 10) part, number value from numbers(100);

system sync replica data_r1 cluster;
system sync replica data_r2 cluster;

select count() from data_r1;
select count() from data_r2;

create table data_r3 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '3') order by key partition by (part, key % 2) settings cluster=1, cluster_replication_factor=2;
create table data_r4 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '4') order by key partition by (part, key % 2) settings cluster=1, cluster_replication_factor=2;
select throwIf(is_readonly = 1) from system.replicas where database = currentDatabase() and table like 'data_%' format Null;

system sync replica data_r1 cluster;
system sync replica data_r2 cluster;
system sync replica data_r3 cluster;
system sync replica data_r4 cluster;

-- FIXME: we need to sync cluster twice to apply all changes
system sync replica data_r4 cluster;
system sync replica data_r3 cluster;
system sync replica data_r2 cluster;
system sync replica data_r1 cluster;

select partition, length(arraySort(active_replicas)) from system.cluster_partitions where database = currentDatabase() and table = 'data_r1' order by 1;
select _table, count(), length(groupArrayDistinct(_partition_id)) size from merge(currentDatabase(), '^data_') group by _table order by 1 settings cluster_query_shards=0;
select _table, count(), length(groupArrayDistinct(_partition_id)) size from merge(currentDatabase(), '^data_') group by _table order by 1 settings cluster_query_shards=1;
