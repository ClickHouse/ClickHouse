drop table if exists data_r1;
drop table if exists data_r2;
drop table if exists data_r3;
drop table if exists data_r4;

create table data_r1 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '1') order by key partition by part settings cluster=1, cluster_replication_factor=2;
create table data_r2 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '2') order by key partition by part settings cluster=1, cluster_replication_factor=2;
select throwIf(is_readonly = 1) from system.replicas where database = currentDatabase() and table like 'data_%' format Null;

insert into data_r1 select number key, number%10 part, number value from numbers(100000);
system sync replica data_r1 cluster;
system sync replica data_r2 cluster;
select replica, arraySort(groupArray(partition)) from system.cluster_partitions array join active_replicas as replica where database = currentDatabase() and table = 'data_r1' group by 1 order by 1;

create table data_r3 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '3') order by key partition by part settings cluster=1, cluster_replication_factor=2;
create table data_r4 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '4') order by key partition by part settings cluster=1, cluster_replication_factor=2;
system sync replica data_r4 cluster;
system sync replica data_r3 cluster;
select replica, length(groupArray(partition)) from system.cluster_partitions array join active_replicas as replica where database = currentDatabase() and table = 'data_r3' group by 1 order by 1;

-- TODO: force balancing on data_r1/data_r2, otherwise this test takes 5 (DISTRIBUTOR_NO_JOB_DELAY_MS) seconds
drop table data_r3;
drop table data_r4;
system sync replica data_r2 cluster;
system sync replica data_r1 cluster;
select replica, arraySort(groupArray(partition)) from system.cluster_partitions array join active_replicas as replica where database = currentDatabase() and table = 'data_r1' group by 1 order by 1;
