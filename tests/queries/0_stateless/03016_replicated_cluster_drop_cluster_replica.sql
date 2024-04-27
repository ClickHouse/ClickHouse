SET allow_experimental_analyzer=0; -- FIXME: analyzer is not supported yet

drop table if exists data_r1;
drop table if exists data_r2;
drop table if exists data_r3;
drop table if exists data_r4;

create table data_r1 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '1') order by key partition by part settings cluster=1, cluster_replication_factor=2;
create table data_r2 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '2') order by key partition by part settings cluster=1, cluster_replication_factor=2;
create table data_r3 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '3') order by key partition by part settings cluster=1, cluster_replication_factor=2;
create table data_r4 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '4') order by key partition by part settings cluster=1, cluster_replication_factor=2;
select throwIf(is_readonly = 1) from system.replicas where database = currentDatabase() and table like 'data_%' format Null;

insert into data_r1 select number key, intDiv(number, 10) part, number value from numbers(100);

system sync replica data_r1;
system sync replica data_r2;
system sync replica data_r3;
system sync replica data_r4;

-- { echo }
select count() from data_r1;
select replica, length(groupArray(partition)) from system.cluster_partitions array join active_replicas as replica where database = currentDatabase() and table = 'data_r1' group by 1 order by 1;

system drop cluster replica data_r3;
system drop cluster replica data_r4;
select table, is_readonly from system.replicas where database = currentDatabase() and table like 'data_%' order by 1;
system sync replica data_r1 cluster;
select replica, length(groupArray(partition)) from system.cluster_partitions array join active_replicas as replica where database = currentDatabase() and table = 'data_r1' group by 1 order by 1;
select count() from data_r4;

system restart replica data_r3;
system restart replica data_r4;
select table, is_readonly from system.replicas where database = currentDatabase() and table like 'data_%' order by 1;
system sync replica data_r1 cluster;
select replica, length(groupArray(partition)) from system.cluster_partitions array join active_replicas as replica where database = currentDatabase() and table = 'data_r1' group by 1 order by 1;
select count() from data_r4;
