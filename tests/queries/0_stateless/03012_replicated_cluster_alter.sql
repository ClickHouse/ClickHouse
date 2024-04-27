SET allow_experimental_analyzer=0; -- FIXME: analyzer is not supported yet

-- Tags: no-random-merge-tree-settings

drop table if exists data_r1;
drop table if exists data_r2;
drop table if exists data_r3;
drop table if exists data_r4;

create table data_r1 (part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '1') order by tuple() partition by part settings cluster=1, cluster_replication_factor=2;
create table data_r2 (part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '2') order by tuple() partition by part settings cluster=1, cluster_replication_factor=2;
create table data_r3 (part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '3') order by tuple() partition by part settings cluster=1, cluster_replication_factor=2;
create table data_r4 (part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '4') order by tuple() partition by part settings cluster=1, cluster_replication_factor=2;
select throwIf(is_readonly = 1) from system.replicas where database = currentDatabase() and table like 'data_%' format Null;

insert into data_r1 select number part, number value from numbers(10);

alter table data_r1 add column value_str String settings alter_sync=2;

show create data_r1;
show create data_r2;
show create data_r3;
show create data_r4;
