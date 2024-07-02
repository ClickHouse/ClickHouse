SET allow_experimental_analyzer=0; -- FIXME: analyzer is not supported yet

drop table if exists data_r1;
drop table if exists data_r2;
drop table if exists data_r3;
drop table if exists data_r4;

-- Change merge_selecting_sleep_ms/max_merge_selecting_sleep_ms to execute merges instantly after "Log entry is not created for mutation partX because log was updated" error.
create table data_r1 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '1') order by key partition by (part, key % 50) settings cluster=1, cluster_replication_factor=2, merge_selecting_sleep_ms=100, max_merge_selecting_sleep_ms=100;
create table data_r2 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '2') order by key partition by (part, key % 50) settings cluster=1, cluster_replication_factor=2, merge_selecting_sleep_ms=100, max_merge_selecting_sleep_ms=100;
create table data_r3 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '3') order by key partition by (part, key % 50) settings cluster=1, cluster_replication_factor=2, merge_selecting_sleep_ms=100, max_merge_selecting_sleep_ms=100;
create table data_r4 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '4') order by key partition by (part, key % 50) settings cluster=1, cluster_replication_factor=2, merge_selecting_sleep_ms=100, max_merge_selecting_sleep_ms=100;

insert into data_r1 select number key, intDiv(number, 10) part, number value from numbers(10);

-- { echoOn }
alter table data_r1 update value = value*10 where 1 settings mutations_sync=2;

-- Only after sync replica we could check any data, since SELECT will do distributed query.
system sync replica data_r1 cluster;
system sync replica data_r2 cluster;
system sync replica data_r3 cluster;
system sync replica data_r4 cluster;

select arraySort(groupArray(value)) from data_r1;
select arraySort(groupArray(value)) from data_r2;
select arraySort(groupArray(value)) from data_r3;
select arraySort(groupArray(value)) from data_r4;
