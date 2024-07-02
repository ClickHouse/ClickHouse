SET allow_experimental_analyzer=0; -- FIXME: analyzer is not supported yet

drop table if exists data_r1;
drop table if exists data_r2;
drop table if exists data_r3;
drop table if exists data_r4;

create table data_r1 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '1') order by key partition by (part, key % 50) settings cluster=1, cluster_replication_factor=2;
create table data_r2 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '2') order by key partition by (part, key % 50) settings cluster=1, cluster_replication_factor=2;
create table data_r3 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '3') order by key partition by (part, key % 50) settings cluster=1, cluster_replication_factor=2;
create table data_r4 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '4') order by key partition by (part, key % 50) settings cluster=1, cluster_replication_factor=2;

insert into data_r1 select number key, intDiv(number, 10) part, number value from numbers(10);

-- NOTE: this is required for proper results after OPTIMIZE, is this OK?
system sync replica data_r1 cluster;
system sync replica data_r2 cluster;
system sync replica data_r3 cluster;
system sync replica data_r4 cluster;

-- { echoOn }
optimize table data_r1 final;
select count() from data_r1;
select count() from data_r2;
select count() from data_r3;
select count() from data_r4;

optimize table data_r2 final;
select count() from data_r1;
select count() from data_r2;
select count() from data_r3;
select count() from data_r4;
