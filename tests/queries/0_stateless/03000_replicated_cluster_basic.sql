SET allow_experimental_analyzer=0; -- FIXME: analyzer is not supported yet

drop table if exists data_r1;
drop table if exists data_r2;
drop table if exists data_r3;
drop table if exists data_r4;

-- TODO(cluster): support CLUSTER BY expr INTO x BUCKETS
create table data_r1 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '1') order by key partition by (part, key % 50) settings cluster=1, cluster_replication_factor=2;
create table data_r2 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '2') order by key partition by (part, key % 50) settings cluster=1, cluster_replication_factor=2;
create table data_r3 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '3') order by key partition by (part, key % 50) settings cluster=1, cluster_replication_factor=2;
create table data_r4 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '4') order by key partition by (part, key % 50) settings cluster=1, cluster_replication_factor=2;
select throwIf(is_readonly = 1) from system.replicas where database = currentDatabase() and table like 'data_%' format Null;

insert into data_r1 select number key, intDiv(number, 10) part, number value from numbers(10);

-- Only after sync replica we could check any data, since SELECT will do distributed query.
system sync replica data_r1 cluster;
system sync replica data_r2 cluster;
system sync replica data_r3 cluster;
system sync replica data_r4 cluster;

-- { echo }
select count() from data_r1;
select count() from data_r2;
select count() from data_r3;
select count() from data_r4;

select * from data_r1 order by key limit 1;
select * from data_r2 order by key limit 1;
select * from data_r3 order by key limit 1;
select * from data_r4 order by key limit 1;

-- disable count() optimization (if any)
select count(ignore(*)) from data_r1;
select count(ignore(*)) from data_r2;
select count(ignore(*)) from data_r3;
select count(ignore(*)) from data_r4;

-- just a smoke test of the system.parts.replicas, only smoke because now it is non-predictable -- pseudo random.
-- TODO: check other fields, right now they do not match (due to the race between INSERT and re-sharding)
select table, count() from system.cluster_partitions where database = currentDatabase() and table like 'data_%' group by 1 order by 1;

-- check that we are able to read all entries from ZooKeeper back
detach table data_r1;
detach table data_r2;
detach table data_r3;
detach table data_r4;
attach table data_r1;
attach table data_r2;
attach table data_r3;
attach table data_r4;
select throwIf(is_readonly = 1) from system.replicas where database = currentDatabase() and table like 'data_%' format Null;

select count() from data_r1;
select count() from data_r2;
select count() from data_r3;
select count() from data_r4;

-- one more time after DETACH
select table, count() from system.cluster_partitions where database = currentDatabase() and table like 'data_%' group by 1 order by 1;

-- one more time SELECT after DETACH
select count(ignore(*)) from data_r1;
select count(ignore(*)) from data_r2;
select count(ignore(*)) from data_r3;
select count(ignore(*)) from data_r4;
