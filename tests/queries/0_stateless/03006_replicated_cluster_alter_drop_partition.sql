SET allow_experimental_analyzer=0; -- FIXME: analyzer is not supported yet

-- FIXME: simple count() cannot be used because in this case we rely on the
-- implicit smallest column, which can be different on various tables (i.e.
-- data_r1 can have 0 rows and will have "key" as a smallet column, data_r2
-- will have rows and will return "part", since it is better compressed
-- then the key).

drop table if exists data_r1;
drop table if exists data_r2;
drop table if exists data_r3;
drop table if exists data_r4;

create table data_r1 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '1') order by key partition by (part, intDiv(key, 50)) settings cluster=1, cluster_replication_factor=2;
create table data_r2 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '2') order by key partition by (part, intDiv(key, 50)) settings cluster=1, cluster_replication_factor=2;
create table data_r3 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '3') order by key partition by (part, intDiv(key, 50)) settings cluster=1, cluster_replication_factor=2;
create table data_r4 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '4') order by key partition by (part, intDiv(key, 50)) settings cluster=1, cluster_replication_factor=2;

insert into data_r1 select number key, number%4 part, number value from numbers(20);

-- { echoOn }
alter table data_r1 drop partition (0, 0) settings mutations_sync=2;

-- Only after sync replica we could check any data, since SELECT will do distributed query.
system sync replica data_r1 cluster;
system sync replica data_r2 cluster;
system sync replica data_r3 cluster;
system sync replica data_r4 cluster;
-- FIXME: why this is required?

select count(key) from data_r1;
select count(key) from data_r2;
select count(key) from data_r3;
select count(key) from data_r4;
