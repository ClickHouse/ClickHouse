drop table if exists data;
create table data (key Int) engine=Memory();
-- NOTE: internal_replication is false, so INSERT will be done only into one shard
insert into function clusterAllReplicas(test_cluster_two_shards, currentDatabase(), data, rand()) values (2);
select * from data order by key;
drop table data;
