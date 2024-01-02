-- Tags: no-parallel

create database if not exists shard_0;
create database if not exists shard_1;

drop table if exists dist_01850;
drop table if exists shard_0.data_01850;

create table shard_0.data_01850 (key Int) engine=Memory();
create table dist_01850 (key Int) engine=Distributed('test_cluster_two_replicas_different_databases', /* default_database= */ '', data_01850, key);

set insert_distributed_sync=1;
set prefer_localhost_replica=0;
insert into dist_01850 values (1); -- { serverError 60 }

drop table if exists dist_01850;
drop table shard_0.data_01850;

drop database shard_0;
drop database shard_1;
