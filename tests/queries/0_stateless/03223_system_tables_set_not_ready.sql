-- Tags: no-fasttest
-- Tag no-fasttest -- due to EmbeddedRocksDB

drop table if exists null;
drop table if exists dist;
create table null as system.one engine=Null;
create table dist as null engine=Distributed(test_cluster_two_shards, currentDatabase(), 'null', rand());
insert into dist settings prefer_localhost_replica=0 values (1);
select 'system.distribution_queue', count() from system.distribution_queue where exists(select 1) and database = currentDatabase();

drop table if exists rocksdb;
create table rocksdb (key Int) engine=EmbeddedRocksDB() primary key key;
insert into rocksdb values (1);
select 'system.rocksdb', count()>0 from system.rocksdb where exists(select 1) and database = currentDatabase();

select 'system.databases', count() from system.databases where exists(select 1) and database = currentDatabase();

drop table if exists mt;
create table mt (key Int) engine=MergeTree() order by key;
alter table mt delete where 1;
select 'system.mutations', count() from system.mutations where exists(select 1) and database = currentDatabase();

drop table if exists rep1;
drop table if exists rep2;
create table rep1 (key Int) engine=ReplicatedMergeTree('/{database}/rep', '{table}') order by key;
create table rep2 (key Int) engine=ReplicatedMergeTree('/{database}/rep', '{table}') order by key;
system stop fetches rep2;
insert into rep1 values (1);
system sync replica rep2 pull;
select 'system.replication_queue', count()>0 from system.replication_queue where exists(select 1) and database = currentDatabase();
