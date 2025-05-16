-- Tags: no-ordinary-database, no-replicated-database, distributed, zookeeper

set database_atomic_wait_for_drop_and_detach_synchronously = 0;

select 'test MergeTree undrop';
drop table if exists 02681_undrop_mergetree sync;
create table 02681_undrop_mergetree (id Int32) Engine=MergeTree() order by id;
insert into 02681_undrop_mergetree values (1),(2),(3);
drop table 02681_undrop_mergetree;
select table from system.dropped_tables where table = '02681_undrop_mergetree' limit 1;
undrop table 02681_undrop_mergetree;
select * from 02681_undrop_mergetree order by id;
drop table 02681_undrop_mergetree sync;

select 'test detach';
drop table if exists 02681_undrop_detach sync;
create table 02681_undrop_detach (id Int32, num Int32) Engine=MergeTree() order by id;
insert into 02681_undrop_detach values (1, 1);
detach table 02681_undrop_detach sync;
undrop table 02681_undrop_detach; -- { serverError TABLE_ALREADY_EXISTS }
attach table 02681_undrop_detach;
alter table 02681_undrop_detach update num = 2 where id = 1;
select command from system.mutations where table='02681_undrop_detach' and database=currentDatabase() limit 1;
drop table 02681_undrop_detach sync;

select 'test MergeTree with cluster';
drop table if exists 02681_undrop_uuid_on_cluster on cluster test_shard_localhost sync format Null;
create table 02681_undrop_uuid_on_cluster on cluster test_shard_localhost (id Int32) Engine=MergeTree() order by id format Null;
insert into 02681_undrop_uuid_on_cluster values (1),(2),(3);
drop table 02681_undrop_uuid_on_cluster on cluster test_shard_localhost format Null;
select table from system.dropped_tables where table = '02681_undrop_uuid_on_cluster' limit 1;
undrop table 02681_undrop_uuid_on_cluster on cluster test_shard_localhost format Null;
select * from 02681_undrop_uuid_on_cluster order by id;
drop table 02681_undrop_uuid_on_cluster sync;

select 'test MergeTree without uuid on cluster';
drop table if exists 02681_undrop_no_uuid_on_cluster on cluster test_shard_localhost sync format Null;
create table 02681_undrop_no_uuid_on_cluster on cluster test_shard_localhost (id Int32) Engine=MergeTree() order by id format Null;
insert into 02681_undrop_no_uuid_on_cluster values (1),(2),(3);
drop table 02681_undrop_no_uuid_on_cluster on cluster test_shard_localhost format Null;
select table from system.dropped_tables where table = '02681_undrop_no_uuid_on_cluster' limit 1;
undrop table 02681_undrop_no_uuid_on_cluster on cluster test_shard_localhost format Null;
select * from 02681_undrop_no_uuid_on_cluster order by id;
drop table 02681_undrop_no_uuid_on_cluster on cluster test_shard_localhost sync format Null;

select 'test ReplicatedMergeTree undrop';
drop table if exists 02681_undrop_replicatedmergetree sync;
create table 02681_undrop_replicatedmergetree (id Int32) Engine=ReplicatedMergeTree('/clickhouse/tables/{database}/02681_undrop_replicatedmergetree', 'test_undrop') order by id;
insert into 02681_undrop_replicatedmergetree values (1),(2),(3);
drop table 02681_undrop_replicatedmergetree;
select table from system.dropped_tables where table = '02681_undrop_replicatedmergetree' limit 1;
undrop table 02681_undrop_replicatedmergetree;
select * from 02681_undrop_replicatedmergetree order by id;
drop table 02681_undrop_replicatedmergetree sync;

select 'test Log undrop';
drop table if exists 02681_undrop_log sync;
create table 02681_undrop_log (id Int32) Engine=Log();
insert into 02681_undrop_log values (1),(2),(3);
drop table 02681_undrop_log;
select table from system.dropped_tables where table = '02681_undrop_log' limit 1;
undrop table 02681_undrop_log;
select * from 02681_undrop_log order by id;
drop table 02681_undrop_log sync;

select 'test Distributed undrop';
drop table if exists 02681_undrop_distributed sync;
create table 02681_undrop_distributed (id Int32) Engine = Distributed(test_shard_localhost, currentDatabase(), 02681_undrop, rand());
drop table 02681_undrop_distributed;
select table from system.dropped_tables where table = '02681_undrop_distributed' limit 1;
undrop table 02681_undrop_distributed;
drop table 02681_undrop_distributed sync;

select 'test MergeTree drop and undrop multiple times';
drop table if exists 02681_undrop_multiple sync;
create table 02681_undrop_multiple (id Int32) Engine=MergeTree() order by id;
insert into 02681_undrop_multiple values (1);
drop table 02681_undrop_multiple;
create table 02681_undrop_multiple (id Int32) Engine=MergeTree() order by id;
insert into 02681_undrop_multiple values (2);
drop table 02681_undrop_multiple;
create table 02681_undrop_multiple (id Int32) Engine=MergeTree() order by id;
insert into 02681_undrop_multiple values (3);
drop table 02681_undrop_multiple;
select table from system.dropped_tables where table = '02681_undrop_multiple' limit 1;
undrop table 02681_undrop_multiple;
select * from 02681_undrop_multiple order by id;
undrop table 02681_undrop_multiple; -- { serverError TABLE_ALREADY_EXISTS }
drop table 02681_undrop_multiple sync;
