-- just a smoke test

-- quirk for ON CLUSTER does not uses currentDatabase()
drop database if exists db_01294;
create database db_01294;

drop table if exists db_01294.dist_01294;
create table db_01294.dist_01294 as system.one engine=Distributed(test_shard_localhost, system, one);
-- flush
system flush distributed db_01294.dist_01294;
system flush distributed on cluster test_shard_localhost db_01294.dist_01294;
-- stop
system stop distributed sends;
system stop distributed sends db_01294.dist_01294;
system stop distributed sends on cluster test_shard_localhost db_01294.dist_01294;
-- start
system start distributed sends;
system start distributed sends db_01294.dist_01294;
system start distributed sends on cluster test_shard_localhost db_01294.dist_01294;

drop database db_01294;
