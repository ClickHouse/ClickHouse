-- just a smoke test

drop table if exists dist_01294;
create table dist_01294 as system.one engine=Distributed(test_shard_localhost, system, one);
-- flush
system flush distributed dist_01294;
system flush distributed on cluster test_shard_localhost dist_01294;
-- stop
system stop distributed sends dist_01294;
system stop distributed sends on cluster test_shard_localhost dist_01294;
-- start
system start distributed sends dist_01294;
system start distributed sends on cluster test_shard_localhost dist_01294;

drop table dist_01294;
