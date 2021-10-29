drop table if exists dist;
create table dist as system.one engine=Distributed('test_shard_localhost', system, one);
-- { echo }
select dummy as foo from remote('127.{2,3}', currentDatabase(), dist) limit 1 settings prefer_localhost_replica=0, distributed_push_down_limit=0;
select dummy as foo from remote('127.{2,3}', currentDatabase(), dist) limit 1 settings prefer_localhost_replica=0, distributed_push_down_limit=1;
select dummy as foo from remote('127.{2,3}', currentDatabase(), dist) limit 1 settings prefer_localhost_replica=0, distributed_group_by_no_merge=1;
