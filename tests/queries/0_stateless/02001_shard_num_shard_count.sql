-- Tags: shard

select shardNum() n, shardCount() c;
select shardNum() n, shardCount() c from remote('127.0.0.{1,2,3}', system.one) order by n settings prefer_localhost_replica = 0;
select shardNum() n, shardCount() c from remote('127.0.0.{1,2,3}', system.one) order by n settings prefer_localhost_replica = 1;
