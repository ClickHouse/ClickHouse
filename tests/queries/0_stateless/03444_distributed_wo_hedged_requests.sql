drop table if exists 03444_local;
drop table if exists 03444_distr;

create table 03444_local (id UInt32) engine = MergeTree order by id as select * from numbers(100);
create table 03444_distr (id UInt32) engine = Distributed(test_cluster_one_shard_two_replicas, currentDatabase(), 03444_local);

select 'remote() 1 shard: ' || count()
from remote('127.0.0.1|127.0.0.2|127.0.0.3', currentDatabase(), 03444_local)
settings prefer_localhost_replica = 0,
         use_hedged_requests = 0,
         max_parallel_replicas = 100;

select 'remote() 3 shards: ' || count()
from remote('127.0.0.1|127.0.0.2,127.0.0.3|127.0.0.4,127.0.0.5|127.0.0.6', currentDatabase(), 03444_local)
settings prefer_localhost_replica = 0,
         use_hedged_requests = 0,
         max_parallel_replicas = 100;

select 'Distributed: ' || count()
from 03444_distr
settings prefer_localhost_replica = 0,
         use_hedged_requests = 0,
         max_parallel_replicas = 100;

drop table 03444_local;
drop table 03444_distr;
