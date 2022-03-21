-- Tags: deadlock, distributed

DROP TABLE IF EXISTS t_local;
DROP TABLE IF EXISTS t_dist;

create table t_local(a int) engine Log;
create table t_dist (a int) engine Distributed(test_shard_localhost, currentDatabase(), 't_local', cityHash64(a));

set insert_distributed_sync = 1;

insert into t_dist values (1);

DROP TABLE t_local;
DROP TABLE t_dist;
