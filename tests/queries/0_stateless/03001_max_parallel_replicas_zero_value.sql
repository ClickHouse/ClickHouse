drop table if exists test_d;
create table test_d engine=Distributed(test_cluster_two_shard_three_replicas_localhost, system, numbers);
select * from test_d limit 10 settings max_parallel_replicas = 0, prefer_localhost_replica = 0; --{clientError BAD_ARGUMENTS}
drop table test_d;

