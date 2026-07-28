
set max_threads = 16;
set use_hedged_requests = 0;
set max_parallel_replicas = 3;
set cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
set enable_parallel_replicas = 1;
set parallel_replicas_for_non_replicated_merge_tree = 1;
set allow_aggregate_partitions_independently = 1;

drop table if exists t2;

create table t2(a Int16) engine=MergeTree order by tuple() partition by a % 8 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

system stop merges t2;

insert into t2 select number from numbers_mt(1e6);
insert into t2 select number from numbers_mt(1e6);

select a from t2 group by a format Null;
