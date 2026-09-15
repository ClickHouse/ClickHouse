create table src (y Int8) engine MergeTree order by y as select 1;
select 1 from (select 2 from src) settings enable_parallel_replicas=1, max_parallel_replicas=2, cluster_for_parallel_replicas='test_shard_localhost', parallel_replicas_for_non_replicated_merge_tree=1;
