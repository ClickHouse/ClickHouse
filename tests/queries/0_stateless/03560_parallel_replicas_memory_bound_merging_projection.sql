drop table if exists pr_t;

create table pr_t(a UInt64, b UInt64) engine=MergeTree order by a;
ALTER TABLE pr_t ADD PROJECTION p_agg (SELECT a, sum(b) GROUP BY a);
insert into pr_t select number % 1000, number % 1000 from numbers_mt(1e6);

set parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer

set max_threads = 4;
set enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
set distributed_aggregation_memory_efficient = 1, enable_memory_bound_merging_of_aggregation_results = 1;
set enable_analyzer = 1, parallel_replicas_local_plan = 1, optimize_use_projections = 1, parallel_replicas_support_projection = 1;
set read_in_order_two_level_merge_threshold = 1000;

-- { echoOn } --
set optimize_aggregation_in_order = 0;
SELECT trimLeft(*) FROM (explain select sum(b) from pr_t group by a order by a limit 5 offset 500) WHERE explain LIKE '%ReadFromMergeTree%';
set optimize_aggregation_in_order = 1;
explain pipeline select sum(b) from pr_t group by a order by a limit 5 offset 500;
select sum(b) from pr_t group by a order by a limit 5 offset 500;
-- { echoOff } --

drop table if exists pr_t;
