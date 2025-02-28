drop table if exists distinct_in_order;

set optimize_distinct_in_order=1;
set enable_parallel_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree=1, parallel_replicas_local_plan=1;
set optimize_read_in_order=0;

create table distinct_in_order (a int, b int, c int) engine=MergeTree() order by (a, b);
insert into distinct_in_order select number % number, number % 5, number % 10 from numbers(1,1000000);

select distinct a, b, x, y from (select a, b, 1 as x, 2 as y from distinct_in_order order by a) order by a, b;

drop table if exists distinct_in_order;
