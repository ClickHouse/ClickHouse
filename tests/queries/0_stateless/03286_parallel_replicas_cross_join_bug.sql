drop table if exists tab;
create table tab (x UInt64) engine = MergeTree order by tuple();
insert into tab select number from numbers(1e7);

set enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_for_non_replicated_merge_tree = true, parallel_replicas_local_plan = false;

select * from tab l, tab r where l.x < r.x and r.x < 2;

drop table if exists tab;
