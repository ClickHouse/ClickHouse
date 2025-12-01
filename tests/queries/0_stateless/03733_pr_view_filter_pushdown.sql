drop table if exists t_03733;
drop table if exists v_03733;

create table t_03733(a UInt32, b String) engine=MergeTree order by ();
create view v_03733 as select * from t_03733;

SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree=1;

insert into t_03733 select number, toString(number) from numbers(10);

SELECT * FROM v_03733 WHERE a = 0;

select substring(trimLeft(explain), 1, 6) from (explain select * from v_03733 where a = 0) where explain ilike '%Filter%';
