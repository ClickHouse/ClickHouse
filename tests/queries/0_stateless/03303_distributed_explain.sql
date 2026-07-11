-- Tags: no-random-settings

set enable_analyzer=1;

set serialize_query_plan=0;
explain  actions = 1, distributed=1 select sum(number) from remote('127.0.0.{1,2,3}', numbers(5)) group by bitAnd(number, 3);
explain distributed=1 select * from (select * from remote('127.0.0.{1,2}', numbers(2)) where number=1);

select '----------';

set serialize_query_plan=1;
explain  actions = 1, distributed=1 select sum(number) from remote('127.0.0.{1,2,3}', numbers(5)) group by bitAnd(number, 3);
explain distributed=1 select * from (select * from remote('127.0.0.{1,2}', numbers(2)) where number=1);

select '----------';

DROP TABLE IF EXISTS test_parallel_replicas;
CREATE TABLE test_parallel_replicas (number UInt64) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO test_parallel_replicas SELECT * FROM numbers(10);

SET enable_parallel_replicas=2, max_parallel_replicas=2, cluster_for_parallel_replicas='test_cluster_one_shard_two_replicas', parallel_replicas_for_non_replicated_merge_tree=1, parallel_replicas_local_plan=1;

explain  actions = 1, distributed=1 SELECT sum(number) from test_parallel_replicas group by bitAnd(number, 3);
