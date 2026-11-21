-- Tags: no-random-settings

-- Serialized query plan is not supported
set serialize_query_plan=0;

set enable_analyzer=1;
set max_threads=2;

-- Basic distributed pipeline explain
explain pipeline distributed=1 select sum(number) from remote('127.0.0.{1,2,3}', numbers(5)) group by bitAnd(number, 3);

select '----------';

-- With header option
explain pipeline header=1, distributed=1 select sum(number) from remote('127.0.0.{1,2}', numbers(5));

select '----------';

-- Verify that distributed + graph is not supported
explain pipeline distributed=1, graph=1 select 1 from remote('127.0.0.{1,2}', numbers(1)); -- { serverError NOT_IMPLEMENTED }

-- Verify that distributed pipeline is not supported with serialized query plan
explain pipeline distributed=1 select sum(number) from remote('127.0.0.{1,2}', numbers(5)) settings serialize_query_plan=1; -- { serverError NOT_IMPLEMENTED }

select '----------';

-- Distributed pipeline with parallel replicas: table with multiple parts, ORDER BY c
DROP TABLE IF EXISTS test_distributed_pipeline;
CREATE TABLE test_distributed_pipeline (a UInt64, b UInt64, c UInt64) ENGINE=MergeTree() ORDER BY c;
INSERT INTO test_distributed_pipeline SELECT number, number * 2, number * 3 FROM numbers(100);
INSERT INTO test_distributed_pipeline SELECT number, number * 2, number * 3 FROM numbers(100, 100);
INSERT INTO test_distributed_pipeline SELECT number, number * 2, number * 3 FROM numbers(200, 100);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas=2, max_parallel_replicas=2, cluster_for_parallel_replicas='test_cluster_one_shard_two_replicas', parallel_replicas_for_non_replicated_merge_tree=1, parallel_replicas_local_plan=1;

explain pipeline distributed=1 SELECT * FROM test_distributed_pipeline ORDER BY c;

DROP TABLE test_distributed_pipeline;
