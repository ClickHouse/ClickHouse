DROP TABLE IF EXISTS t1 SYNC;
DROP TABLE IF EXISTS t2 SYNC;
DROP TABLE IF EXISTS t3 SYNC;

CREATE TABLE t1(k UInt32, v UInt32) ENGINE ReplicatedMergeTree('/parallel_replicas/{database}/test_tbl', 'r1') ORDER BY k settings index_granularity=10;
CREATE TABLE t2(k UInt32, v UInt32) ENGINE ReplicatedMergeTree('/parallel_replicas/{database}/test_tbl', 'r2') ORDER BY k settings index_granularity=10;
CREATE TABLE t3(k UInt32, v UInt32) ENGINE ReplicatedMergeTree('/parallel_replicas/{database}/test_tbl', 'r3') ORDER BY k settings index_granularity=10;

insert into t1 select number, number from numbers(1000);
insert into t1 select number, number from numbers(1000, 1000);
insert into t1 select number, number from numbers(2000, 1000);

insert into t2 select number, number from numbers(3000, 1000);
insert into t2 select number, number from numbers(4000, 1000);
insert into t2 select number, number from numbers(5000, 1000);

insert into t3 select number, number from numbers(6000, 1000);
insert into t3 select number, number from numbers(7000, 1000);
insert into t3 select number, number from numbers(8000, 1000);

system sync replica t1;
system sync replica t2;
system sync replica t3;

SELECT count(), min(k), max(k), avg(k)
FROM t1
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, prefer_localhost_replica = 0,
         cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost', parallel_replicas_single_task_marks_count_multiplier = 0.001;
