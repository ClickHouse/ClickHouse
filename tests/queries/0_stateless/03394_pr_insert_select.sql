-- Tags: distributed

-- <Warning> ConnectionPoolWithFailover: Connection failed at try №1 - is not a problem
SET send_logs_level = 'fatal';


SET parallel_distributed_insert_select=2;

-- MT

DROP TABLE IF EXISTS t_mt_source;
DROP TABLE IF EXISTS t_mt_target;

CREATE TABLE t_mt_source (k UInt64, v String) ENGINE = MergeTree() ORDER BY k;
CREATE TABLE t_mt_target (k UInt64, v String) ENGINE = MergeTree() ORDER BY ();

INSERT INTO t_mt_source SELECT number as k, toString(number) as v FROM system.numbers LIMIT 1e6;
INSERT INTO t_mt_target SELECT * FROM t_mt_source SETTINGS enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3;

select * from t_mt_source order by k
except
select * from t_mt_target order by k;

DROP TABLE t_mt_source;
DROP TABLE t_mt_target;

-- RMT

DROP TABLE IF EXISTS t_rmt_source SYNC;
DROP TABLE IF EXISTS t_rmt_target SYNC;

CREATE TABLE t_rmt_source (k UInt64, v String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rmt_source', 'r1') ORDER BY k;
CREATE TABLE t_rmt_target (k UInt64, v String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rmt_target', 'r1') ORDER BY ();

INSERT INTO t_rmt_source SELECT number as k, toString(number) as v FROM system.numbers LIMIT 1e6 settings parallel_distributed_insert_select=0;
INSERT INTO t_rmt_target SELECT * FROM t_rmt_source SETTINGS enable_parallel_replicas = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3;

select * from t_rmt_source order by k
except
select * from t_rmt_target order by k;

DROP TABLE t_rmt_source SYNC;
DROP TABLE t_rmt_target SYNC;

--- test_cluster_1_shard_3_replicas_1_unavailable

