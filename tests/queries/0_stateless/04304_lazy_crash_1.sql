-- https://github.com/ClickHouse/ClickHouse/issues/106095

SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'parallel_replicas';
SET read_in_order_two_level_merge_threshold = 2;

DROP TABLE IF EXISTS test SYNC;
CREATE TABLE test (k String, v Array(UInt8)) ENGINE = MergeTree ORDER BY k;
INSERT INTO test VALUES ('a', [1, 2]);
INSERT INTO test VALUES ('a', [3]);
INSERT INTO test VALUES ('a', [4, 5]);

SELECT k, arrayJoin(v) FROM test ORDER BY ALL;
DROP TABLE IF EXISTS test SYNC;
