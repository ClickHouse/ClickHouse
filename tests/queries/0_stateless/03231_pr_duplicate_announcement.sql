DROP TABLE IF EXISTS test_table SYNC;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=ReplicatedMergeTree('/clickhouse/test/{database}/test_table', 'r1') ORDER BY tuple();

INSERT INTO test_table VALUES (0, 'Value_0'), (1, 'Value_1'), (2, 'Value_2');

DROP TABLE IF EXISTS test_table_for_in SYNC;
CREATE TABLE test_table_for_in
(
    id UInt64
) ENGINE=ReplicatedMergeTree('/clickhouse/test/{database}/test_table_for_in', 'r1') ORDER BY tuple();

INSERT INTO test_table_for_in VALUES (0), (1);

SET allow_experimental_parallel_reading_from_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SELECT id, value FROM test_table WHERE id IN (SELECT id FROM test_table_for_in UNION DISTINCT SELECT id FROM test_table_for_in);

DROP TABLE test_table SYNC;
DROP TABLE test_table_for_in SYNC;
