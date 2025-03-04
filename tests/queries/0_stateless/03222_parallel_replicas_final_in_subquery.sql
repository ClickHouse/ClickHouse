DROP TABLE IF EXISTS test_00808;

CREATE TABLE test_00808
(
    `date` Date,
    `id` Int8,
    `name` String,
    `value` Int64,
    `sign` Int8
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY (id, date);

INSERT INTO test_00808 VALUES('2000-01-01', 1, 'test string 1', 1, 1);
INSERT INTO test_00808 VALUES('2000-01-01', 2, 'test string 2', 2, 1);

SET allow_experimental_parallel_reading_from_replicas = 2, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree=1, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer

SELECT * FROM (SELECT * FROM test_00808 FINAL) WHERE id = 1; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE test_00808;
