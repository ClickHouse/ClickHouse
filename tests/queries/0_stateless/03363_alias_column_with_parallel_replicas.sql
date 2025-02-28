DROP TABLE IF EXISTS alias_column_with_parallel_replicas_table;

CREATE TABLE alias_column_with_parallel_replicas_table (c1 UInt64, c2 UInt64, c3 String ALIAS 'qwqw')
ENGINE = MergeTree
ORDER BY c1;

INSERT INTO alias_column_with_parallel_replicas_table SELECT number, number FROM numbers(1000);

SET allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SELECT c1, c2, c3 FROM alias_column_with_parallel_replicas_table ORDER BY c2 limit 6;

-- DROP TABLE alias_column_with_parallel_replicas_table;