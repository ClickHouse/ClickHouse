SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET allow_experimental_parallel_reading_from_replicas = 1;
SET cluster_for_parallel_replicas = 'parallel_replicas';
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (a UInt64, b UInt64, c UInt64, d UInt64, x Array(String))
ENGINE MergeTree() PARTITION BY b ORDER BY a;
INSERT INTO test_table SELECT number, number % 2, number, number % 3, ['a', 'b', 'c'] FROM numbers(1);
ALTER TABLE test_table ADD COLUMN y Array(String) ALIAS ['qwqw'] AFTER x;

SELECT y FROM test_table ORDER BY c;

SET allow_experimental_parallel_reading_from_replicas = 1;
SELECT '----';
SELECT y FROM remote('127.0.0.{1,2}', default, test_table) ORDER BY c settings extremes=1;
