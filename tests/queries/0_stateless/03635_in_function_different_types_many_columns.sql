-- Tags: no-parallel-replicas, no-random-merge-tree-settings
-- followup to 02882_primary_key_index_in_function_different_types

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value UInt64
) ENGINE=MergeTree ORDER BY (id, value) SETTINGS index_granularity = 8192, index_granularity_bytes = '1Mi';

INSERT INTO test_table SELECT number, number FROM numbers(10);

EXPLAIN indexes = 1, description=0 SELECT id FROM test_table WHERE (id, value) IN (SELECT '5', number FROM numbers(5));
EXPLAIN indexes = 1, description=0 SELECT id FROM test_table WHERE (id, value) IN (SELECT 'not a number', number FROM numbers(5));
EXPLAIN indexes = 1, description=0 SELECT id FROM test_table WHERE (id, value) IN (SELECT 42, 'not a number' UNION ALL SELECT 5, toString(number) FROM numbers(5));
EXPLAIN indexes = 1, description=0 SELECT id FROM test_table WHERE (id, value) IN (SELECT '42', 'not a number' UNION ALL SELECT 'not a number', '42' FROM numbers(5));

DROP TABLE test_table;
