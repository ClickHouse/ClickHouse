DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value UInt64
) ENGINE=MergeTree ORDER BY (id, value) SETTINGS index_granularity = 8192, index_granularity_bytes = '1Mi';

INSERT INTO test_table SELECT number, number FROM numbers(10);

set enable_analyzer = 0;

EXPLAIN indexes = 1, description=0 SELECT id FROM test_table WHERE id <= 10 AND value IN (SELECT 5);
EXPLAIN indexes = 1, description=0 SELECT id FROM test_table WHERE id <= 10 AND value IN (SELECT '5');
EXPLAIN indexes = 1, description=0 SELECT id FROM test_table WHERE id <= 10 AND value IN (SELECT toUInt8(number) FROM numbers(5));
EXPLAIN indexes = 1, description=0 SELECT id FROM test_table WHERE id <= 10 AND value IN (SELECT toString(number) FROM numbers(5));

set enable_analyzer = 1;

EXPLAIN indexes = 1, description=0 SELECT id FROM test_table WHERE id <= 10 AND value IN (SELECT 5);
EXPLAIN indexes = 1, description=0 SELECT id FROM test_table WHERE id <= 10 AND value IN (SELECT '5');
EXPLAIN indexes = 1, description=0 SELECT id FROM test_table WHERE id <= 10 AND value IN (SELECT toUInt8(number) FROM numbers(5));
EXPLAIN indexes = 1, description=0 SELECT id FROM test_table WHERE id <= 10 AND value IN (SELECT toString(number) FROM numbers(5));

DROP TABLE test_table;
