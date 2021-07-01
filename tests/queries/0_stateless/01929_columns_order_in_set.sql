DROP TABLE IF EXISTS test_set;
DROP TABLE IF EXISTS test_table;

CREATE TABLE test_set(id UInt64, name String) ENGINE = Set;
INSERT INTO test_set VALUES (1, 'Mary'), (2, 'Jane'), (3, 'Mary'), (4, 'Jack');

CREATE TABLE test_table (id UInt64, name String) ENGINE = MergeTree() ORDER BY (id) SETTINGS index_granularity = 8192;
INSERT INTO test_table VALUES (1, 'Jack'), (2, 'Mary'), (3, 'Mary'), (4, 'John'), (5, 'Mary');

SELECT * FROM test_table WHERE (id, name) IN (test_set);

DROP TABLE test_set;
DROP TABLE test_table;
