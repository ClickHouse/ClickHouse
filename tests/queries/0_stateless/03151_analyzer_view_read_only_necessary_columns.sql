DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id;

DROP VIEW IF EXISTS test_view;
CREATE VIEW test_view AS SELECT id, value FROM test_table;

EXPLAIN header = 1 SELECT sum(id) FROM test_view settings enable_analyzer=1;

DROP VIEW test_view;
DROP TABLE test_table;
