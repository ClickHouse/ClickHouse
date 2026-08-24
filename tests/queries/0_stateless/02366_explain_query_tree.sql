SET enable_analyzer = 1;

EXPLAIN QUERY TREE run_passes = 0 SELECT 1;

SELECT '--';

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=TinyLog;

INSERT INTO test_table VALUES (0, 'Value');

EXPLAIN QUERY TREE run_passes = 0 SELECT id, value FROM test_table;

SELECT '--';

EXPLAIN QUERY TREE run_passes = 1 SELECT id, value FROM test_table;

SELECT '--';

EXPLAIN QUERY TREE run_passes = 0 SELECT arrayMap(x -> x + id, [1, 2, 3]) FROM test_table;

SELECT '--';

EXPLAIN QUERY TREE run_passes = 1 SELECT arrayMap(x -> x + 1, [1, 2, 3]) FROM test_table;

SELECT '--';

EXPLAIN QUERY TREE run_passes = 0 WITH x -> x + 1 AS lambda SELECT lambda(id) FROM test_table;

SELECT '--';

EXPLAIN QUERY TREE run_passes = 1 WITH x -> x + 1 AS lambda SELECT lambda(id) FROM test_table;

DROP TABLE test_table;
