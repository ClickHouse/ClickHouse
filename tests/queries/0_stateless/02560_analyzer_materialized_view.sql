SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id;

DROP VIEW IF EXISTS test_materialized_view;
CREATE MATERIALIZED VIEW test_materialized_view
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id AS SELECT id, value FROM test_table;

INSERT INTO test_table VALUES (0, 'Value_0');
SELECT id, value FROM test_materialized_view ORDER BY id;

SELECT '--';

INSERT INTO test_table VALUES (1, 'Value_1');
SELECT id, value FROM test_materialized_view ORDER BY id;

DROP TABLE IF EXISTS test_table_data;
CREATE TABLE test_table_data
(
    id UInt64,
    value String
) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table_data VALUES (2, 'Value_2'), (3, 'Value_3');

SELECT '--';

INSERT INTO test_table SELECT id, value FROM test_table_data;
SELECT id, value FROM test_materialized_view ORDER BY id;

DROP TABLE test_table_data;
DROP VIEW test_materialized_view;
DROP TABLE test_table;
