SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (id UInt64, value Tuple(a UInt64)) ENGINE=MergeTree ORDER BY id;

INSERT INTO test_table VALUES (0, tuple(0));

-- { echoOn }

SELECT value FROM (SELECT tupleElement(value, 'a') AS value FROM test_table);

EXPLAIN QUERY TREE SELECT value FROM (
    SELECT tupleElement(value, 'a') AS value FROM test_table
);

SELECT '--';

EXPLAIN QUERY TREE SELECT value FROM (
    SELECT tupleElement(value, 'a') AS value FROM test_table
) SETTINGS optimize_functions_to_subcolumns = 1;

SELECT '--';

EXPLAIN QUERY TREE SELECT value FROM (
    SELECT tupleElement(value, 'a') AS value FROM test_table SETTINGS optimize_functions_to_subcolumns = 0
) SETTINGS optimize_functions_to_subcolumns = 1;

SELECT '--';

EXPLAIN QUERY TREE SELECT value FROM (
    SELECT tupleElement(value, 'a') AS value FROM test_table
) SETTINGS optimize_functions_to_subcolumns = 0;

SELECT '--';

EXPLAIN QUERY TREE SELECT value FROM (
    SELECT tupleElement(value, 'a') AS value FROM test_table SETTINGS optimize_functions_to_subcolumns = 1
) SETTINGS optimize_functions_to_subcolumns = 0;

-- { echoOff }

DROP TABLE test_table;
