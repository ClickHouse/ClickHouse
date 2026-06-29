-- https://github.com/ClickHouse/ClickHouse/issues/11000

DROP TABLE IF EXISTS test_table_01;
DROP TABLE IF EXISTS test_table_02;
DROP TABLE IF EXISTS test_view_01;

SET enable_analyzer = 1;

CREATE TABLE test_table_01 (
    column Int32
) ENGINE = Memory();

CREATE TABLE test_table_02 (
    column Int32
) ENGINE = Memory();

CREATE VIEW test_view_01 AS
SELECT
    t1.column,
    t2.column
FROM test_table_01 AS t1
    INNER JOIN test_table_02 AS t2 ON t1.column = t2.column;

DROP TABLE IF EXISTS test_table_01;
DROP TABLE IF EXISTS test_table_02;
DROP TABLE IF EXISTS test_view_01;
