-- Test: WITH function-expression alias used in IN inside CREATE VIEW
-- https://github.com/ClickHouse/ClickHouse/issues/99308

DROP TABLE IF EXISTS test_table;
DROP VIEW IF EXISTS test_view;
DROP VIEW IF EXISTS test_view_nested;

CREATE TABLE test_table (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO test_table VALUES (1), (2), (3);

-- Case 1: ASTFunction alias in WITH + IN inside CREATE VIEW
CREATE VIEW test_view AS
    WITH tuple(1, 2, 3) AS ev
    SELECT * FROM test_table WHERE x IN ev;

SELECT * FROM test_view ORDER BY x;

-- Case 2: Nested CTE referencing function alias
CREATE VIEW test_view_nested AS
    WITH tuple(1, 2) AS ev
    SELECT * FROM (SELECT * FROM test_table WHERE x IN ev) ORDER BY x;

SELECT * FROM test_view_nested ORDER BY x;

DROP VIEW test_view_nested;
DROP VIEW test_view;
DROP TABLE test_table;
