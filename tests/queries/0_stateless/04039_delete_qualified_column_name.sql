-- Tags: no-fasttest, no-replicated-database
-- Verify that DELETE FROM with qualified column names works.
-- See https://github.com/ClickHouse/ClickHouse/issues/71760

DROP TABLE IF EXISTS test_delete_qualified;

CREATE TABLE test_delete_qualified
(
    id Int32,
    value String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test_delete_qualified VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- DELETE with qualified column name (database.table.column)
DELETE FROM test_delete_qualified WHERE default.test_delete_qualified.id = 1;

SELECT * FROM test_delete_qualified ORDER BY id;

-- DELETE with table.column qualification
DELETE FROM test_delete_qualified WHERE test_delete_qualified.id = 2;

SELECT * FROM test_delete_qualified ORDER BY id;

DROP TABLE test_delete_qualified;
