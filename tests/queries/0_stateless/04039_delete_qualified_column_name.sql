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

-- Case 1: DELETE with table.column qualification
DELETE FROM test_delete_qualified WHERE test_delete_qualified.id = 2;

SELECT * FROM test_delete_qualified ORDER BY id;

DROP TABLE test_delete_qualified;

-- Case 2: DELETE with database.table.column qualification
DROP DATABASE IF EXISTS test_delete_db_04039;
CREATE DATABASE test_delete_db_04039;

CREATE TABLE test_delete_db_04039.t (id Int32, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_delete_db_04039.t VALUES (1, 'a'), (2, 'b'), (3, 'c');

DELETE FROM test_delete_db_04039.t WHERE test_delete_db_04039.t.id = 1;

SELECT * FROM test_delete_db_04039.t ORDER BY id;

-- Case 3: Non-matching qualifier must NOT be stripped (should error)
DELETE FROM test_delete_db_04039.t WHERE no_such_table.id = 3; -- { serverError UNKNOWN_IDENTIFIER }

DROP DATABASE test_delete_db_04039;
