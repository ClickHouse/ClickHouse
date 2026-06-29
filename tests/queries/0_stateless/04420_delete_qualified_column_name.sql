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

-- Case 2: DELETE with database.table.column qualification.
-- Use the test's own (randomly named) database so the test is safe to run
-- concurrently with itself, as the flaky check does.
CREATE TABLE t_04420 (id Int32, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_04420 VALUES (1, 'a'), (2, 'b'), (3, 'c');

DELETE FROM {CLICKHOUSE_DATABASE:Identifier}.t_04420 WHERE {CLICKHOUSE_DATABASE:Identifier}.t_04420.id = 1;

SELECT * FROM t_04420 ORDER BY id;

-- Case 3: Non-matching qualifier must NOT be stripped (should error)
DELETE FROM t_04420 WHERE no_such_table.id = 3; -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE t_04420;
