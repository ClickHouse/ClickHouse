
DROP TABLE IF EXISTS test.t_04071;
CREATE TABLE test.t_04071 (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO test.t_04071 VALUES (1), (2), (3);

-- Verify _table works as virtual column
SELECT x, _table FROM test.t_04071 ORDER BY x;

-- Attempting to UPDATE common virtual column _table should fail with NOT_IMPLEMENTED
ALTER TABLE test.t_04071 UPDATE _table = 'foo' WHERE 1; -- { serverError NOT_IMPLEMENTED }

-- DESCRIBE on a table function should include _table in virtual columns
DESCRIBE TABLE numbers(10) SETTINGS describe_include_virtual_columns=1;

-- DESCRIBE on a regular table should also include _table
DESCRIBE TABLE test.t_04071 SETTINGS describe_include_virtual_columns=1, describe_compact_output=1;

DROP TABLE IF EXISTS test.t_04071;
