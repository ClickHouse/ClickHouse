-- Tags: no-replicated-database
-- For Replicated databases it's not allowed to execute ALTERs of different types (replicated and non replicated) in single query. (NOT_IMPLEMENTED)

-- Test for issue #70678: ALTER UPDATE with RENAME unexpected behavior
-- If the ALTER statement is atomic, both UPDATE and RENAME should either
-- succeed together or fail together.
-- The fix rejects UPDATE + RENAME on the same column early to ensure atomicity.

set alter_sync=2;
set mutations_sync=2;

DROP TABLE IF EXISTS test_alter_atomic;

-- Test 1: MergeTree engine - same behavior expected
CREATE TABLE test_alter_atomic (key Int32, c0 Int32) ENGINE = MergeTree ORDER BY key;
INSERT INTO test_alter_atomic VALUES (1, 0);

ALTER TABLE test_alter_atomic UPDATE c0 = 1 WHERE true, RENAME COLUMN c0 TO c1;

-- Verify column still has original name
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'test_alter_atomic' ORDER BY position;
INSERT INTO test_alter_atomic VALUES (2, 2);
SELECT * FROM test_alter_atomic ORDER BY key;

DROP TABLE test_alter_atomic;

-- Test 2: Verify UPDATE + RENAME on DIFFERENT columns works fine
CREATE TABLE test_alter_atomic (key Int32, c0 Int32, c1 Int32) ENGINE = MergeTree ORDER BY key;
INSERT INTO test_alter_atomic VALUES (1, 10, 20);

-- UPDATE c0, RENAME c1 should work because they are different columns
ALTER TABLE test_alter_atomic UPDATE c0 = 100 WHERE true, RENAME COLUMN c1 TO c2;

SELECT key, c0, c2 FROM test_alter_atomic;
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'test_alter_atomic' ORDER BY position;

DROP TABLE test_alter_atomic;

-- Test 5: Verify DELETE + RENAME on DIFFERENT columns works fine
CREATE TABLE test_alter_atomic (key Int32, c0 Int32, c1 Int32) ENGINE = MergeTree ORDER BY key;
INSERT INTO test_alter_atomic VALUES (1, 10, 20), (2, 30, 40);

-- DELETE based on c0, RENAME c1 should work because they are different columns
ALTER TABLE test_alter_atomic DELETE WHERE c0 = 10, RENAME COLUMN c1 TO c2;

SELECT * FROM test_alter_atomic;
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'test_alter_atomic' ORDER BY position;

DROP TABLE test_alter_atomic;
