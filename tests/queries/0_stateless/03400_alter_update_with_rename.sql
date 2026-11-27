-- Tags: no-replicated-database
-- For Replicated databases it's not allowed to execute ALTERs of different types (replicated and non replicated) in single query. (NOT_IMPLEMENTED)

-- Test for issue #70678: ALTER UPDATE with RENAME unexpected behavior
-- If the ALTER statement is atomic, both UPDATE and RENAME should either
-- succeed together or fail together.
-- The fix rejects UPDATE + RENAME on the same column early to ensure atomicity.

DROP TABLE IF EXISTS test_alter_atomic;

-- Test 1: Memory engine - UPDATE + RENAME on same column should be rejected
CREATE TABLE test_alter_atomic (c0 Int32) ENGINE = Memory;
INSERT INTO test_alter_atomic VALUES (0);

-- This should fail early with a clear error, without making any changes
ALTER TABLE test_alter_atomic UPDATE c0 = 1 WHERE true, RENAME COLUMN c0 TO c1; -- { serverError BAD_ARGUMENTS }

-- Verify the column still has its original name (c0) since the ALTER was rejected
SELECT * FROM test_alter_atomic;
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'test_alter_atomic';

-- The INSERT should succeed since atomicity is preserved
INSERT INTO test_alter_atomic VALUES (2);
SELECT * FROM test_alter_atomic ORDER BY c0;

DROP TABLE test_alter_atomic;

-- Test 2: MergeTree engine - same behavior expected
CREATE TABLE test_alter_atomic (key Int32, c0 Int32) ENGINE = MergeTree ORDER BY key;
INSERT INTO test_alter_atomic VALUES (1, 0);

-- This should be rejected early
ALTER TABLE test_alter_atomic UPDATE c0 = 1 WHERE true, RENAME COLUMN c0 TO c1; -- { serverError BAD_ARGUMENTS }

-- Verify column still has original name
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'test_alter_atomic' ORDER BY position;
INSERT INTO test_alter_atomic VALUES (2, 2);
SELECT * FROM test_alter_atomic ORDER BY key;

DROP TABLE test_alter_atomic;

-- Test 3: DELETE + RENAME on same column should be rejected
CREATE TABLE test_alter_atomic (c0 Int32) ENGINE = Memory;
INSERT INTO test_alter_atomic VALUES (0);

-- This should fail early with a clear error
ALTER TABLE test_alter_atomic DELETE WHERE c0 = 0, RENAME COLUMN c0 TO c1; -- { serverError BAD_ARGUMENTS }

-- Verify column still has original name
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'test_alter_atomic';
SELECT * FROM test_alter_atomic;

DROP TABLE test_alter_atomic;

-- Test 4: Verify UPDATE + RENAME on DIFFERENT columns works fine
CREATE TABLE test_alter_atomic (key Int32, c0 Int32, c1 Int32) ENGINE = MergeTree ORDER BY key;
INSERT INTO test_alter_atomic VALUES (1, 10, 20);

-- UPDATE c0, RENAME c1 should work because they are different columns
ALTER TABLE test_alter_atomic UPDATE c0 = 100 WHERE true, RENAME COLUMN c1 TO c2 SETTINGS mutations_sync=2;

SELECT key, c0, c2 FROM test_alter_atomic;
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'test_alter_atomic' ORDER BY position;

DROP TABLE test_alter_atomic;

-- Test 5: Verify DELETE + RENAME on DIFFERENT columns works fine
CREATE TABLE test_alter_atomic (key Int32, c0 Int32, c1 Int32) ENGINE = MergeTree ORDER BY key;
INSERT INTO test_alter_atomic VALUES (1, 10, 20), (2, 30, 40);

-- DELETE based on c0, RENAME c1 should work because they are different columns
ALTER TABLE test_alter_atomic DELETE WHERE c0 = 10, RENAME COLUMN c1 TO c2 SETTINGS mutations_sync=2;

SELECT * FROM test_alter_atomic;
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'test_alter_atomic' ORDER BY position;

DROP TABLE test_alter_atomic;
