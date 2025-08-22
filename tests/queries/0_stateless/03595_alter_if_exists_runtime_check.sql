-- Test runtime IF EXISTS checks for ALTER COLUMN commands
-- This test verifies that IF EXISTS clauses work correctly when column state changes
-- between validate() and apply() phases within the same ALTER statement

DROP TABLE IF EXISTS test_alter_if_exists;

-- Test 1: DROP COLUMN IF EXISTS with column deleted by previous command
CREATE TABLE test_alter_if_exists (c0 Int32, c1 String) ENGINE = Memory;
-- This should succeed - first DROP removes c0, second DROP with IF EXISTS should be silently ignored
ALTER TABLE test_alter_if_exists DROP COLUMN c0, DROP COLUMN IF EXISTS c0;
DESC test_alter_if_exists;

DROP TABLE test_alter_if_exists;

-- Test 2: MODIFY COLUMN IF EXISTS with column deleted by previous command  
CREATE TABLE test_alter_if_exists (c0 Int32, c1 String) ENGINE = Memory;
-- This should succeed - DROP removes c0, MODIFY with IF EXISTS should be silently ignored
ALTER TABLE test_alter_if_exists DROP COLUMN c0, MODIFY COLUMN IF EXISTS c0 Int64;
DESC test_alter_if_exists;

DROP TABLE test_alter_if_exists;

-- Test 3: COMMENT COLUMN IF EXISTS with column deleted by previous command
CREATE TABLE test_alter_if_exists (x Int32, y String) ENGINE = Memory;
-- This should succeed - DROP removes x, COMMENT with IF EXISTS should be silently ignored
ALTER TABLE test_alter_if_exists DROP COLUMN x, COMMENT COLUMN IF EXISTS x 'test comment';
DESC test_alter_if_exists;

DROP TABLE test_alter_if_exists;

-- Test 4: RENAME COLUMN IF EXISTS with column deleted by previous command
CREATE TABLE test_alter_if_exists (x Int32, y String) ENGINE = Memory;
-- This should succeed - DROP removes x, RENAME with IF EXISTS should be silently ignored
ALTER TABLE test_alter_if_exists DROP COLUMN x, RENAME COLUMN IF EXISTS x TO z;
DESC test_alter_if_exists;

DROP TABLE test_alter_if_exists;

-- Test 5: Multiple operations in sequence
CREATE TABLE test_alter_if_exists (a Int32, b String, c Float64) ENGINE = Memory;
-- Complex case: multiple drops and modifications
ALTER TABLE test_alter_if_exists 
    DROP COLUMN a, 
    DROP COLUMN IF EXISTS a,
    MODIFY COLUMN IF EXISTS a Int64,
    COMMENT COLUMN IF EXISTS a 'should be ignored',
    RENAME COLUMN IF EXISTS a TO a_renamed,
    MODIFY COLUMN IF EXISTS b String DEFAULT 'test',
    DROP COLUMN c,
    MODIFY COLUMN IF EXISTS c Float32;
DESC test_alter_if_exists;

DROP TABLE test_alter_if_exists;

-- Test 6: Verify that without IF EXISTS, operations fail as expected
CREATE TABLE test_alter_if_exists (x Int32, y String) ENGINE = Memory;

-- This should fail - trying to drop non-existent column without IF EXISTS
ALTER TABLE test_alter_if_exists DROP COLUMN x, DROP COLUMN x; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

DROP TABLE IF EXISTS test_alter_if_exists;