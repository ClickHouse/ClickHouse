-- Tags: no-replicated-database

-- Test for issue #85608: Logical Error when using DROP COLUMN and COMMENT COLUMN IF EXISTS in same ALTER
-- This test verifies that COMMENT COLUMN IF EXISTS works correctly when the column is being dropped in the same ALTER statement

-- Test with Memory engine
DROP TABLE IF EXISTS test_alter_drop_comment;

CREATE TABLE test_alter_drop_comment (
    c0 Int,
    c1 Int,
    c2 String
) ENGINE = Memory;

-- Test case 1: DROP COLUMN + COMMENT COLUMN IF EXISTS (should succeed)
-- This was previously failing with "Cannot find column c0 in ColumnsDescription"
ALTER TABLE test_alter_drop_comment 
    DROP COLUMN c0, 
    COMMENT COLUMN IF EXISTS c0 'this comment should be silently ignored';

-- Verify that c0 is dropped and c1, c2 remain
DESCRIBE test_alter_drop_comment;

-- Test case 2: COMMENT COLUMN IF EXISTS on non-existent column (should succeed)
ALTER TABLE test_alter_drop_comment 
    COMMENT COLUMN IF EXISTS non_existent_column 'this should be ignored';

-- Verify table structure is unchanged
DESCRIBE test_alter_drop_comment;

-- Test case 3: COMMENT COLUMN without IF EXISTS on non-existent column (should fail)
ALTER TABLE test_alter_drop_comment 
    COMMENT COLUMN non_existent_column 'this should fail'; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

-- Test case 4: Multiple operations with IF EXISTS
ALTER TABLE test_alter_drop_comment 
    DROP COLUMN c1,
    COMMENT COLUMN IF EXISTS c1 'dropped column comment',
    COMMENT COLUMN IF EXISTS c2 'existing column comment';

-- Verify final state - only c2 should remain with comment
DESCRIBE test_alter_drop_comment;

SELECT table, name, comment 
FROM system.columns 
WHERE table = 'test_alter_drop_comment' AND database = currentDatabase()
ORDER BY name;

DROP TABLE test_alter_drop_comment;

-- Test with different table engines
-- Test with MergeTree
CREATE TABLE test_alter_drop_comment_mt (
    id Int32,
    value String,
    status Int8
) ENGINE = MergeTree() ORDER BY id;

ALTER TABLE test_alter_drop_comment_mt 
    DROP COLUMN status,
    COMMENT COLUMN IF EXISTS status 'dropped status column',
    COMMENT COLUMN IF EXISTS value 'existing value column';

DESCRIBE test_alter_drop_comment_mt;

DROP TABLE test_alter_drop_comment_mt;

-- Test edge case: try to drop and comment the same column without IF EXISTS (should fail)
CREATE TABLE test_alter_fail (c0 Int, c1 Int) ENGINE = Memory;

ALTER TABLE test_alter_fail 
    DROP COLUMN c0, 
    COMMENT COLUMN c0 'this should fail'; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }

DROP TABLE test_alter_fail;
