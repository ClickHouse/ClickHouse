-- Regression test for "Unexpected return type from getSubcolumn. Expected UInt64. Got Nullable(UInt64)" with join_use_nulls
-- Issue: When join_use_nulls=true, columns from outer-joined tables become nullable,
-- but getSubcolumn function return type wasn't updated to match, causing a type mismatch error.

SET join_use_nulls = true;

-- Create simple test tables with String columns that have 'size' subcolumn
DROP TABLE IF EXISTS test_left;
DROP TABLE IF EXISTS test_right;

CREATE TABLE test_left (id Int32, data String) ENGINE = Memory;
CREATE TABLE test_right (id Int32, data String) ENGINE = Memory;

INSERT INTO test_left VALUES (1, 'left1'), (2, 'left2');
INSERT INTO test_right VALUES (2, 'right2'), (3, 'right3');

-- Test 1: Access size subcolumn on right table in FULL JOIN
-- With join_use_nulls=true, r.data becomes Nullable(String)
-- So r.data.size should return Nullable(UInt64)
SELECT 
    l.id,
    r.`data.size`
FROM test_left l
FULL JOIN test_right r ON l.id = r.id
ORDER BY l.id, r.id
FORMAT Null;

-- Test 2: Access size subcolumn on left table in FULL JOIN  
-- With join_use_nulls=true, l.data becomes Nullable(String)
-- So l.data.size should return Nullable(UInt64)
SELECT 
    r.id,
    l.`data.size`
FROM test_left l
FULL JOIN test_right r ON l.id = r.id
ORDER BY l.id, r.id
FORMAT Null;

-- Test 3: Access size subcolumn on right table in LEFT JOIN
-- With join_use_nulls=true, r.data becomes Nullable(String)
SELECT 
    l.id,
    r.`data.size`
FROM test_left l
LEFT JOIN test_right r ON l.id = r.id
ORDER BY l.id
FORMAT Null;

-- Test 4: Access size subcolumn on left table in RIGHT JOIN
-- With join_use_nulls=true, l.data becomes Nullable(String)
SELECT 
    r.id,
    l.`data.size`
FROM test_left l
RIGHT JOIN test_right r ON l.id = r.id
ORDER BY r.id
FORMAT Null;

DROP TABLE test_left;
DROP TABLE test_right;
