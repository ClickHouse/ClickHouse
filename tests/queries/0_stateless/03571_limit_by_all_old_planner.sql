-- Test that LIMIT BY ALL throws an exception when using the old planner
-- This tests the changes in TreeWriter.cpp

SET max_threads = 1;
SET max_insert_threads = 1;
SET max_block_size = 65536;

CREATE TABLE test_limit_by_all_old_planner (
    id Int32,
    category String,
    value Int32
) ENGINE = Memory;

INSERT INTO test_limit_by_all_old_planner VALUES 
(1, 'A', 100),
(2, 'B', 200);

-- This should throw an exception when using old planner
SELECT id, category, value 
FROM test_limit_by_all_old_planner 
LIMIT 1 BY ALL
SETTINGS allow_experimental_analyzer = 0; -- {serverError NOT_IMPLEMENTED}

DROP TABLE test_limit_by_all_old_planner; 