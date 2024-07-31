-- Step 1: Create the database if it does not exist on the cluster
CREATE DATABASE IF NOT EXISTS test_mlt65kbw ON CLUSTER 'test_shard_localhost';

-- Step 2: Use the database to ensure all subsequent operations are within this context
USE test_mlt65kbw;

-- Step 3: Create necessary tables on the cluster within the correct database
CREATE TABLE test_mlt65kbw.local_table ON CLUSTER 'test_shard_localhost'
(
    id UInt32,
    value String
)
ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE test_mlt65kbw.distributed_table ON CLUSTER 'test_shard_localhost'
(
    id UInt32,
    value String
)
ENGINE = Distributed('test_shard_localhost', 'test_mlt65kbw', 'local_table', rand());

-- Step 4: Insert data into the distributed table
INSERT INTO test_mlt65kbw.distributed_table VALUES (1, 'test1'), (2, 'test2'), (3, 'test3');

-- Step 5: Query the distributed table to verify the results in the correct format
-- Suppress unnecessary output
SET output_format_enable_aggregation_tree = 0, output_format_enable_debug_info = 0;

SELECT id, value FROM test_mlt65kbw.distributed_table ORDER BY id FORMAT TabSeparated;
