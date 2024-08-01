-- Step 1: Create the database if it does not exist on the cluster
CREATE DATABASE IF NOT EXISTS test_mlt65kbw ON CLUSTER 'test_shard_localhost';

-- Step 2: Use the database to ensure all subsequent operations are within this context
USE test_mlt65kbw;

-- Step 3: Create necessary tables on the cluster within the correct database
CREATE TABLE local_table ON CLUSTER 'test_shard_localhost'
(
    id UInt32,
    value String
)
ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE distributed_table ON CLUSTER 'test_shard_localhost'
(
    id UInt32,
    value String
)
ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'local_table', rand());

-- Step 4: Insert data into the distributed table
INSERT INTO distributed_table VALUES (1, 'test1'), (2, 'test2'), (3, 'test3');

-- Step 5: Query the distributed table to verify the results
-- Ensure the query format is correct and suppress unnecessary output
SELECT id, value FROM distributed_table ORDER BY id FORMAT TabSeparated;

-- Step 6: Drop the tables to clean up
DROP TABLE IF EXISTS local_table ON CLUSTER 'test_shard_localhost';
DROP TABLE IF EXISTS distributed_table ON CLUSTER 'test_shard_localhost';

-- Drop the database to clean up
DROP DATABASE IF EXISTS test_mlt65kbw ON CLUSTER 'test_shard_localhost';
