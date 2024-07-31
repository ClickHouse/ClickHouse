-- Step 1: Create the database if it does not exist on the cluster
CREATE DATABASE IF NOT EXISTS test_mlt65kbw ON CLUSTER 'test_shard_localhost';

-- Step 2: Create necessary tables on the cluster within the correct database
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

-- Step 3: Insert data into the distributed table
INSERT INTO test_mlt65kbw.distributed_table VALUES (1, 'test1'), (2, 'test2'), (3, 'test3');

-- Step 4: Query the distributed table to verify the results in the correct format
SELECT id, value FROM test_mlt65kbw.distributed_table ORDER BY id FORMAT TabSeparated;
