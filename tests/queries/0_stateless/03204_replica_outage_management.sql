-- Step 1: Create necessary tables on the cluster
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
ENGINE = Distributed('test_shard_localhost', 'default', 'local_table', rand());

-- Step 2: Insert data into the distributed table
INSERT INTO distributed_table VALUES (1, 'test1'), (2, 'test2'), (3, 'test3');

-- Step 3: Simulate network error

-- Step 4: Query the distributed table to verify the results
SELECT id, value FROM distributed_table ORDER BY id;
