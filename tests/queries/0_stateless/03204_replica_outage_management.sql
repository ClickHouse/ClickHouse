-- Step 1: Create necessary tables
CREATE TABLE local_table ON CLUSTER '{cluster}'
(
    id UInt32,
    value String
)
ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE distributed_table ON CLUSTER '{cluster}'
(
    id UInt32,
    value String
)
ENGINE = Distributed('{cluster}', 'default', 'local_table', rand());

-- Step 2: Insert data into the distributed table
INSERT INTO distributed_table VALUES (1, 'test1'), (2, 'test2'), (3, 'test3');

-- Step 3: Simulate network error
-- This would typically be done by changing the network conditions or the configuration of the cluster.
-- For the sake of this test, we will assume the implementation of such a simulation is handled by the testing framework.

-- Step 4: Query the distributed table to verify the results
SELECT id, value FROM distributed_table ORDER BY id;
